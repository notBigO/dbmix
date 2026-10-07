// Package cli owns application wiring; the engine has no flag or filesystem knowledge.
package cli

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"os"
	"time"

	"github.com/notBigO/dbmix/internal/config"
	"github.com/notBigO/dbmix/internal/connectors"
	"github.com/notBigO/dbmix/internal/engine"
	"github.com/notBigO/dbmix/internal/planner"
	"github.com/notBigO/dbmix/internal/state"
)

const Example = `version: 1
source:
  type: postgres
  uri: ${SOURCE_URI}
  allow_insecure: true # Local development only; use verified TLS elsewhere.
destination:
  type: mysql
  uri: ${DESTINATION_URI}
  allow_insecure: true
source_quiescent: true # Stop source writes through migration AND resume.
mode: upsert
batch_size: 1000
workers: 4
queue_size: 4
max_batch_bytes: 16777216
retries: 3
timeout_seconds: 30
entities:
  - source: public.customers
    target: customers
    key: id
`

func Run(ctx context.Context, args []string, out, errOut io.Writer, version string) error {
	if len(args) == 0 {
		return usage(out)
	}
	command := args[0]
	switch command {
	case "help", "--help", "-h":
		return usage(out)
	case "version":
		_, err := fmt.Fprintln(out, "dbmix", version)
		return err
	case "connectors":
		_, err := fmt.Fprintln(out, "NAME        SOURCE  DESTINATION\npostgres    yes     yes\nmysql       yes     yes\nmongodb     yes     yes\n\nOffline upserts only. SQL source keys: signed integers. MongoDB source keys: integer or ObjectID.")
		return err
	}
	fs := flag.NewFlagSet(command, flag.ContinueOnError)
	fs.SetOutput(errOut)
	path := fs.String("f", "migration.yaml", "configuration file")
	dir := fs.String("state-dir", ".dbmix", "local migration state directory")
	if err := fs.Parse(args[1:]); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return nil
		}
		return err
	}
	if command == "init" {
		if fs.NArg() != 0 {
			return fmt.Errorf("init takes no positional arguments")
		}
		f, err := os.OpenFile(*path, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0600)
		if err != nil {
			return fmt.Errorf("create config: %w", err)
		}
		_, writeErr := f.WriteString(Example)
		return errors.Join(writeErr, f.Close())
	}
	if command != "plan" && command != "migrate" && command != "resume" && command != "status" {
		return fmt.Errorf("unknown command %q; run dbmix help", command)
	}
	if (command == "status" || command == "resume") && fs.NArg() != 1 {
		return fmt.Errorf("%s requires a migration ID (flags go before the ID)", command)
	}
	if (command == "plan" || command == "migrate") && fs.NArg() != 0 {
		return fmt.Errorf("%s accepts -f config.yaml; use resume <id> for a saved plan", command)
	}
	store, err := state.Open(*dir)
	if err != nil {
		return fmt.Errorf("open state directory: %w", err)
	}
	if command == "status" {
		m, err := store.Load(fs.Arg(0))
		if err != nil {
			return err
		}
		return printJSON(out, m)
	}
	var m state.Migration
	var unlock func() error
	if command == "resume" {
		unlock, err = store.Lock(fs.Arg(0))
		if err != nil {
			return err
		}
		defer func() { _ = unlock() }()
		m, err = store.Load(fs.Arg(0))
		if err != nil {
			return err
		}
		if m.Status == "completed" {
			return fmt.Errorf("migration is already completed")
		}
	} else {
		c, err := config.Load(*path)
		if err != nil {
			return err
		}
		m.Plan.Config = c
	}
	c := m.Plan.Config
	if err = c.Validate(); err != nil {
		return fmt.Errorf("validate saved configuration: %w", err)
	}
	fingerprint, err := c.Fingerprint()
	if err != nil {
		return err
	}
	if command == "resume" && fingerprint != m.EndpointFingerprint {
		return fmt.Errorf("connection environment changed since planning; refusing to reuse checkpoints")
	}
	connectCtx, cancel := context.WithTimeout(ctx, time.Duration(c.TimeoutSeconds)*time.Second)
	src, dst, err := connectors.Open(connectCtx, c)
	cancel()
	if err != nil {
		return err
	}
	defer func() { _ = src.Close(); _ = dst.Close() }()
	planCtx, planCancel := context.WithTimeout(ctx, time.Duration(c.TimeoutSeconds)*time.Second)
	defer planCancel()
	if command == "resume" {
		if err = planner.Verify(planCtx, m.Plan, src, dst); err != nil {
			return fmt.Errorf("verify saved plan: %w", err)
		}
	} else {
		p, err := planner.Build(planCtx, c, src, dst)
		if err != nil {
			return err
		}
		m = state.New(p)
		m.EndpointFingerprint = fingerprint
		unlock, err = store.Lock(m.ID)
		if err != nil {
			return err
		}
		defer func() { _ = unlock() }()
		if err = store.Save(&m); err != nil {
			return err
		}
	}
	planCancel()
	if command == "plan" {
		return printJSON(out, m)
	}
	log := slog.New(slog.NewJSONHandler(errOut, nil))
	log.Info("migration started", "migration_id", m.ID, "source", c.Source.Type, "destination", c.Destination.Type)
	e := engine.Engine{Source: src, Destination: dst, Save: store.Save, Log: log}
	if err = e.Run(ctx, &m); err != nil {
		return fmt.Errorf("migration %s stopped; run dbmix resume --state-dir %s %s: %w", m.ID, *dir, m.ID, err)
	}
	records, bytes := m.Totals()
	rate := float64(0)
	if m.ElapsedSeconds > 0 {
		rate = float64(records) / m.ElapsedSeconds
	}
	return printJSON(out, struct {
		ID            string  `json:"migration_id"`
		Status        string  `json:"status"`
		Records       int64   `json:"records"`
		Bytes         int64   `json:"bytes"`
		Seconds       float64 `json:"elapsed_seconds"`
		Rate          float64 `json:"records_per_second"`
		FailedBatches int64   `json:"failed_batch_attempts"`
	}{m.ID, m.Status, records, bytes, m.ElapsedSeconds, rate, m.FailedBatches})
}
func printJSON(w io.Writer, v any) error {
	enc := json.NewEncoder(w)
	enc.SetIndent("", "  ")
	return enc.Encode(v)
}
func usage(w io.Writer) error {
	_, err := fmt.Fprintln(w, `dbmix — offline database migrations with durable resume

Usage:
  dbmix init [-f migration.yaml]
  dbmix plan -f migration.yaml [--state-dir .dbmix]
  dbmix migrate -f migration.yaml [--state-dir .dbmix]
  dbmix status [--state-dir .dbmix] <migration-id>
  dbmix resume [--state-dir .dbmix] <migration-id>
  dbmix connectors
  dbmix version

plan is read-only against databases; it saves a local plan ID.
resume executes a saved plan or continues an interrupted migration.
Connection URIs must come from environment references such as ${SOURCE_URI}.`)
	return err
}
