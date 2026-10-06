//go:build integration

package integration

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/notBigO/dbmix/internal/cli"
	"github.com/notBigO/dbmix/internal/config"
	"github.com/notBigO/dbmix/internal/connector"
	"github.com/notBigO/dbmix/internal/connectors"
	"github.com/notBigO/dbmix/internal/engine"
	"github.com/notBigO/dbmix/internal/model"
	"github.com/notBigO/dbmix/internal/planner"
	"github.com/notBigO/dbmix/internal/state"
	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
)

type ambiguousWrite struct {
	connector.Destination
	calls atomic.Int64
}
type temporaryFailure struct{}

func (temporaryFailure) Error() string   { return "connection lost after commit" }
func (temporaryFailure) Temporary() bool { return true }
func (d *ambiguousWrite) Write(ctx context.Context, entity string, schema model.Schema, rows []model.Record) error {
	if err := d.Destination.Write(ctx, entity, schema, rows); err != nil {
		return err
	}
	if d.calls.Add(1) == 1 {
		return temporaryFailure{}
	}
	return nil
}
func TestAmbiguousAcknowledgement(t *testing.T) {
	seed(t, "postgres", "retry_source", 51)
	for _, kind := range []string{"postgres", "mysql", "mongodb"} {
		t.Run(kind, func(t *testing.T) {
			s, d, p := openPlan(t, cfg(t, "postgres", kind, "retry_source", "retry_"+kind))
			wrapped := &ambiguousWrite{Destination: d}
			run(t, s, wrapped, p)
			verify(t, kind, "retry_"+kind, 51)
			if wrapped.calls.Load() != 4 {
				t.Fatalf("expected replay; calls=%d", wrapped.calls.Load())
			}
		})
	}
}
func TestMongoPartialBatchResume(t *testing.T) {
	seed(t, "postgres", "partial_source", 3)
	col := mg.Database("dbmix").Collection("partial_target")
	ctx := context.Background()
	if err := col.Drop(ctx); err != nil {
		t.Fatal(err)
	}
	if _, err := col.Indexes().CreateOne(ctx, mongo.IndexModel{Keys: bson.D{{Key: "enabled", Value: 1}}, Options: options.Index().SetUnique(true).SetName("unique_enabled")}); err != nil {
		t.Fatal(err)
	}
	c := cfg(t, "postgres", "mongodb", "partial_source", "partial_target")
	c.Workers = 1
	c.BatchSize = 3
	s, d, p := openPlan(t, c)
	store, _ := state.Open(t.TempDir())
	m := state.New(p)
	e := engine.Engine{Source: s, Destination: d, Save: store.Save, Log: slog.New(slog.NewTextHandler(io.Discard, nil))}
	if err := e.Run(ctx, &m); err == nil {
		t.Fatal("duplicate key was swallowed")
	}
	if m.Progress[0].Cursor != "" {
		t.Fatal("partial batch checkpointed")
	}
	n, err := col.CountDocuments(ctx, bson.D{})
	if err != nil || n != 2 {
		t.Fatalf("expected two committed writes, got %d: %v", n, err)
	}
	if err = col.Indexes().DropOne(ctx, "unique_enabled"); err != nil {
		t.Fatal(err)
	}
	if err = e.Run(ctx, &m); err != nil {
		t.Fatal(err)
	}
	verify(t, "mongodb", "partial_target", 3)
}
func TestSQLBatchRollback(t *testing.T) {
	seed(t, "postgres", "rollback_source", 3)
	_, err := pg.Exec("DROP TABLE IF EXISTS rollback_target")
	if err != nil {
		t.Fatal(err)
	}
	s, d, p := openPlan(t, cfg(t, "postgres", "postgres", "rollback_source", "rollback_target"))
	ctx := context.Background()
	schema := p.Entities[0].Output
	if err = d.Prepare(ctx, "rollback_target", schema); err != nil {
		t.Fatal(err)
	}
	if _, err = pg.Exec("CREATE UNIQUE INDEX rollback_unique ON rollback_target(enabled)"); err != nil {
		t.Fatal(err)
	}
	rows, _, err := s.Read(ctx, "rollback_source", p.Entities[0].Input, "", p.Entities[0].Upper, 3)
	if err != nil {
		t.Fatal(err)
	}
	if err = d.Write(ctx, "rollback_target", schema, rows); err == nil {
		t.Fatal("expected unique constraint failure")
	}
	var count int
	if err = pg.QueryRow("SELECT COUNT(*) FROM rollback_target").Scan(&count); err != nil || count != 0 {
		t.Fatal("transaction left partial data", err)
	}
}
func TestObjectIDResume(t *testing.T) {
	ctx := context.Background()
	col := mg.Database("dbmix").Collection("oid_source")
	if err := col.Drop(ctx); err != nil {
		t.Fatal(err)
	}
	ids := []bson.ObjectID{bson.NewObjectID(), bson.NewObjectID(), bson.NewObjectID()}
	for _, id := range ids {
		if _, err := col.InsertOne(ctx, bson.M{"_id": id, "name": "ObjectID"}); err != nil {
			t.Fatal(err)
		}
	}
	for _, dst := range []string{"postgres", "mysql", "mongodb"} {
		t.Run(dst, func(t *testing.T) {
			c := cfg(t, "mongodb", dst, "oid_source", "oid_"+dst)
			c.Entities[0].Fields = []config.Field{{Source: "_id", Target: key(dst), Type: model.ObjectID}, {Source: "name", Target: "name", Type: model.String}}
			c.BatchSize = 1
			s, d, p := openPlan(t, c)
			run(t, s, d, p)
			run(t, s, d, p)
			if dst == "mongodb" {
				for _, id := range ids {
					if err := mg.Database("dbmix").Collection("oid_"+dst).FindOne(ctx, bson.M{"_id": id}).Err(); err != nil {
						t.Fatal(err)
					}
				}
			} else {
				db := pg
				placeholder := "$1"
				if dst == "mysql" {
					db = my
					placeholder = "?"
				}
				for _, id := range ids {
					var name string
					if err := db.QueryRow("SELECT name FROM oid_"+dst+" WHERE id="+placeholder, id.Hex()).Scan(&name); err != nil || name != "ObjectID" {
						t.Fatal(err)
					}
				}
			}
		})
	}
}
func TestCLIProcessResume(t *testing.T) {
	binary := filepath.Join(t.TempDir(), "dbmix")
	build := exec.Command("go", "build", "-o", binary, "./cmd/dbmix")
	build.Dir = ".."
	if output, err := build.CombinedOutput(); err != nil {
		t.Fatalf("build: %s %v", output, err)
	}
	ctx := context.Background()
	if _, err := pg.Exec("DROP TABLE IF EXISTS process_source"); err != nil {
		t.Fatal(err)
	}
	if _, err := pg.Exec("CREATE TABLE process_source(id BIGINT PRIMARY KEY, name TEXT NOT NULL)"); err != nil {
		t.Fatal(err)
	}
	if _, err := pg.Exec("INSERT INTO process_source SELECT n, 'record-' || n FROM generate_series(1,2000) n"); err != nil {
		t.Fatal(err)
	}
	t.Setenv("PROCESS_SOURCE", pgURI)
	t.Setenv("PROCESS_DEST", myURI)
	for _, signal := range []os.Signal{syscall.SIGTERM, syscall.SIGKILL} {
		t.Run(signal.String(), func(t *testing.T) {
			dir := t.TempDir()
			target := "process_term"
			if signal == syscall.SIGKILL {
				target = "process_kill"
			}
			configPath := filepath.Join(dir, "migration.yaml")
			text := fmt.Sprintf("version: 1\nsource: {type: postgres, uri: '${PROCESS_SOURCE}', allow_insecure: true}\ndestination: {type: mysql, uri: '${PROCESS_DEST}', allow_insecure: true}\nsource_quiescent: true\nbatch_size: 1\nworkers: 3\nentities: [{source: process_source, target: %s, key: id}]\n", target)
			if err := os.WriteFile(configPath, []byte(text), 0600); err != nil {
				t.Fatal(err)
			}
			var out bytes.Buffer
			if err := cli.Run(ctx, []string{"plan", "-f", configPath, "--state-dir", dir}, &out, io.Discard, "test"); err != nil {
				t.Fatal(err)
			}
			var m state.Migration
			if err := json.Unmarshal(out.Bytes(), &m); err != nil {
				t.Fatal(err)
			}
			cmd := exec.Command(binary, "resume", "--state-dir", dir, m.ID)
			stderr, err := cmd.StderrPipe()
			if err != nil {
				t.Fatal(err)
			}
			if err = cmd.Start(); err != nil {
				t.Fatal(err)
			}
			observed := make(chan error, 1)
			go func() {
				scanner := bufio.NewScanner(stderr)
				sent := false
				for scanner.Scan() {
					if !sent && strings.Contains(scanner.Text(), "batch migrated") {
						observed <- cmd.Process.Signal(signal)
						sent = true
					}
				}
				if !sent {
					observed <- errors.New("process exited before migrating a batch")
				}
			}()
			select {
			case err = <-observed:
				if err != nil {
					t.Fatal(err)
				}
			case <-time.After(15 * time.Second):
				_ = cmd.Process.Kill()
				t.Fatal("process failed to start migration")
			}
			if err = cmd.Wait(); err == nil {
				t.Fatal("expected interrupted process")
			}
			store, _ := state.Open(dir)
			saved, err := store.Load(m.ID)
			if err != nil {
				t.Fatal(err)
			}
			if saved.Status == "completed" || saved.Progress[0].Records >= 2000 {
				t.Fatal("process wasn't interrupted")
			}
			if signal == syscall.SIGTERM && saved.Status != "interrupted" {
				t.Fatalf("SIGTERM status %s", saved.Status)
			}
			if err = cli.Run(ctx, []string{"resume", "--state-dir", dir, m.ID}, io.Discard, io.Discard, "test"); err != nil {
				t.Fatal(err)
			}
			var count int
			var sum int64
			if err = my.QueryRow("SELECT COUNT(*),SUM(id) FROM "+target).Scan(&count, &sum); err != nil || count != 2000 || sum != 2001000 {
				t.Fatalf("resume data: %d %d %v", count, sum, err)
			}
		})
	}
}

// This test stops only this project's Compose service, then restores it before returning.
func TestDestinationUnavailableResume(t *testing.T) {
	seed(t, "postgres", "unavailable_source", 31)
	c := cfg(t, "postgres", "mysql", "unavailable_source", "unavailable_target")
	c.Retries = 0
	c.TimeoutSeconds = 2
	s, d, p := openPlan(t, c)
	compose := func(args ...string) error {
		cmd := exec.Command("docker", append([]string{"compose"}, args...)...)
		cmd.Dir = ".."
		output, err := cmd.CombinedOutput()
		if err != nil {
			return fmt.Errorf("compose: %s: %w", output, err)
		}
		return nil
	}
	if err := compose("stop", "mysql"); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := compose("up", "-d", "--wait", "mysql"); err != nil {
			t.Error(err)
		}
	})
	store, _ := state.Open(t.TempDir())
	m := state.New(p)
	e := engine.Engine{Source: s, Destination: d, Save: store.Save, Log: slog.New(slog.NewTextHandler(io.Discard, nil))}
	if err := e.Run(context.Background(), &m); err == nil {
		t.Fatal("unavailable destination succeeded")
	}
	if m.Status != "failed" && m.Status != "interrupted" {
		t.Fatal("wrong failure status")
	}
	if err := compose("up", "-d", "--wait", "mysql"); err != nil {
		t.Fatal(err)
	}
	// A fresh process would open new pools. Reopen here rather than relying on driver recovery.
	s2, d2, _ := openPlan(t, c)
	e.Source = s2
	e.Destination = d2
	if err := e.Run(context.Background(), &m); err != nil {
		t.Fatal(err)
	}
	verify(t, "mysql", "unavailable_target", 31)
}

func TestRejectMixedMongoKeysAndUnsafeMySQLIndex(t *testing.T) {
	ctx := context.Background()
	col := mg.Database("dbmix").Collection("mixed_source")
	if err := col.Drop(ctx); err != nil {
		t.Fatal(err)
	}
	if _, err := col.InsertMany(ctx, []any{bson.M{"_id": int64(1)}, bson.M{"_id": "hidden-by-type-bracket"}}); err != nil {
		t.Fatal(err)
	}
	c := cfg(t, "mongodb", "postgres", "mixed_source", "mixed_target")
	c.Entities[0].Fields = []config.Field{{Source: "_id", Target: "id", Type: model.Int64}}
	s, d, err := connectors.Open(ctx, c)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = s.Close(); _ = d.Close() }()
	if _, err = planner.Build(ctx, c, s, d); err == nil {
		t.Fatal("mixed key types accepted")
	}
	seed(t, "postgres", "unsafe_source", 2)
	seed(t, "mysql", "unsafe_target", 2)
	if _, err = my.Exec("CREATE UNIQUE INDEX unsafe_unique ON unsafe_target(enabled)"); err != nil {
		t.Fatal(err)
	}
	c = cfg(t, "postgres", "mysql", "unsafe_source", "unsafe_target")
	s2, d2, err := connectors.Open(ctx, c)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = s2.Close(); _ = d2.Close() }()
	if _, err = planner.Build(ctx, c, s2, d2); err == nil {
		t.Fatal("ambiguous MySQL upsert accepted")
	}
}

func TestRejectTimestampRounding(t *testing.T) {
	seed(t, "postgres", "precision_source", 1)
	seed(t, "postgres", "precision_target", 0)
	if _, err := pg.Exec("ALTER TABLE precision_target ALTER COLUMN created_at TYPE timestamptz(0)"); err != nil {
		t.Fatal(err)
	}
	c := cfg(t, "postgres", "postgres", "precision_source", "precision_target")
	s, d, err := connectors.Open(context.Background(), c)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = s.Close(); _ = d.Close() }()
	if _, err = planner.Build(context.Background(), c, s, d); err == nil || !strings.Contains(err.Error(), "precision") {
		t.Fatalf("narrowing timestamp destination accepted: %v", err)
	}
}
