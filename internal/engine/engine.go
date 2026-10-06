// Package engine coordinates bounded work and durable ordered acknowledgements.
package engine

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/notBigO/dbmix/internal/connector"
	"github.com/notBigO/dbmix/internal/model"
	"github.com/notBigO/dbmix/internal/planner"
	"github.com/notBigO/dbmix/internal/retry"
	"github.com/notBigO/dbmix/internal/state"
)

type Engine struct {
	Source      connector.Source
	Destination connector.Destination
	Save        func(*state.Migration) error
	Log         *slog.Logger
}
type batch struct {
	seq    int64
	cursor model.Cursor
	rows   []model.Record
	bytes  int64
}
type ack struct {
	seq     int64
	cursor  model.Cursor
	records int64
	bytes   int64
}

func (e *Engine) Run(ctx context.Context, m *state.Migration) (err error) {
	if m.Status == "completed" {
		return fmt.Errorf("migration is already completed")
	}
	if e.Log == nil {
		e.Log = slog.Default()
	}
	started := time.Now()
	previous := m.ElapsedSeconds
	m.Status = "running"
	m.LastError = ""
	if err = e.Save(m); err != nil {
		return fmt.Errorf("save running state: %w", err)
	}
	defer func() {
		m.ElapsedSeconds = previous + time.Since(started).Seconds()
		if err != nil {
			m.Status = "failed"
			if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
				m.Status = "interrupted"
			}
			m.LastError = err.Error()
		} else {
			m.Status = "completed"
		}
		if saveErr := e.Save(m); saveErr != nil {
			err = errors.Join(err, fmt.Errorf("save final state: %w", saveErr))
		}
	}()
	for i, p := range m.Plan.Entities {
		if m.Progress[i].Done {
			continue
		}
		if err = e.operation(ctx, m, func(c context.Context) error { return e.Destination.Prepare(c, p.Target, p.Output) }); err != nil {
			return fmt.Errorf("prepare %s: %w", p.Target, err)
		}
		if err = e.entity(ctx, m, i, p, started, previous); err != nil {
			return fmt.Errorf("migrate %s: %w", p.Source, err)
		}
		m.Progress[i].Done = true
		if err = e.Save(m); err != nil {
			return fmt.Errorf("save entity completion: %w", err)
		}
	}
	return nil
}
func (e *Engine) operation(ctx context.Context, m *state.Migration, fn func(context.Context) error) error {
	return retry.Do(ctx, m.Plan.Config.Retries, func() error {
		c, cancel := context.WithTimeout(ctx, time.Duration(m.Plan.Config.TimeoutSeconds)*time.Second)
		defer cancel()
		return fn(c)
	})
}
func (e *Engine) entity(parent context.Context, m *state.Migration, index int, p planner.Entity, started time.Time, previous float64) error {
	ctx, cancel := context.WithCancel(parent)
	defer cancel()
	cfg := m.Plan.Config
	jobs := make(chan batch, cfg.QueueSize)
	acks := make(chan ack, cfg.Workers)
	// Permits are returned only after durable contiguous acknowledgement, not when
	// a worker finishes. A stalled early batch cannot grow the reorder map forever.
	permits := make(chan struct{}, cfg.Workers+cfg.QueueSize)
	var wg sync.WaitGroup
	var once sync.Once
	var firstErr error
	fail := func(err error) { once.Do(func() { firstErr = err; cancel() }) }
	initial := m.Progress[index].Cursor
	wg.Go(func() {
		defer close(jobs)
		cursor := initial
		var seq int64
		if p.Upper == "" {
			return
		}
		for {
			select {
			case permits <- struct{}{}:
			case <-ctx.Done():
				return
			}
			var rows []model.Record
			var next model.Cursor
			err := e.operation(ctx, m, func(c context.Context) error {
				var readErr error
				rows, next, readErr = e.Source.Read(c, p.Source, p.Input, cursor, p.Upper, cfg.BatchSize)
				return readErr
			})
			if err != nil {
				fail(fmt.Errorf("read batch: %w", err))
				return
			}
			if len(rows) == 0 {
				<-permits
				return
			}
			if next == "" || next == cursor {
				fail(fmt.Errorf("source returned a non-advancing cursor"))
				return
			}
			transformed, size, err := model.Transform(rows, p.Fields)
			if err != nil {
				fail(fmt.Errorf("transform batch: %w", err))
				return
			}
			if size > cfg.MaxBatchBytes {
				fail(fmt.Errorf("batch exceeds max_batch_bytes; reduce batch_size"))
				return
			}
			b := batch{seq: seq, cursor: next, rows: transformed, bytes: size}
			select {
			case jobs <- b:
				cursor = next
				seq++
			case <-ctx.Done():
				return
			}
		}
	})
	for range cfg.Workers {
		wg.Go(func() {
			for {
				select {
				case <-ctx.Done():
					return
				case b, ok := <-jobs:
					if !ok {
						return
					}
					t := time.Now()
					if err := e.operation(ctx, m, func(c context.Context) error { return e.Destination.Write(c, p.Target, p.Output, b.rows) }); err != nil {
						fail(fmt.Errorf("write batch %d: %w", b.seq, err))
						return
					}
					e.Log.Info("batch migrated", "migration_id", m.ID, "source", p.Source, "destination", p.Target, "batch", b.seq, "records", len(b.rows), "duration", time.Since(t))
					select {
					case acks <- ack{seq: b.seq, cursor: b.cursor, records: int64(len(b.rows)), bytes: b.bytes}:
					case <-ctx.Done():
						return
					}
				}
			}
		})
	}
	go func() { wg.Wait(); close(acks) }()
	pending := map[int64]ack{}
	var next int64
	var saveErr error
	// Drain successful acknowledgements even during cancellation. Missing earlier
	// batches leave a gap; later successes are intentionally replayed on resume.
	for a := range acks {
		pending[a.seq] = a
		for saveErr == nil {
			item, ok := pending[next]
			if !ok {
				break
			}
			old := m.Progress[index]
			progress := &m.Progress[index]
			progress.Cursor = item.cursor
			progress.Records += item.records
			progress.Bytes += item.bytes
			m.ElapsedSeconds = previous + time.Since(started).Seconds()
			if err := e.Save(m); err != nil {
				m.Progress[index] = old
				saveErr = fmt.Errorf("save checkpoint: %w", err)
				fail(saveErr)
				break
			}
			delete(pending, next)
			next++
			<-permits
		}
	}
	// Closing acks follows wg.Wait, so firstErr is now safe to read.
	if firstErr != nil {
		if !errors.Is(firstErr, context.Canceled) {
			m.FailedBatches++
		}
		return firstErr
	}
	return parent.Err()
}
