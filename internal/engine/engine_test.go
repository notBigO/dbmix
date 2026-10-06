package engine

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/notBigO/dbmix/internal/config"
	"github.com/notBigO/dbmix/internal/model"
	"github.com/notBigO/dbmix/internal/planner"
	"github.com/notBigO/dbmix/internal/state"
)

type fakeSource struct {
	total int
	reads atomic.Int64
}

func (s *fakeSource) Inspect(context.Context, config.Entity) (model.Inspection, error) {
	return model.Inspection{}, nil
}
func (s *fakeSource) Close() error { return nil }
func (s *fakeSource) Read(_ context.Context, _ string, _ model.Schema, after, _ model.Cursor, limit int) ([]model.Record, model.Cursor, error) {
	s.reads.Add(1)
	start, _ := strconv.Atoi(string(after))
	out := []model.Record{}
	for i := start + 1; i <= s.total && len(out) < limit; i++ {
		out = append(out, model.Record{"id": int64(i)})
		after = model.Cursor(strconv.Itoa(i))
	}
	return out, after, nil
}

type fakeDest struct {
	mu      sync.Mutex
	records map[int64]bool
	before  func(context.Context, int64) error
}

func (d *fakeDest) Validate(context.Context, string, model.Schema) error { return nil }
func (d *fakeDest) Prepare(context.Context, string, model.Schema) error  { return nil }
func (d *fakeDest) Close() error                                         { return nil }
func (d *fakeDest) Write(ctx context.Context, _ string, _ model.Schema, rows []model.Record) error {
	if d.before != nil {
		if err := d.before(ctx, rows[0]["id"].(int64)); err != nil {
			return err
		}
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	for _, r := range rows {
		d.records[r["id"].(int64)] = true
	}
	return nil
}
func fixture(total int) (*Engine, *state.Migration, *fakeSource, *fakeDest) {
	schema := model.Schema{Key: "id", Fields: []model.Field{{Name: "id", Type: model.Int64}}}
	p := planner.Plan{Config: config.Config{BatchSize: 1, Workers: 3, QueueSize: 2, MaxBatchBytes: 1024, TimeoutSeconds: 5}, Entities: []planner.Entity{{Source: "s", Target: "d", Input: schema, Output: schema, Upper: model.Cursor(strconv.Itoa(total)), Fields: []model.Projection{{Source: "id", Target: schema.Fields[0]}}}}}
	m := state.New(p)
	s := &fakeSource{total: total}
	d := &fakeDest{records: map[int64]bool{}}
	return &Engine{Source: s, Destination: d, Save: func(*state.Migration) error { return nil }, Log: slog.New(slog.NewTextHandler(io.Discard, nil))}, &m, s, d
}
func TestOutOfOrderWindow(t *testing.T) {
	e, m, s, d := fixture(100)
	blocked := make(chan struct{})
	later := make(chan struct{}, 4)
	d.before = func(ctx context.Context, id int64) error {
		if id == 1 {
			select {
			case <-blocked:
			case <-ctx.Done():
				return ctx.Err()
			}
		} else if id <= 5 {
			later <- struct{}{}
		}
		return nil
	}
	var maxCursor atomic.Int64
	e.Save = func(m *state.Migration) error {
		n, _ := strconv.ParseInt(string(m.Progress[0].Cursor), 10, 64)
		maxCursor.Store(n)
		return nil
	}
	done := make(chan error, 1)
	go func() { done <- e.Run(context.Background(), m) }()
	for range 4 {
		select {
		case <-later:
		case <-time.After(3 * time.Second):
			t.Fatal("writers stalled")
		}
	}
	if s.reads.Load() > 5 || maxCursor.Load() != 0 {
		t.Fatal("window unbounded or checkpoint passed gap")
	}
	close(blocked)
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	if m.Progress[0].Records != 100 || len(d.records) != 100 || m.Status != "completed" {
		t.Fatalf("%+v", m.Progress)
	}
}
func TestFailureResume(t *testing.T) {
	e, m, _, d := fixture(40)
	d.before = func(_ context.Context, id int64) error {
		if id == 10 {
			return errors.New("permanent failure")
		}
		return nil
	}
	if err := e.Run(context.Background(), m); err == nil || m.Status != "failed" {
		t.Fatal("expected failure")
	}
	n, _ := strconv.Atoi(string(m.Progress[0].Cursor))
	if n >= 10 {
		t.Fatal("checkpoint crossed failure")
	}
	d.before = nil
	if err := e.Run(context.Background(), m); err != nil {
		t.Fatal(err)
	}
	if len(d.records) != 40 || m.Progress[0].Records != 40 {
		t.Fatal("resume lost or overcounted rows")
	}
}
func TestCancellation(t *testing.T) {
	e, m, _, d := fixture(40)
	ctx, cancel := context.WithCancel(context.Background())
	d.before = func(ctx context.Context, _ int64) error { cancel(); <-ctx.Done(); return ctx.Err() }
	err := e.Run(ctx, m)
	if !errors.Is(err, context.Canceled) || m.Status != "interrupted" {
		t.Fatalf("%v %s", err, m.Status)
	}
}
func TestCheckpointFailureReplays(t *testing.T) {
	e, m, _, d := fixture(10)
	failed := false
	e.Save = func(m *state.Migration) error {
		if m.Progress[0].Records > 0 && !failed {
			failed = true
			return errors.New("disk unavailable")
		}
		return nil
	}
	if err := e.Run(context.Background(), m); err == nil {
		t.Fatal("state error swallowed")
	}
	if m.Progress[0].Records != 0 {
		t.Fatal("failed checkpoint retained")
	}
	e.Save = func(*state.Migration) error { return nil }
	if err := e.Run(context.Background(), m); err != nil {
		t.Fatal(err)
	}
	if len(d.records) != 10 || m.Progress[0].Records != 10 {
		t.Fatal("bad replay")
	}
}
func TestBatchByteLimit(t *testing.T) {
	e, m, _, _ := fixture(10)
	m.Plan.Config.MaxBatchBytes = 1
	if err := e.Run(context.Background(), m); err == nil {
		t.Fatal("byte limit ignored")
	}
}
func BenchmarkEngine(b *testing.B) {
	for b.Loop() {
		e, m, _, _ := fixture(10000)
		m.Plan.Config.BatchSize = 100
		if err := e.Run(context.Background(), m); err != nil {
			b.Fatal(err)
		}
	}
}
