package retry

import (
	"context"
	"errors"
	"testing"
	"time"
)

type transient struct{}

func (transient) Error() string   { return "temporary" }
func (transient) Temporary() bool { return true }
func TestRetry(t *testing.T) {
	for _, tt := range []struct {
		name string
		err  error
		want int
	}{{"transient", transient{}, 3}, {"permanent", errors.New("bad"), 1}, {"success", nil, 1}} {
		t.Run(tt.name, func(t *testing.T) {
			n := 0
			_ = Do(context.Background(), 2, func() error { n++; return tt.err })
			if n != tt.want {
				t.Fatalf("attempts %d", n)
			}
		})
	}
}
func TestCancelBackoff(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	n := 0
	start := time.Now()
	err := Do(ctx, 10, func() error { n++; cancel(); return transient{} })
	if !errors.Is(err, context.Canceled) || n != 1 || time.Since(start) > time.Second {
		t.Fatalf("%d %v", n, err)
	}
}
