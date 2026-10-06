package retry

import (
	"context"
	"errors"
	"math/rand/v2"
	"net"
	"time"
)

type temporary interface{ Temporary() bool }

func Transient(err error) bool {
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return false
	}
	var t temporary
	if errors.As(err, &t) {
		return t.Temporary()
	}
	var n net.Error
	return errors.As(err, &n) && n.Timeout()
}

// Do retries only classified transient errors. Cancellation interrupts the backoff.
func Do(ctx context.Context, retries int, fn func() error) error {
	for attempt := 0; ; attempt++ {
		if err := ctx.Err(); err != nil {
			return err
		}
		err := fn()
		if err == nil || attempt >= retries || !Transient(err) {
			return err
		}
		ceiling := min(5*time.Second, 100*time.Millisecond*time.Duration(1<<attempt))
		delay := ceiling/2 + time.Duration(rand.Int64N(int64(ceiling/2)))
		timer := time.NewTimer(delay)
		select {
		case <-ctx.Done():
			timer.Stop()
			return ctx.Err()
		case <-timer.C:
		}
	}
}
