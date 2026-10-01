package workflow

import (
	"context"
	"time"
)

// Clock abstracts time so retry/backoff logic can be tested deterministically.
type Clock interface {
	Now() time.Time
	Sleep(ctx context.Context, d time.Duration) error
}

type realClock struct{}

// NewRealClock returns a wall-clock Clock.
func NewRealClock() Clock { return realClock{} }

func (realClock) Now() time.Time { return time.Now() }

func (realClock) Sleep(ctx context.Context, d time.Duration) error {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-t.C:
		return nil
	}
}
