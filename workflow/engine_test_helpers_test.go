package workflow

import (
	"bytes"
	"fmt"
	"sync"
	"testing"
	"time"
)

// logBuffer is a goroutine-safe sink for the engine decision log.
type logBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (l *logBuffer) logf(format string, args ...any) {
	l.mu.Lock()
	defer l.mu.Unlock()
	fmt.Fprintf(&l.buf, format+"\n", args...)
}

func (l *logBuffer) String() string {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.buf.String()
}

func newTestEngine(t *testing.T) (*Engine, *FileStore, *logBuffer) {
	t.Helper()
	store, err := NewFileStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	lb := &logBuffer{}
	return NewEngine(Options{Store: store, Clock: NewRealClock(), Logf: lb.logf}), store, lb
}

func task(id string, deps ...string) TaskDef {
	return TaskDef{ID: id, Dependencies: deps}
}

func withRetry(id string, p RetryPolicy, deps ...string) TaskDef {
	t := task(id, deps...)
	t.Retry = p
	return t
}

func msRetry(timeout time.Duration, max int, bo, maxBo time.Duration) RetryPolicy {
	return RetryPolicy{
		Timeout:        Duration(timeout),
		MaxRetries:     max,
		Backoff:        BackoffFixed,
		InitialBackoff: Duration(bo),
		MaxBackoff:     Duration(maxBo),
	}
}

func assertStatus(t *testing.T, res *RunResult, id string, want Status) {
	t.Helper()
	for _, tr := range res.Tasks {
		if tr.ID == id {
			if tr.Status != want {
				t.Fatalf("task %s: want %s got %s (reason=%s)", id, want, tr.Status, tr.Reason)
			}
			return
		}
	}
	t.Fatalf("task %s missing from result", id)
}
