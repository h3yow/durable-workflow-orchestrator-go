package workflow

import (
	"context"
	"errors"
	"fmt"
	"sync/atomic"
	"testing"
	"time"
)

func TestRetryExhaustionMarksFailedAndSkipsDownstream(t *testing.T) {
	dir := t.TempDir()
	defs := []TaskDef{
		{ID: "flaky", Retry: RetryPolicy{MaxAttempts: 3, Backoff: BackoffPolicy{Kind: BackoffFixed, Interval: Duration(time.Millisecond)}}},
		{ID: "child", Deps: []string{"flaky"}},
		{ID: "other"},
	}
	wf, _ := Build("retry-chain", defs)
	var attempts int32
	eng, _ := NewEngine(Config{StateDir: dir, MaxWorkers: 2, Logger: testLogger(t)})
	defer eng.Close()
	eng.Register(wf, map[string]TaskFunc{
		"flaky": func(TaskContext) error {
			n := atomic.AddInt32(&attempts, 1)
			return fmt.Errorf("transient failure #%d", n) // plain error => retryable
		},
		"child": func(TaskContext) error { t.Error("child must never run"); return nil },
		"other": func(TaskContext) error { return nil },
	})
	res, err := eng.Run(context.Background(), "r", wf)
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("INPUT flaky max_attempts=3 -> flaky attempts=%d status=%s child=%s other=%s | FINAL status=%s",
		attempts, res.Tasks["flaky"].Status, res.Tasks["child"].Status, res.Tasks["other"].Status, res.Status)
	if attempts != 3 {
		t.Fatalf("attempts=%d want 3", attempts)
	}
	if res.Tasks["flaky"].Status != StatusFailed || !res.Tasks["flaky"].Retryable {
		t.Fatalf("flaky=%+v", res.Tasks["flaky"])
	}
	if res.Tasks["child"].Status != StatusSkipped {
		t.Fatalf("child=%s want SKIPPED", res.Tasks["child"].Status)
	}
	if res.Tasks["other"].Status != StatusSucceeded {
		t.Fatalf("other=%s", res.Tasks["other"].Status)
	}
	if res.Status != RunFailed {
		t.Fatalf("run=%s", res.Status)
	}
}

func TestPermanentFailureNotRetried(t *testing.T) {
	dir := t.TempDir()
	defs := []TaskDef{{ID: "p", Retry: RetryPolicy{MaxAttempts: 5}}}
	wf, _ := Build("perm", defs)
	var n int32
	eng, _ := NewEngine(Config{StateDir: dir, MaxWorkers: 1, Logger: testLogger(t)})
	defer eng.Close()
	eng.Register(wf, map[string]TaskFunc{"p": func(TaskContext) error {
		atomic.AddInt32(&n, 1)
		return PermanentError("cannot divide by zero")
	}})
	res, _ := eng.Run(context.Background(), "r", wf)
	t.Logf("INPUT permanent with max_attempts=5 -> attempts=%d status=%s retryable=%v",
		n, res.Tasks["p"].Status, res.Tasks["p"].Retryable)
	if n != 1 {
		t.Fatalf("attempts=%d want 1", n)
	}
	if res.Tasks["p"].Status != StatusFailed || res.Tasks["p"].Retryable {
		t.Fatalf("p=%+v", res.Tasks["p"])
	}
}

func TestBackoffKinds(t *testing.T) {
	cases := []struct {
		kind     BackoffKind
		interval time.Duration
		retry    int
		want     time.Duration
	}{
		{BackoffFixed, 10 * time.Millisecond, 1, 10 * time.Millisecond},
		{BackoffFixed, 10 * time.Millisecond, 3, 10 * time.Millisecond},
		{BackoffLinear, 10 * time.Millisecond, 3, 30 * time.Millisecond},
		{BackoffExponent, 10 * time.Millisecond, 1, 10 * time.Millisecond},
		{BackoffExponent, 10 * time.Millisecond, 3, 40 * time.Millisecond},
	}
	for _, c := range cases {
		got := backoffDelay(BackoffPolicy{Kind: c.kind, Interval: Duration(c.interval), Multiplier: 2}, c.retry)
		if got != c.want {
			t.Fatalf("%s retry=%d: got %s want %s", c.kind, c.retry, got, c.want)
		}
		t.Logf("BACKOFF kind=%s retry=%d -> %s", c.kind, c.retry, got)
	}
	// max delay cap
	got := backoffDelay(BackoffPolicy{Kind: BackoffExponent, Interval: Duration(10 * time.Millisecond), MaxDelay: Duration(15 * time.Millisecond)}, 4)
	if got != 15*time.Millisecond {
		t.Fatalf("cap got %s", got)
	}
}

func TestAttemptTimeoutIsRetryableThenFails(t *testing.T) {
	dir := t.TempDir()
	defs := []TaskDef{
		{ID: "slow", Timeout: Duration(15 * time.Millisecond), Retry: RetryPolicy{MaxAttempts: 2, Backoff: BackoffPolicy{Kind: BackoffFixed, Interval: Duration(time.Millisecond)}}},
		{ID: "after", Deps: []string{"slow"}},
	}
	wf, _ := Build("timeout", defs)
	var n int32
	eng, _ := NewEngine(Config{StateDir: dir, MaxWorkers: 1, Logger: testLogger(t)})
	defer eng.Close()
	eng.Register(wf, map[string]TaskFunc{
		"slow": func(tc TaskContext) error {
			atomic.AddInt32(&n, 1)
			<-tc.Done() // cooperative: respects the per-attempt timeout
			return errors.New("should be superseded by timeout")
		},
		"after": func(TaskContext) error { t.Error("after must not run"); return nil },
	})
	res, err := eng.Run(context.Background(), "r", wf)
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("INPUT slow timeout=15ms attempts=2 -> attempts=%d slow=%s after=%s err=%q",
		atomic.LoadInt32(&n), res.Tasks["slow"].Status, res.Tasks["after"].Status, res.Tasks["slow"].Error)
	if n != 2 {
		t.Fatalf("attempts=%d want 2", n)
	}
	if res.Tasks["slow"].Status != StatusFailed {
		t.Fatalf("slow=%s", res.Tasks["slow"].Status)
	}
	if res.Tasks["after"].Status != StatusSkipped {
		t.Fatalf("after=%s", res.Tasks["after"].Status)
	}
}

func TestRunTimeoutCascadesCancellation(t *testing.T) {
	dir := t.TempDir()
	defs := []TaskDef{
		{ID: "blocker"},
		{ID: "waiting", Deps: []string{"blocker"}},
	}
	wf, _ := Build("run-timeout", defs)
	eng, _ := NewEngine(Config{
		StateDir:   dir,
		MaxWorkers: 2,
		RunTimeout: Duration(40 * time.Millisecond),
		Logger:     testLogger(t),
	})
	defer eng.Close()
	eng.Register(wf, map[string]TaskFunc{
		"blocker": func(tc TaskContext) error {
			<-tc.Done() // runs until run timeout
			return nil
		},
		"waiting": func(TaskContext) error { t.Error("waiting must not start"); return nil },
	})
	start := time.Now()
	res, err := eng.Run(context.Background(), "r", wf)
	elapsed := time.Since(start)
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("INPUT run_timeout=40ms -> elapsed=%s blocker=%s waiting=%s | FINAL status=%s",
		elapsed.Round(time.Millisecond), res.Tasks["blocker"].Status, res.Tasks["waiting"].Status, res.Status)
	if res.Status != RunCanceled {
		t.Fatalf("status=%s want CANCELED", res.Status)
	}
	if res.Tasks["blocker"].Status != StatusCanceled {
		t.Fatalf("blocker=%s want CANCELED (running task cascaded)", res.Tasks["blocker"].Status)
	}
	if res.Tasks["waiting"].Status != StatusCanceled {
		t.Fatalf("waiting=%s want CANCELED (waiting task cascaded)", res.Tasks["waiting"].Status)
	}
	if elapsed > 2*time.Second {
		t.Fatalf("cancellation took too long: %s", elapsed)
	}
}

func TestContextCancelKeepsCompletedResults(t *testing.T) {
	dir := t.TempDir()
	defs := []TaskDef{
		{ID: "done"},
		{ID: "blocker"},
		{ID: "later", Deps: []string{"blocker"}},
	}
	wf, _ := Build("ctx-cancel", defs)
	releaseBlocker := make(chan struct{})
	eng, _ := NewEngine(Config{StateDir: dir, MaxWorkers: 3, Logger: testLogger(t)})
	defer eng.Close()
	eng.Register(wf, map[string]TaskFunc{
		"done": func(TaskContext) error { return nil },
		"blocker": func(tc TaskContext) error {
			select {
			case <-releaseBlocker:
				return nil
			case <-tc.Done():
				return nil
			}
		},
		"later": func(TaskContext) error { return nil },
	})
	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		time.Sleep(40 * time.Millisecond)
		cancel()
	}()
	res, err := eng.Run(ctx, "r", wf)
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("INPUT cancel during blocker -> done=%s blocker=%s later=%s | FINAL status=%s",
		res.Tasks["done"].Status, res.Tasks["blocker"].Status, res.Tasks["later"].Status, res.Status)
	if res.Tasks["done"].Status != StatusSucceeded {
		t.Fatalf("completed task done=%s must keep its result", res.Tasks["done"].Status)
	}
	if res.Status != RunCanceled {
		t.Fatalf("status=%s", res.Status)
	}
}
