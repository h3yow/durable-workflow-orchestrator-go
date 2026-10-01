package workflow

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// A retryable failure twice, then success: attempts == 3, task Succeeded.
func TestRetryableThenSuccess(t *testing.T) {
	eng, _, lb := newTestEngine(t)
	def := WorkflowDef{Name: "retry", Concurrency: 1, Tasks: []TaskDef{
		withRetry("flaky", msRetry(time.Second, 3, time.Millisecond, 10*time.Millisecond)),
	}}
	var calls int32
	reg := Registry{
		"flaky": func(_ context.Context, in TaskInput) (string, error) {
			n := atomic.AddInt32(&calls, 1)
			if n < 3 {
				return "", Retryable(errors.New("transient"))
			}
			return "ok", nil
		},
	}
	res, err := eng.Start(context.Background(), def, reg)
	if err != nil {
		t.Fatal(err)
	}
	assertStatus(t, res, "flaky", StatusSucceeded)
	if calls != 3 {
		t.Fatalf("expected 3 attempts, got %d", calls)
	}
	if res.Tasks[0].Attempts != 3 {
		t.Fatalf("recorded attempts=%d", res.Tasks[0].Attempts)
	}
	t.Logf("input: fail twice with Retryable; decision log:\n%s", lb.String())
}

// Exhaust retries: task Failed, all transitive downstream Skipped,
// unrelated branch keeps running and Succeeds.
func TestRetriesExhaustedDownstreamSkipped(t *testing.T) {
	eng, _, lb := newTestEngine(t)
	def := WorkflowDef{Name: "fail", Concurrency: 4, Tasks: []TaskDef{
		task("bad"),
		task("down1", "bad"),
		task("down2", "down1"),
		task("other"),
		task("other-child", "other"),
	}}
	def.Tasks[0].Retry = msRetry(time.Second, 2, time.Millisecond, 5*time.Millisecond)
	var badCalls int32
	ok := func(_ context.Context, in TaskInput) (string, error) { return "ok", nil }
	reg := Registry{
		"bad": func(_ context.Context, in TaskInput) (string, error) {
			atomic.AddInt32(&badCalls, 1)
			return "", Retryable(errors.New("always broken"))
		},
		"down1":       ok,
		"down2":       ok,
		"other":       ok,
		"other-child": ok,
	}
	res, err := eng.Start(context.Background(), def, reg)
	if err != nil {
		t.Fatal(err)
	}
	if res.Status != StatusFailed {
		t.Fatalf("run status=%s want Failed", res.Status)
	}
	assertStatus(t, res, "bad", StatusFailed)
	assertStatus(t, res, "down1", StatusSkipped)
	assertStatus(t, res, "down2", StatusSkipped)
	assertStatus(t, res, "other", StatusSucceeded)
	assertStatus(t, res, "other-child", StatusSucceeded)
	if badCalls != 3 {
		t.Fatalf("bad attempts=%d want 3 (1 + 2 retries)", badCalls)
	}
	t.Logf("input: bad always retryable, maxRetries=2; decision log:\n%s", lb.String())
}

// A Permanent error fails immediately without retrying.
func TestPermanentFailureNoRetry(t *testing.T) {
	eng, _, lb := newTestEngine(t)
	def := WorkflowDef{Name: "perm", Concurrency: 1, Tasks: []TaskDef{
		withRetry("p", msRetry(time.Second, 5, time.Millisecond, 5*time.Millisecond)),
	}}
	var calls int32
	reg := Registry{"p": func(context.Context, TaskInput) (string, error) {
		atomic.AddInt32(&calls, 1)
		return "", Permanent(errors.New("fatal config"))
	}}
	res, err := eng.Start(context.Background(), def, reg)
	if err != nil {
		t.Fatal(err)
	}
	assertStatus(t, res, "p", StatusFailed)
	if calls != 1 {
		t.Fatalf("permanent error had %d attempts, want 1", calls)
	}
	t.Logf("decision log:\n%s", lb.String())
}

// Per-task timeout is a retryable failure; exhausting retries fails the task.
func TestTaskTimeoutRetriedThenFailed(t *testing.T) {
	eng, _, lb := newTestEngine(t)
	def := WorkflowDef{Name: "tt", Concurrency: 1, Tasks: []TaskDef{
		withRetry("slow", msRetry(20*time.Millisecond, 1, time.Millisecond, 5*time.Millisecond)),
	}}
	var calls int32
	reg := Registry{"slow": func(ctx context.Context, in TaskInput) (string, error) {
		atomic.AddInt32(&calls, 1)
		<-ctx.Done()
		return "", ctx.Err()
	}}
	res, err := eng.Start(context.Background(), def, reg)
	if err != nil {
		t.Fatal(err)
	}
	assertStatus(t, res, "slow", StatusFailed)
	if calls != 2 {
		t.Fatalf("timeout attempts=%d want 2", calls)
	}
	t.Logf("decision log (timeout -> retry -> exhausted):\n%s", lb.String())
}

// Run-level timeout cascades: a long task is canceled and waiting tasks are
// marked Canceled; already finished tasks stay Succeeded.
func TestRunTimeoutCascades(t *testing.T) {
	eng, _, lb := newTestEngine(t)
	def := WorkflowDef{
		Name: "rt", Concurrency: 2, RunTimeout: Duration(60 * time.Millisecond),
		Tasks: []TaskDef{
			task("fast"),
			task("slow"),
			task("waiter", "slow"),
		},
	}
	reg := Registry{
		"fast": func(context.Context, TaskInput) (string, error) { return "done", nil },
		"slow": func(ctx context.Context, in TaskInput) (string, error) {
			<-ctx.Done()
			return "", ctx.Err()
		},
		"waiter": func(context.Context, TaskInput) (string, error) { return "never", nil },
	}
	res, err := eng.Start(context.Background(), def, reg)
	if err != nil {
		t.Fatal(err)
	}
	if res.Status != StatusTimedOut {
		t.Fatalf("run status=%s want TimedOut", res.Status)
	}
	assertStatus(t, res, "fast", StatusSucceeded)  // completed result preserved
	assertStatus(t, res, "slow", StatusCanceled)   // in-flight canceled
	assertStatus(t, res, "waiter", StatusCanceled) // waiting never started
	t.Logf("decision log (run timeout cascade):\n%s", lb.String())
}

// External cancellation cascades to in-flight and waiting tasks, completed
// tasks are preserved.
func TestExternalCancelCascades(t *testing.T) {
	eng, _, lb := newTestEngine(t)
	def := WorkflowDef{
		Name: "cx", Concurrency: 1,
		Tasks: []TaskDef{task("fast"), task("slow"), task("waiter", "slow")},
	}
	started := make(chan struct{}, 1)
	reg := Registry{
		"fast": func(context.Context, TaskInput) (string, error) { return "done", nil },
		"slow": func(ctx context.Context, in TaskInput) (string, error) {
			started <- struct{}{}
			<-ctx.Done()
			return "", ctx.Err()
		},
		"waiter": func(context.Context, TaskInput) (string, error) { return "x", nil },
	}
	ctx, cancel := context.WithCancel(context.Background())
	var wg sync.WaitGroup
	wg.Add(1)
	var res *RunResult
	var rerr error
	go func() { defer wg.Done(); res, rerr = eng.Start(ctx, def, reg) }()
	<-started
	time.Sleep(20 * time.Millisecond)
	cancel()
	wg.Wait()
	if rerr != nil {
		t.Fatal(rerr)
	}
	if res.Status != StatusCanceled {
		t.Fatalf("run status=%s want Canceled", res.Status)
	}
	assertStatus(t, res, "fast", StatusSucceeded)
	assertStatus(t, res, "slow", StatusCanceled)
	assertStatus(t, res, "waiter", StatusCanceled)
	t.Logf("decision log (external cancel cascade):\n%s", lb.String())
}
