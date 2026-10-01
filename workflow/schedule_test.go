package workflow

import (
	"context"
	"fmt"
	"io"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func testLogger(t *testing.T) Logger {
	t.Helper()
	return newLogger(testWriter{t})
}

type testWriter struct{ t *testing.T }

func (w testWriter) Write(p []byte) (int, error) { w.t.Logf("%s", p); return len(p), nil }

func diamondDefs() []TaskDef {
	return []TaskDef{
		{ID: "root"},
		{ID: "a", Deps: []string{"root"}},
		{ID: "b", Deps: []string{"root"}},
		{ID: "c", Deps: []string{"root"}},
		{ID: "join", Deps: []string{"a", "b", "c"}},
		{ID: "leaf1", Deps: []string{"join"}},
		{ID: "leaf2", Deps: []string{"join"}},
	}
}

func TestSchedulingFanInFanOut(t *testing.T) {
	dir := t.TempDir()
	wf, err := Build("diamond", diamondDefs())
	if err != nil {
		t.Fatal(err)
	}
	var (
		mu      sync.Mutex
		runs    = map[string]int{}
		order   []string
		current int32
		maxSeen int32
		release = make(chan struct{})
		blocked int32
	)
	const workers = 2
	eng, err := NewEngine(Config{StateDir: dir, MaxWorkers: workers, Logger: testLogger(t)})
	if err != nil {
		t.Fatal(err)
	}
	defer eng.Close()

	fns := map[string]TaskFunc{}
	for _, d := range diamondDefs() {
		id := d.ID
		fns[id] = func(tc TaskContext) error {
			n := atomic.AddInt32(&current, 1)
			for {
				old := atomic.LoadInt32(&maxSeen)
				if n <= old || atomic.CompareAndSwapInt32(&maxSeen, old, n) {
					break
				}
			}
			mu.Lock()
			runs[id]++
			order = append(order, id)
			mu.Unlock()

			// Block the fan-out layer to prove join waits for ALL of a/b/c.
			// Only `workers` tasks can be blocked at once under the cap.
			if id == "a" || id == "b" || id == "c" {
				if atomic.AddInt32(&blocked, 1) == workers {
					close(release)
				}
				<-release
			}
			atomic.AddInt32(&current, -1)
			return nil
		}
	}
	eng.Register(wf, fns)

	res, err := eng.Run(context.Background(), "run-1", wf)
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("INPUT diamond 7 tasks, workers=%d | SCHEDULE order=%v | FINAL status=%s", workers, order, res.Status)

	if res.Status != RunSucceeded {
		t.Fatalf("status=%s", res.Status)
	}
	// exactly once each
	for _, d := range diamondDefs() {
		if runs[d.ID] != 1 {
			t.Fatalf("task %s executed %d times, want 1", d.ID, runs[d.ID])
		}
		if res.Tasks[d.ID].Status != StatusSucceeded {
			t.Fatalf("task %s final=%s", d.ID, res.Tasks[d.ID].Status)
		}
	}
	// concurrency cap respected
	if maxSeen > workers {
		t.Fatalf("peak concurrency %d > cap %d", maxSeen, workers)
	}
	// precedence: root first, join after a/b/c, leaves after join
	pos := map[string]int{}
	for i, id := range order {
		pos[id] = i
	}
	if pos["root"] != 0 {
		t.Fatalf("root not first: %v", order)
	}
	for _, m := range []string{"a", "b", "c"} {
		if pos[m] > pos["join"] {
			t.Fatalf("join ran before %s: %v", m, order)
		}
	}
	if pos["join"] > pos["leaf1"] || pos["join"] > pos["leaf2"] {
		t.Fatalf("leaf before join: %v", order)
	}
}

func TestSchedulingDeterministicAcrossRuns(t *testing.T) {
	// Same input graph run in 5 fresh directories must produce the same
	// task-start order (ready queue is ordered by definition rank).
	var observed [][]string
	for iter := 0; iter < 5; iter++ {
		dir := t.TempDir()
		wf, _ := Build("det", diamondDefs())
		var mu sync.Mutex
		var order []string
		fns := map[string]TaskFunc{}
		for _, d := range diamondDefs() {
			id := d.ID
			fns[id] = func(tc TaskContext) error {
				mu.Lock()
				order = append(order, id)
				mu.Unlock()
				time.Sleep(time.Millisecond)
				return nil
			}
		}
		eng, err := NewEngine(Config{StateDir: dir, MaxWorkers: 4, Logger: discardLogger{}})
		if err != nil {
			t.Fatal(err)
		}
		eng.Register(wf, fns)
		res, err := eng.Run(context.Background(), fmt.Sprintf("det-%d", iter), wf)
		eng.Close()
		if err != nil {
			t.Fatal(err)
		}
		if res.Status != RunSucceeded {
			t.Fatalf("iter %d status=%s", iter, res.Status)
		}
		observed = append(observed, order)
		t.Logf("RUN %d dispatch order=%v", iter, order)
	}
	for i := 1; i < len(observed); i++ {
		if fmt.Sprint(observed[i]) != fmt.Sprint(observed[0]) {
			t.Fatalf("non-deterministic order: %v vs %v", observed[0], observed[i])
		}
	}
}

func TestIndependentBranchesSurviveFailure(t *testing.T) {
	dir := t.TempDir()
	defs := []TaskDef{
		{ID: "start"},
		{ID: "bad", Deps: []string{"start"}, Retry: RetryPolicy{MaxAttempts: 2}},
		{ID: "good", Deps: []string{"start"}},
		{ID: "badChild", Deps: []string{"bad"}},
	}
	wf, _ := Build("branches", defs)
	eng, _ := NewEngine(Config{StateDir: dir, MaxWorkers: 3, Logger: testLogger(t)})
	defer eng.Close()
	var badRuns int32
	fns := map[string]TaskFunc{
		"start": func(TaskContext) error { return nil },
		"bad": func(TaskContext) error {
			atomic.AddInt32(&badRuns, 1)
			return PermanentError("always broken")
		},
		"good":     func(TaskContext) error { return nil },
		"badChild": func(TaskContext) error { return nil },
	}
	eng.Register(wf, fns)
	res, err := eng.Run(context.Background(), "r", wf)
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("INPUT branches | FINAL start=%s bad=%s good=%s badChild=%s",
		res.Tasks["start"].Status, res.Tasks["bad"].Status, res.Tasks["good"].Status, res.Tasks["badChild"].Status)
	if res.Status != RunFailed {
		t.Fatalf("status=%s", res.Status)
	}
	if res.Tasks["bad"].Status != StatusFailed || badRuns != 1 {
		t.Fatalf("bad: status=%s runs=%d (permanent failure must not retry)", res.Tasks["bad"].Status, badRuns)
	}
	if res.Tasks["badChild"].Status != StatusSkipped {
		t.Fatalf("badChild=%s, want SKIPPED", res.Tasks["badChild"].Status)
	}
	if res.Tasks["good"].Status != StatusSucceeded {
		t.Fatalf("good=%s, want SUCCEEDED (failure must not cancel independent branch)", res.Tasks["good"].Status)
	}
}

var _ = io.Discard

func TestMaxWorkersOneIsStrictlySerial(t *testing.T) {
	dir := t.TempDir()
	defs := []TaskDef{
		{ID: "a"},
		{ID: "b"},
		{ID: "c"},
	}
	wf, _ := Build("serial", defs)
	var peak, cur int32
	var mu sync.Mutex
	var order []string
	fns := map[string]TaskFunc{}
	for _, d := range defs {
		id := d.ID
		fns[id] = func(TaskContext) error {
			n := atomic.AddInt32(&cur, 1)
			for {
				old := atomic.LoadInt32(&peak)
				if n <= old || atomic.CompareAndSwapInt32(&peak, old, n) {
					break
				}
			}
			time.Sleep(2 * time.Millisecond)
			atomic.AddInt32(&cur, -1)
			mu.Lock()
			order = append(order, id)
			mu.Unlock()
			return nil
		}
	}
	eng, _ := NewEngine(Config{StateDir: dir, MaxWorkers: 1, Logger: discardLogger{}})
	defer eng.Close()
	eng.Register(wf, fns)
	res, err := eng.Run(context.Background(), "r", wf)
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("INPUT 3 independent tasks workers=1 -> peak=%d order=%v status=%s", peak, order, res.Status)
	if peak != 1 {
		t.Fatalf("peak concurrency=%d want 1", peak)
	}
	if fmt.Sprint(order) != "[a b c]" {
		t.Fatalf("order=%v want definition order", order)
	}
}
