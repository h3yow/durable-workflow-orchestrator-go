package workflow

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"
)

func seedJournal(t *testing.T, dir, runID string, wf *Workflow, evs ...Event) {
	t.Helper()
	j, err := openJournal(dir, runID)
	if err != nil {
		t.Fatal(err)
	}
	defer j.Close()
	hash, err := DefinitionHash(wf)
	if err != nil {
		t.Fatal(err)
	}
	if err := j.Append(Event{Type: EvRunStarted, RunID: runID, DefHash: hash, TS: time.Now()}); err != nil {
		t.Fatal(err)
	}
	for _, ev := range evs {
		if ev.TS.IsZero() {
			ev.TS = time.Now()
		}
		if err := j.Append(ev); err != nil {
			t.Fatal(err)
		}
	}
}

func chainDefs() []TaskDef {
	return []TaskDef{
		{ID: "a"},
		{ID: "b", Deps: []string{"a"}},
		{ID: "c", Deps: []string{"b"}},
	}
}

func TestResumeSkipsCompletedTasks(t *testing.T) {
	dir := t.TempDir()
	wf, _ := Build("resume", chainDefs())
	seedJournal(t, dir, "r1", wf,
		Event{Type: EvTaskStarted, TaskID: "a", Attempt: 0, Attempts: 1},
		Event{Type: EvTaskSucceeded, TaskID: "a", Attempts: 1, Result: "ok"},
		Event{Type: EvTaskStarted, TaskID: "b", Attempt: 0, Attempts: 1},
		Event{Type: EvTaskSucceeded, TaskID: "b", Attempts: 1, Result: "ok"},
	)
	var execA, execB, execC int32
	eng, _ := NewEngine(Config{StateDir: dir, MaxWorkers: 2, Logger: testLogger(t)})
	defer eng.Close()
	eng.Register(wf, map[string]TaskFunc{
		"a": func(TaskContext) error { atomic.AddInt32(&execA, 1); return nil },
		"b": func(TaskContext) error { atomic.AddInt32(&execB, 1); return nil },
		"c": func(TaskContext) error { atomic.AddInt32(&execC, 1); return nil },
	})
	res, err := eng.Run(context.Background(), "r1", wf)
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("RESUME INPUT a=SUCCEEDED,b=SUCCEEDED durable -> executions a=%d b=%d c=%d | FINAL %s %s %s",
		execA, execB, execC, res.Tasks["a"].Status, res.Tasks["b"].Status, res.Tasks["c"].Status)
	if execA != 0 || execB != 0 {
		t.Fatalf("completed tasks re-executed: a=%d b=%d (must be 0)", execA, execB)
	}
	if execC != 1 {
		t.Fatalf("c executions=%d want 1 (must not be skipped)", execC)
	}
	if res.Status != RunSucceeded {
		t.Fatalf("status=%s", res.Status)
	}
}

func TestResumeReExecutesInterruptedTask(t *testing.T) {
	dir := t.TempDir()
	wf, _ := Build("crash", chainDefs())
	// Process died AFTER persisting b STARTED but BEFORE its verdict.
	seedJournal(t, dir, "r1", wf,
		Event{Type: EvTaskStarted, TaskID: "a", Attempt: 0, Attempts: 1},
		Event{Type: EvTaskSucceeded, TaskID: "a", Attempts: 1},
		Event{Type: EvTaskStarted, TaskID: "b", Attempt: 0, Attempts: 1},
	)
	var execB, execC int32
	eng, _ := NewEngine(Config{StateDir: dir, MaxWorkers: 2, Logger: testLogger(t)})
	defer eng.Close()
	eng.Register(wf, map[string]TaskFunc{
		"a": func(TaskContext) error { t.Error("a must not re-run"); return nil },
		"b": func(TaskContext) error { atomic.AddInt32(&execB, 1); return nil },
		"c": func(TaskContext) error { atomic.AddInt32(&execC, 1); return nil },
	})
	res, err := eng.Run(context.Background(), "r1", wf)
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("CRASH INPUT b=RUNNING durable -> executions b=%d c=%d | FINAL a=%s b=%s(%d attempts) c=%s",
		execB, execC, res.Tasks["a"].Status, res.Tasks["b"].Status, res.Tasks["b"].Attempts, res.Tasks["c"].Status)
	if execB != 1 || execC != 1 {
		t.Fatalf("b=%d c=%d want 1,1", execB, execC)
	}
	if res.Tasks["b"].Status != StatusSucceeded || res.Tasks["c"].Status != StatusSucceeded {
		t.Fatal("interrupted chain did not complete")
	}
}

func TestResumeFinishedRunIsIdempotent(t *testing.T) {
	dir := t.TempDir()
	wf, _ := Build("done", chainDefs())
	seedJournal(t, dir, "r1", wf,
		Event{Type: EvTaskStarted, TaskID: "a", Attempt: 0, Attempts: 1},
		Event{Type: EvTaskSucceeded, TaskID: "a", Attempts: 1},
		Event{Type: EvTaskStarted, TaskID: "b", Attempt: 0, Attempts: 1},
		Event{Type: EvTaskSucceeded, TaskID: "b", Attempts: 1},
		Event{Type: EvTaskStarted, TaskID: "c", Attempt: 0, Attempts: 1},
		Event{Type: EvTaskSucceeded, TaskID: "c", Attempts: 1},
		Event{Type: EvRunFinished, Status: RunSucceeded},
	)
	var ran int32
	eng, _ := NewEngine(Config{StateDir: dir, MaxWorkers: 2, Logger: testLogger(t)})
	defer eng.Close()
	noRun := func(TaskContext) error { atomic.AddInt32(&ran, 1); return nil }
	eng.Register(wf, map[string]TaskFunc{"a": noRun, "b": noRun, "c": noRun})
	res, err := eng.Run(context.Background(), "r1", wf)
	if err != nil {
		t.Fatal(err)
	}
	// A second engine/process must also be a no-op.
	res2, err := eng.Run(context.Background(), "r1", wf)
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("IDEMPOTENT finished run re-invoked twice -> executions=%d statuses=%s,%s", ran, res.Status, res2.Status)
	if ran != 0 {
		t.Fatalf("tasks executed %d times against a finished run", ran)
	}
	if res.Status != RunSucceeded || res2.Status != RunSucceeded {
		t.Fatal("lost terminal verdict")
	}
}

func TestResumeCascadesRecoveredFailure(t *testing.T) {
	dir := t.TempDir()
	defs := []TaskDef{
		{ID: "a"},
		{ID: "b", Deps: []string{"a"}},
		{ID: "c", Deps: []string{"b"}},
		{ID: "d", Deps: []string{"a"}},
	}
	wf, _ := Build("resume-fail", defs)
	seedJournal(t, dir, "r1", wf,
		Event{Type: EvTaskStarted, TaskID: "a", Attempt: 0, Attempts: 1},
		Event{Type: EvTaskSucceeded, TaskID: "a", Attempts: 1},
		Event{Type: EvTaskStarted, TaskID: "b", Attempt: 0, Attempts: 1},
		Event{Type: EvTaskFailed, TaskID: "b", Attempts: 1, Error: "boom", Reason: "permanent", Retryable: false},
	)
	var ranD int32
	eng, _ := NewEngine(Config{StateDir: dir, MaxWorkers: 2, Logger: testLogger(t)})
	defer eng.Close()
	eng.Register(wf, map[string]TaskFunc{
		"a": func(TaskContext) error { t.Error("a re-run"); return nil },
		"b": func(TaskContext) error { t.Error("b re-run"); return nil },
		"c": func(TaskContext) error { t.Error("c must be skipped"); return nil },
		"d": func(TaskContext) error { atomic.AddInt32(&ranD, 1); return nil },
	})
	res, err := eng.Run(context.Background(), "r1", wf)
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("RESUME-FAIL b=FAILED durable -> c=%s d=%s ran=%d | FINAL %s",
		res.Tasks["c"].Status, res.Tasks["d"].Status, ranD, res.Status)
	if res.Tasks["c"].Status != StatusSkipped {
		t.Fatalf("c=%s want SKIPPED on resume", res.Tasks["c"].Status)
	}
	if res.Tasks["d"].Status != StatusSucceeded || ranD != 1 {
		t.Fatal("independent branch must still complete")
	}
}

func TestCorruptJournalRejectedThenRepaired(t *testing.T) {
	dir := t.TempDir()
	wf, _ := Build("corrupt", chainDefs())
	seedJournal(t, dir, "r1", wf)
	// Simulate a torn append: partial JSON with no terminating newline.
	p := journalPath(dir, "r1")
	f, err := os.OpenFile(p, os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.WriteString(`{"v":1,"crc":"00000000","rec":{"type":"RUN_FINI`); err != nil {
		t.Fatal(err)
	}
	f.Close()

	eng, _ := NewEngine(Config{StateDir: dir, MaxWorkers: 1, Logger: discardLogger{}})
	defer eng.Close()
	noop := func(TaskContext) error { return nil }
	eng.Register(wf, map[string]TaskFunc{"a": noop, "b": noop, "c": noop})
	_, err = eng.Run(context.Background(), "r1", wf)
	if !errors.Is(err, ErrCorruptedJournal) {
		t.Fatalf("torn journal: err=%v want ErrCorruptedJournal", err)
	}
	t.Logf("CORRUPT torn tail -> REJECTED: %v", err)

	removed, err := RepairCorruptedJournal(dir, "r1")
	if err != nil {
		t.Fatal(err)
	}
	if removed == 0 {
		t.Fatal("repair should remove at least the torn record")
	}
	t.Logf("REPAIR removed=%d bad/trailing record(s)", removed)

	res, err := eng.Run(context.Background(), "r1", wf)
	if err != nil {
		t.Fatalf("after repair: %v", err)
	}
	if res.Status != RunSucceeded {
		t.Fatalf("post-repair status=%s", res.Status)
	}
}

func TestChecksumMismatchRejected(t *testing.T) {
	dir := t.TempDir()
	wf, _ := Build("crc", chainDefs())
	seedJournal(t, dir, "r1", wf)
	p := journalPath(dir, "r1")
	data, _ := os.ReadFile(p)
	// Append a well-formed line whose CRC is deliberately wrong.
	f, _ := os.OpenFile(p, os.O_WRONLY|os.O_APPEND, 0o644)
	_, _ = f.WriteString(`{"v":1,"crc":"deadbeef","rec":{"type":"TASK_FAILED","task_id":"a"}}` + "\n")
	f.Close()
	eng, _ := NewEngine(Config{StateDir: dir, MaxWorkers: 1, Logger: discardLogger{}})
	defer eng.Close()
	noop := func(TaskContext) error { return nil }
	eng.Register(wf, map[string]TaskFunc{"a": noop, "b": noop, "c": noop})
	_, err := eng.Run(context.Background(), "r1", wf)
	if !errors.Is(err, ErrCorruptedJournal) {
		t.Fatalf("bad crc: err=%v want ErrCorruptedJournal", err)
	}
	t.Logf("CORRUPT checksum mismatch -> REJECTED (file size=%d): %v", len(data)+1, err)

	// Repair must truncate the bad line and the run must then succeed.
	if _, err := RepairCorruptedJournal(dir, "r1"); err != nil {
		t.Fatal(err)
	}
	res, err := eng.Run(context.Background(), "r1", wf)
	if err != nil {
		t.Fatalf("after crc repair: %v", err)
	}
	if res.Status != RunSucceeded {
		t.Fatalf("status=%s", res.Status)
	}
}

func TestDefinitionMismatchRejected(t *testing.T) {
	dir := t.TempDir()
	wf1, _ := Build("wf", chainDefs()) // a,b,c
	seedJournal(t, dir, "r1", wf1)
	// Same run directory, but a different graph.
	wf2, _ := Build("wf", []TaskDef{
		{ID: "a"},
		{ID: "b", Deps: []string{"a"}},
		{ID: "c", Deps: []string{"b"}},
		{ID: "extra", Deps: []string{"c"}},
	})
	eng, _ := NewEngine(Config{StateDir: dir, MaxWorkers: 1, Logger: discardLogger{}})
	defer eng.Close()
	noop := func(TaskContext) error { return nil }
	fns := map[string]TaskFunc{}
	for _, d := range wf2.Tasks {
		fns[d.ID] = noop
	}
	eng.Register(wf2, fns)
	_, err := eng.Run(context.Background(), "r1", wf2)
	if !errors.Is(err, ErrDefinitionMismatch) {
		t.Fatalf("err=%v want ErrDefinitionMismatch", err)
	}
	t.Logf("MISMATCH changed graph on same run -> REJECTED: %v", err)
}

func TestCrashRestartEndToEndNoDuplicateSideEffects(t *testing.T) {
	// Simulate 3 process "lifetimes" against one run id; side-effect counter
	// for completed tasks must never move after their terminal event.
	dir := t.TempDir()
	wf, _ := Build("multi-restart", []TaskDef{
		{ID: "a"},
		{ID: "b", Deps: []string{"a"}},
		{ID: "c", Deps: []string{"b"}},
		{ID: "d", Deps: []string{"c"}},
	})
	sideEffects := filepath.Join(dir, "effects.txt")
	bump := func(id string) TaskFunc {
		return func(TaskContext) error {
			f, err := os.OpenFile(sideEffects, os.O_WRONLY|os.O_CREATE|os.O_APPEND, 0o644)
			if err != nil {
				return err
			}
			defer f.Close()
			_, err = fmt.Fprintf(f, "%s\n", id)
			return err
		}
	}
	fns := map[string]TaskFunc{"a": bump("a"), "b": bump("b"), "c": bump("c"), "d": bump("d")}

	// Lifetime 1: seed journal as if process died after a,b succeeded and
	// c STARTED. Their side effects already durably happened, so model them:
	if err := os.WriteFile(sideEffects, []byte("a\nb\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	seedJournal(t, dir, "run-x", wf,
		Event{Type: EvTaskStarted, TaskID: "a", Attempt: 0, Attempts: 1},
		Event{Type: EvTaskSucceeded, TaskID: "a", Attempts: 1},
		Event{Type: EvTaskStarted, TaskID: "b", Attempt: 0, Attempts: 1},
		Event{Type: EvTaskSucceeded, TaskID: "b", Attempts: 1},
		Event{Type: EvTaskStarted, TaskID: "c", Attempt: 0, Attempts: 1},
	)

	// Lifetime 2: resumes, completes c and d.
	runOnce := func() *RunResult {
		eng, err := NewEngine(Config{StateDir: dir, MaxWorkers: 1, Logger: testLogger(t)})
		if err != nil {
			t.Fatal(err)
		}
		eng.Register(wf, fns)
		res, err := eng.Run(context.Background(), "run-x", wf)
		eng.Close()
		if err != nil {
			t.Fatal(err)
		}
		return res
	}
	res := runOnce()
	if res.Status != RunSucceeded {
		t.Fatalf("lifetime 2 status=%s", res.Status)
	}
	// Lifetime 3: run already terminal -> nothing executes.
	res = runOnce()

	effects, _ := os.ReadFile(sideEffects)
	t.Logf("E2E RESTART side-effect log:\n%s", effects)
	count := map[string]int{}
	for _, line := range splitLines(string(effects)) {
		count[line]++
	}
	for _, id := range []string{"a", "b", "c", "d"} {
		if count[id] != 1 {
			t.Fatalf("task %s produced %d side effects, want exactly 1", id, count[id])
		}
	}
}

func splitLines(s string) []string {
	var out []string
	cur := ""
	for _, r := range s {
		if r == '\n' {
			if cur != "" {
				out = append(out, cur)
			}
			cur = ""
			continue
		}
		cur += string(r)
	}
	if cur != "" {
		out = append(out, cur)
	}
	return out
}
