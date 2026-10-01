package workflow

import (
	"bytes"
	"context"
	"errors"
	"os"
	"sync/atomic"
	"testing"
)

// Simulate a process crash while "mid" is running: construct exactly the
// on-disk state a hard kill would leave behind ("first"=Succeeded,
// "mid"=Running, "last"=Pending, manifest non-terminal), then Resume on a
// fresh Engine. Completed tasks must not run again; the interrupted task
// must be re-attempted; downstream must still execute.
func TestResumeAfterSimulatedCrash(t *testing.T) {
	store, err := NewFileStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	lb := &logBuffer{}
	def := WorkflowDef{Name: "resume", Concurrency: 2, Tasks: []TaskDef{
		task("first"),
		task("mid", "first"),
		task("last", "mid"),
	}}

	// Write the half-finished run as if the process vanished mid-attempt.
	crashed := &RunState{
		RunID: "crashed-run", Definition: def, DefHash: definitionHash(def),
		Status: StatusPending,
		Tasks: map[string]*TaskState{
			"first": {ID: "first", Status: StatusSucceeded, Attempts: 1, Output: "first-out"},
			"mid":   {ID: "mid", Status: StatusRunning, Attempts: 1},
			"last":  {ID: "last", Status: StatusPending},
		},
	}
	if err := store.CreateRun(context.Background(), crashed); err != nil {
		t.Fatal(err)
	}
	// CreateRun marks tasks Pending/Running as-is; force the precise state.
	for _, ts := range crashed.Tasks {
		if err := store.PutTask(context.Background(), crashed.RunID, ts); err != nil {
			t.Fatal(err)
		}
	}

	st, err := store.LoadRun(context.Background(), crashed.RunID)
	if err != nil {
		t.Fatal(err)
	}
	if st.Tasks["first"].Status != StatusSucceeded ||
		st.Tasks["mid"].Status != StatusRunning ||
		st.Tasks["last"].Status != StatusPending {
		t.Fatalf("unexpected simulated crash state: %s/%s/%s",
			st.Tasks["first"].Status, st.Tasks["mid"].Status, st.Tasks["last"].Status)
	}
	t.Logf("simulated crash state: first=%s mid=%s last=%s manifest=%s",
		st.Tasks["first"].Status, st.Tasks["mid"].Status, st.Tasks["last"].Status, st.Status)

	var firstCalls, midCalls, lastCalls int32
	reg := Registry{
		"first": func(context.Context, TaskInput) (string, error) {
			atomic.AddInt32(&firstCalls, 1)
			return "first-out-again", nil
		},
		"mid": func(_ context.Context, in TaskInput) (string, error) {
			atomic.AddInt32(&midCalls, 1)
			if in.Attempt != 2 {
				return "", errors.New("resume must re-run mid as attempt 2, preserving attempts count")
			}
			return "mid-out", nil
		},
		"last": func(_ context.Context, in TaskInput) (string, error) {
			atomic.AddInt32(&lastCalls, 1)
			if in.Outputs["mid"] != "mid-out" {
				return "", errors.New("last missing mid output")
			}
			return "last-out", nil
		},
	}

	eng := NewEngine(Options{Store: store, Logf: lb.logf})
	res, err := eng.Resume(context.Background(), crashed.RunID, def, reg)
	if err != nil {
		t.Fatalf("resume: %v", err)
	}
	if res.Status != StatusSucceeded {
		t.Fatalf("resumed run status=%s reason=%s", res.Status, res.Reason)
	}
	if firstCalls != 0 {
		t.Fatalf("completed task first executed %d times after resume, want 0", firstCalls)
	}
	if midCalls != 1 {
		t.Fatalf("interrupted task mid re-executed %d times on resume, want 1", midCalls)
	}
	if lastCalls != 1 {
		t.Fatalf("downstream task last executed %d times, want 1", lastCalls)
	}
	assertStatus(t, res, "first", StatusSucceeded)
	assertStatus(t, res, "mid", StatusSucceeded)
	assertStatus(t, res, "last", StatusSucceeded)
	if tr := findTask(res, "first"); tr.Output != "first-out" {
		t.Fatalf("first output must be preserved, got %q", tr.Output)
	}
	t.Logf("recovery decision log:\n%s", lb.String())
}

func findTask(res *RunResult, id string) TaskResult {
	for _, tr := range res.Tasks {
		if tr.ID == id {
			return tr
		}
	}
	return TaskResult{}
}

// Resuming an already terminal run returns the stored result without
// executing anything.
func TestResumeTerminalRunIsIdempotent(t *testing.T) {
	store, _ := NewFileStore(t.TempDir())
	lb := &logBuffer{}
	def := WorkflowDef{Name: "t", Concurrency: 1, Tasks: []TaskDef{task("a")}}
	var calls int32
	reg := Registry{"a": func(context.Context, TaskInput) (string, error) {
		atomic.AddInt32(&calls, 1)
		return "x", nil
	}}
	eng := NewEngine(Options{Store: store, Logf: lb.logf})
	res, err := eng.Start(context.Background(), def, reg)
	if err != nil {
		t.Fatal(err)
	}
	again, err := eng.Resume(context.Background(), res.RunID, def, reg)
	if err != nil {
		t.Fatal(err)
	}
	if again.Status != StatusSucceeded || calls != 1 {
		t.Fatalf("terminal resume re-executed tasks: calls=%d status=%s", calls, again.Status)
	}
	t.Logf("terminal resume returned stored result; calls=%d", calls)
}

// Resuming with a changed definition is rejected (no partial execution).
func TestResumeRejectsDefinitionMismatch(t *testing.T) {
	store, _ := NewFileStore(t.TempDir())
	def := WorkflowDef{Name: "t", Concurrency: 1, Tasks: []TaskDef{task("a")}}
	reg := Registry{"a": func(context.Context, TaskInput) (string, error) { return "x", nil }}
	eng := NewEngine(Options{Store: store, Logf: (&logBuffer{}).logf})
	res, err := eng.Start(context.Background(), def, reg)
	if err != nil {
		t.Fatal(err)
	}
	changed := def
	changed.Version = "2"
	_, merr := eng.Resume(context.Background(), res.RunID, changed, reg)
	if !errors.Is(merr, ErrDefinitionMismatch) {
		t.Fatalf("expected ErrDefinitionMismatch, got %v", merr)
	}
	t.Logf("definition mismatch rejected: %v", merr)
}

// A torn record (checksum mismatch) makes Resume fail loudly instead of
// re-running anything.
func TestResumeRejectsCorruption(t *testing.T) {
	store, _ := NewFileStore(t.TempDir())
	def := WorkflowDef{Name: "t", Concurrency: 1, Tasks: []TaskDef{task("a")}}
	reg := Registry{"a": func(context.Context, TaskInput) (string, error) { return "x", nil }}
	eng := NewEngine(Options{Store: store, Logf: (&logBuffer{}).logf})
	res, _ := eng.Start(context.Background(), def, reg)
	// Tamper with the raw file directly, bypassing checksum recomputation.
	tp := store.taskPath(res.RunID, "a")
	raw, rerr := os.ReadFile(tp)
	if rerr != nil {
		t.Fatal(rerr)
	}
	dirty := bytes.Replace(raw, []byte(`"output": "x"`), []byte(`"output": "tampered"`), 1)
	if bytes.Equal(dirty, raw) {
		t.Fatal("test fixture: expected string not found")
	}
	if werr := os.WriteFile(tp, dirty, 0o600); werr != nil {
		t.Fatal(werr)
	}
	_, cerr := eng.Resume(context.Background(), res.RunID, def, reg)
	if !errors.Is(cerr, ErrRunCorrupted) {
		t.Fatalf("expected ErrRunCorrupted, got %v", cerr)
	}
	t.Logf("corrupt resume rejected: %v", cerr)
}
