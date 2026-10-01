package workflow

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func newTestStore(t *testing.T) *FileStore {
	t.Helper()
	dir := t.TempDir()
	s, err := NewFileStore(dir)
	if err != nil {
		t.Fatalf("NewFileStore: %v", err)
	}
	return s
}

func sampleState(runID string) *RunState {
	def := WorkflowDef{Name: "wf", Concurrency: 1, Tasks: []TaskDef{{ID: "a"}, {ID: "b", Dependencies: []string{"a"}}}}
	return &RunState{
		RunID:      runID,
		Definition: def,
		DefHash:    definitionHash(def),
		Tasks: map[string]*TaskState{
			"a": {ID: "a", Status: StatusSucceeded, Output: "out-a", Attempts: 1},
			"b": {ID: "b", Status: StatusPending},
		},
	}
}

func TestStoreRoundTrip(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()
	st := sampleState("run-1")
	if err := s.CreateRun(ctx, st); err != nil {
		t.Fatalf("CreateRun: %v", err)
	}
	st.Tasks["b"].Status = StatusSucceeded
	st.Tasks["b"].Output = "out-b"
	if err := s.PutTask(ctx, "run-1", st.Tasks["b"]); err != nil {
		t.Fatalf("PutTask: %v", err)
	}
	loaded, err := s.LoadRun(ctx, "run-1")
	if err != nil {
		t.Fatalf("LoadRun: %v", err)
	}
	if loaded.Tasks["a"].Output != "out-a" || loaded.Tasks["b"].Status != StatusSucceeded {
		t.Fatalf("unexpected loaded state: %+v / %+v", loaded.Tasks["a"], loaded.Tasks["b"])
	}
	t.Logf("round-trip OK: a=%+v b=%+v", loaded.Tasks["a"], loaded.Tasks["b"])
}

func TestStoreRejectsCorruptedTaskRecord(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()
	st := sampleState("run-2")
	if err := s.CreateRun(ctx, st); err != nil {
		t.Fatalf("CreateRun: %v", err)
	}
	// Tamper with the task record without updating its checksum.
	p := s.taskPath("run-2", "a")
	raw, err := os.ReadFile(p)
	if err != nil {
		t.Fatal(err)
	}
	tampered := strings.Replace(string(raw), `"output": "out-a"`, `"output": "HACKED"`, 1)
	if err := os.WriteFile(p, []byte(tampered), 0o600); err != nil {
		t.Fatal(err)
	}
	_, err = s.LoadRun(ctx, "run-2")
	if !errors.Is(err, ErrRunCorrupted) {
		t.Fatalf("expected ErrRunCorrupted, got %v", err)
	}
	var ce *CorruptionError
	errors.As(err, &ce)
	if ce.TaskID != "a" {
		t.Fatalf("expected pinpoint task a, got %q", ce.TaskID)
	}
	t.Logf("corruption rejected as expected: %v", err)

	// A fresh store over the same directory must still refuse to silently
	// continue.
	s2, err := NewFileStore(s.Dir())
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s2.LoadRun(ctx, "run-2"); !errors.Is(err, ErrRunCorrupted) {
		t.Fatalf("reopen expected corruption error, got %v", err)
	}
}

func TestStoreRejectsCorruptedManifest(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()
	if err := s.CreateRun(ctx, sampleState("run-3")); err != nil {
		t.Fatal(err)
	}
	p := s.manifestPath("run-3")
	raw, _ := os.ReadFile(p)
	tampered := strings.Replace(string(raw), `"name": "wf"`, `"name": "evil"`, 1)
	if err := os.WriteFile(p, []byte(tampered), 0o600); err != nil {
		t.Fatal(err)
	}
	_, err := s.LoadRun(ctx, "run-3")
	if !errors.Is(err, ErrRunCorrupted) {
		t.Fatalf("expected ErrRunCorrupted, got %v", err)
	}
	t.Logf("manifest corruption rejected: %v", err)
}

func TestStoreRepairsLeftoverTempFiles(t *testing.T) {
	dir := t.TempDir()
	s, err := NewFileStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	if err := s.CreateRun(ctx, sampleState("run-4")); err != nil {
		t.Fatal(err)
	}
	// Simulate a crash during an atomic write: a .tmp file is left behind.
	tmp := filepath.Join(s.Dir(), "runs", "run-4", "tasks", "a.json.tmp")
	if err := os.WriteFile(tmp, []byte("partial garbage"), 0o600); err != nil {
		t.Fatal(err)
	}
	s2, err := NewFileStore(s.Dir())
	if err != nil {
		t.Fatalf("repair open failed: %v", err)
	}
	if _, err := os.Stat(tmp); !os.IsNotExist(err) {
		t.Fatalf("temp file not repaired: %v", err)
	}
	loaded, err := s2.LoadRun(ctx, "run-4")
	if err != nil {
		t.Fatalf("load after repair: %v", err)
	}
	if loaded.Tasks["a"].Status != StatusSucceeded {
		t.Fatalf("unexpected status after repair: %s", loaded.Tasks["a"].Status)
	}
	t.Logf("temp file cleaned and run loads cleanly")
}

func TestStoreMissingRun(t *testing.T) {
	s := newTestStore(t)
	if _, err := s.LoadRun(context.Background(), "nope"); !errors.Is(err, ErrRunNotFound) {
		t.Fatalf("expected ErrRunNotFound, got %v", err)
	}
}
