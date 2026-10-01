package workflow

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
)

// Store persists workflow runs on the local filesystem.
type Store interface {
	CreateRun(ctx context.Context, state *RunState) error
	LoadRun(ctx context.Context, runID string) (*RunState, error)
	PutTask(ctx context.Context, runID string, task *TaskState) error
	UpdateRun(ctx context.Context, state *RunState) error
	ListRuns(ctx context.Context) ([]string, error)
}

// FileStore is a local-directory JSON Store with per-record checksums and
// atomic (temp-file + rename) writes.
//
// Layout:
//
//	<dir>/runs/<runID>/manifest.json
//	<dir>/runs/<runID>/tasks/<taskID>.json
type FileStore struct {
	dir string
}

// NewFileStore opens a local directory-backed store and performs startup
// repair: leftover .tmp files from a crashed write are removed. A torn
// record that survived (checksum mismatch) is NOT silently repaired; it is
// rejected on load with a CorruptionError.
func NewFileStore(dir string) (*FileStore, error) {
	if dir == "" {
		return nil, errors.New("store directory must not be empty")
	}
	if err := os.MkdirAll(filepath.Join(dir, "runs"), 0o755); err != nil {
		return nil, fmt.Errorf("create store dir: %w", err)
	}
	if err := repairTempFiles(filepath.Join(dir, "runs")); err != nil {
		return nil, err
	}
	return &FileStore{dir: dir}, nil
}

// Dir returns the root directory of the store.
func (s *FileStore) Dir() string { return s.dir }

func repairTempFiles(root string) error {
	return filepath.Walk(root, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if !info.IsDir() && strings.HasSuffix(path, ".tmp") {
			return os.Remove(path)
		}
		return nil
	})
}

func (s *FileStore) runDir(runID string) string {
	return filepath.Join(s.dir, "runs", runID)
}

func (s *FileStore) taskPath(runID, taskID string) string {
	return filepath.Join(s.runDir(runID), "tasks", taskID+".json")
}

func (s *FileStore) manifestPath(runID string) string {
	return filepath.Join(s.runDir(runID), "manifest.json")
}

func checksumBytes(b []byte) string {
	sum := sha256.Sum256(b)
	return hex.EncodeToString(sum[:])
}

func taskChecksum(t *TaskState) string {
	c := *t
	c.Checksum = ""
	b, _ := json.Marshal(&c)
	return checksumBytes(b)
}

func runChecksum(r *RunState) string {
	c := *r
	c.Checksum = ""
	b, _ := json.Marshal(&c)
	return checksumBytes(b)
}

// atomicWriteJSON writes v to path via a temp file followed by an atomic
// rename, so a crash can never leave a partially overwritten record.
func atomicWriteJSON(path string, v any) error {
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return err
	}
	b, err := json.MarshalIndent(v, "", "  ")
	if err != nil {
		return err
	}
	tmp := path + ".tmp"
	f, err := os.OpenFile(tmp, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o600)
	if err != nil {
		return err
	}
	if _, err := f.Write(b); err != nil {
		f.Close()
		os.Remove(tmp)
		return err
	}
	if err := f.Sync(); err != nil {
		f.Close()
		os.Remove(tmp)
		return err
	}
	if err := f.Close(); err != nil {
		os.Remove(tmp)
		return err
	}
	return os.Rename(tmp, path)
}

// CreateRun persists a brand-new run: task records first, manifest last.
// If the run already exists it is an error.
func (s *FileStore) CreateRun(ctx context.Context, state *RunState) error {
	if state == nil || state.RunID == "" {
		return errors.New("invalid run state")
	}
	if _, err := os.Stat(s.manifestPath(state.RunID)); err == nil {
		return fmt.Errorf("run %s already exists", state.RunID)
	}
	if err := os.MkdirAll(filepath.Join(s.runDir(state.RunID), "tasks"), 0o755); err != nil {
		return err
	}
	ids := make([]string, 0, len(state.Tasks))
	for id := range state.Tasks {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	for _, id := range ids {
		if err := s.PutTask(ctx, state.RunID, state.Tasks[id]); err != nil {
			return err
		}
	}
	state.Status = StatusPending
	state.Checksum = runChecksum(state)
	if err := atomicWriteJSON(s.manifestPath(state.RunID), state); err != nil {
		return err
	}
	return nil
}

// PutTask stores a single task record, refreshing its checksum.
func (s *FileStore) PutTask(ctx context.Context, runID string, task *TaskState) error {
	if task == nil || task.ID == "" {
		return errors.New("invalid task state")
	}
	task.Checksum = taskChecksum(task)
	return atomicWriteJSON(s.taskPath(runID, task.ID), task)
}

// UpdateRun rewrites the manifest, refreshing its checksum.
func (s *FileStore) UpdateRun(ctx context.Context, state *RunState) error {
	if state == nil || state.RunID == "" {
		return errors.New("invalid run state")
	}
	state.Checksum = runChecksum(state)
	return atomicWriteJSON(s.manifestPath(state.RunID), state)
}

// LoadRun reads the manifest and all task records, verifying every checksum.
// A checksum mismatch or unreadable record yields a CorruptionError wrapping
// ErrRunCorrupted; a missing run yields ErrRunNotFound.
func (s *FileStore) LoadRun(ctx context.Context, runID string) (*RunState, error) {
	manifest := s.manifestPath(runID)
	b, err := os.ReadFile(manifest)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, &CorruptionError{RunID: runID, Kind: ErrRunNotFound,
				Reason: "manifest does not exist"}
		}
		return nil, err
	}
	var state RunState
	if err := json.Unmarshal(b, &state); err != nil {
		return nil, &CorruptionError{RunID: runID, Kind: ErrRunCorrupted,
			Reason: "manifest is not valid JSON: " + err.Error()}
	}
	want := state.Checksum
	if got := runChecksum(&state); got != want {
		return nil, &CorruptionError{RunID: runID, Kind: ErrRunCorrupted,
			Reason: fmt.Sprintf("manifest checksum mismatch: stored=%s computed=%s", want, got)}
	}
	if state.Tasks == nil {
		state.Tasks = map[string]*TaskState{}
	}
	// Load and verify every task referenced by the definition.
	taskDir := filepath.Join(s.runDir(runID), "tasks")
	entries, err := os.ReadDir(taskDir)
	if err != nil {
		return nil, &CorruptionError{RunID: runID, Kind: ErrRunCorrupted,
			Reason: "task directory unreadable: " + err.Error()}
	}
	taskFiles := map[string]bool{}
	for _, en := range entries {
		if en.IsDir() || !strings.HasSuffix(en.Name(), ".json") {
			continue
		}
		id := strings.TrimSuffix(en.Name(), ".json")
		taskFiles[id] = true
	}
	for _, td := range state.Definition.Tasks {
		if !taskFiles[td.ID] {
			return nil, &CorruptionError{RunID: runID, TaskID: td.ID, Kind: ErrRunCorrupted,
				Reason: "task record missing: " + td.ID}
		}
		tb, err := os.ReadFile(s.taskPath(runID, td.ID))
		if err != nil {
			return nil, &CorruptionError{RunID: runID, TaskID: td.ID, Kind: ErrRunCorrupted,
				Reason: "task record unreadable: " + err.Error()}
		}
		var t TaskState
		if err := json.Unmarshal(tb, &t); err != nil {
			return nil, &CorruptionError{RunID: runID, TaskID: td.ID, Kind: ErrRunCorrupted,
				Reason: "task record is not valid JSON: " + err.Error()}
		}
		if got := taskChecksum(&t); got != t.Checksum {
			return nil, &CorruptionError{RunID: runID, TaskID: td.ID, Kind: ErrRunCorrupted,
				Reason: fmt.Sprintf("task %s checksum mismatch: stored=%s computed=%s", td.ID, t.Checksum, got)}
		}
		if t.ID != td.ID {
			return nil, &CorruptionError{RunID: runID, TaskID: td.ID, Kind: ErrRunCorrupted,
				Reason: "task record id mismatch: " + t.ID}
		}
		state.Tasks[t.ID] = &t
	}
	if len(state.Tasks) != len(state.Definition.Tasks) {
		return nil, &CorruptionError{RunID: runID, Kind: ErrRunCorrupted,
			Reason: "task count differs from definition"}
	}
	return &state, nil
}

// ListRuns returns run IDs currently present in the store.
func (s *FileStore) ListRuns(ctx context.Context) ([]string, error) {
	entries, err := os.ReadDir(filepath.Join(s.dir, "runs"))
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, err
	}
	var ids []string
	for _, en := range entries {
		if en.IsDir() {
			ids = append(ids, en.Name())
		}
	}
	sort.Strings(ids)
	return ids, nil
}
