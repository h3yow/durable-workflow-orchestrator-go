package workflow

import (
	"bufio"
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"hash/crc32"
	"os"
	"path/filepath"
	"sort"
	"time"
)

// journalVersion is the record envelope version supported by this build.
const journalVersion = 1

// EventType enumerates durable journal records.
type EventType string

const (
	EvRunStarted    EventType = "RUN_STARTED"
	EvTaskStarted   EventType = "TASK_STARTED"
	EvTaskProgress  EventType = "TASK_PROGRESS" // attempt finished, retry scheduled (observability)
	EvTaskSucceeded EventType = "TASK_SUCCEEDED"
	EvTaskFailed    EventType = "TASK_FAILED"
	EvTaskSkipped   EventType = "TASK_SKIPPED"
	EvTaskCanceled  EventType = "TASK_CANCELED"
	EvRunFinished   EventType = "RUN_FINISHED"
)

// Event is the payload of one journal record.
type Event struct {
	Type      EventType `json:"type"`
	RunID     string    `json:"run_id,omitempty"`
	TaskID    string    `json:"task_id,omitempty"`
	Attempt   int       `json:"attempt,omitempty"`
	Attempts  int       `json:"attempts,omitempty"`
	Status    RunStatus `json:"status,omitempty"`
	Reason    string    `json:"reason,omitempty"`
	Error     string    `json:"error,omitempty"`
	Retryable bool      `json:"retryable,omitempty"`
	Result    string    `json:"result,omitempty"`
	DefHash   string    `json:"def_hash,omitempty"`
	TS        time.Time `json:"ts"`
}

// envelope is the on-disk, checksum-protepped framing.
type envelope struct {
	V   int             `json:"v"`
	CRC string          `json:"crc"`
	Rec json.RawMessage `json:"rec"`
}

// journalPath returns <dir>/<runID>/journal.log
func journalPath(dir, runID string) string {
	return filepath.Join(dir, runID, "journal.log")
}

// DefinitionHash hashes the workflow definition so recovery can reject a
// run whose graph changed underneath a persisted state.
func DefinitionHash(wf *Workflow) (string, error) {
	// marshal tasks in definition order -> deterministic bytes
	type wire struct {
		Name  string     `json:"name"`
		Tasks []*TaskDef `json:"tasks"`
	}
	b, err := json.Marshal(wire{Name: wf.Name, Tasks: defsInOrder(wf)})
	if err != nil {
		return "", err
	}
	sum := sha256.Sum256(b)
	return hex.EncodeToString(sum[:]), nil
}

func defsInOrder(wf *Workflow) []*TaskDef {
	out := make([]*TaskDef, 0, len(wf.order))
	for _, id := range wf.order {
		out = append(out, wf.Tasks[id])
	}
	return out
}

// Journal appends durable event records to a local file and replays them.
type Journal struct {
	path string
	f    *os.File
	w    *bufio.Writer
}

// openJournal opens (creating if needed) the run journal for appending.
func openJournal(dir, runID string) (*Journal, error) {
	if err := os.MkdirAll(filepath.Join(dir, runID), 0o755); err != nil {
		return nil, err
	}
	p := journalPath(dir, runID)
	f, err := os.OpenFile(p, os.O_RDWR|os.O_CREATE, 0o644)
	if err != nil {
		return nil, err
	}
	if _, err := f.Seek(0, os.SEEK_END); err != nil {
		f.Close()
		return nil, err
	}
	return &Journal{path: p, f: f, w: bufio.NewWriter(f)}, nil
}

// Append writes one checksummed record and fsyncs it so a crash leaves
// either the whole record or none (a torn tail is detected on replay).
func (j *Journal) Append(ev Event) error {
	rec, err := json.Marshal(ev)
	if err != nil {
		return err
	}
	sum := crc32.ChecksumIEEE(rec)
	env, err := json.Marshal(envelope{V: journalVersion, CRC: fmt.Sprintf("%08x", sum), Rec: rec})
	if err != nil {
		return err
	}
	if _, err := j.w.Write(env); err != nil {
		return err
	}
	if err := j.w.WriteByte('\n'); err != nil {
		return err
	}
	if err := j.w.Flush(); err != nil {
		return err
	}
	return j.f.Sync()
}

func (j *Journal) Close() error {
	if j.f == nil {
		return nil
	}
	if err := j.w.Flush(); err != nil {
		j.f.Close()
		return err
	}
	return j.f.Close()
}

// replay reads all records, rejecting corruption or invariant violations
// rather than silently continuing.
func (j *Journal) replay(wf *Workflow) ([]Event, map[string]*TaskState, RunStatus, error) {
	data, err := os.ReadFile(j.path)
	if err != nil {
		return nil, nil, "", err
	}
	var events []Event
	scanner := bufio.NewScanner(bytes.NewReader(data))
	scanner.Buffer(make([]byte, 1024*1024), 64*1024*1024)
	lineNo := 0
	for scanner.Scan() {
		lineNo++
		line := bytes.TrimSpace(scanner.Bytes())
		if len(line) == 0 {
			continue
		}
		var env envelope
		if err := json.Unmarshal(line, &env); err != nil {
			return nil, nil, "", fmt.Errorf("%w: line %d is not a valid record envelope", ErrCorruptedJournal, lineNo)
		}
		if env.V != journalVersion {
			return nil, nil, "", fmt.Errorf("%w: line %d has version %d, want %d", ErrJournalVersion, lineNo, env.V, journalVersion)
		}
		var wantCRC string
		if len(env.Rec) > 0 {
			sum := crc32.ChecksumIEEE(env.Rec)
			wantCRC = fmt.Sprintf("%08x", sum)
		}
		if wantCRC != env.CRC {
			return nil, nil, "", fmt.Errorf("%w: line %d checksum mismatch (torn/partial write)", ErrCorruptedJournal, lineNo)
		}
		var ev Event
		if err := json.Unmarshal(env.Rec, &ev); err != nil {
			return nil, nil, "", fmt.Errorf("%w: line %d payload unreadable: %v", ErrCorruptedJournal, lineNo, err)
		}
		events = append(events, ev)
	}
	if err := scanner.Err(); err != nil {
		// bufio.ErrTooLong etc. -> treat last line as a partial write
		return nil, nil, "", fmt.Errorf("%w: %v", ErrCorruptedJournal, err)
	}
	// A file that does not end in '\n' means the last record may be a torn
	// append. os.ReadFile keeps trailing bytes; detect explicitly.
	if len(data) > 0 && data[len(data)-1] != '\n' {
		return nil, nil, "", fmt.Errorf("%w: file does not end with a record terminator", ErrCorruptedJournal)
	}

	states, runStatus, err := foldEvents(events, wf)
	if err != nil {
		return nil, nil, "", err
	}
	return events, states, runStatus, nil
}

// foldEvents applies events to a fresh state table, enforcing invariants.
func foldEvents(events []Event, wf *Workflow) (map[string]*TaskState, RunStatus, error) {
	states := make(map[string]*TaskState, len(wf.Tasks))
	for _, id := range wf.order {
		states[id] = &TaskState{ID: id, Status: StatusPending}
	}
	runStatus := RunRunning
	runStarted := false
	runFinished := false
	for i, ev := range events {
		if runFinished {
			return nil, "", fmt.Errorf("%w: event %d (%s) appears after RUN_FINISHED", ErrStateInconsistent, i, ev.Type)
		}
		switch ev.Type {
		case EvRunStarted:
			if runStarted {
				return nil, "", fmt.Errorf("%w: duplicate RUN_STARTED", ErrStateInconsistent)
			}
			runStarted = true
			if wf != nil {
				want, err := DefinitionHash(wf)
				if err != nil {
					return nil, "", err
				}
				if ev.DefHash != "" && ev.DefHash != want {
					return nil, "", fmt.Errorf("%w: run %s definition hash %s != current %s",
						ErrDefinitionMismatch, ev.RunID, ev.DefHash, want)
				}
			}
		case EvRunFinished:
			if !runStarted {
				return nil, "", fmt.Errorf("%w: RUN_FINISHED without RUN_STARTED", ErrStateInconsistent)
			}
			runFinished = true
			runStatus = ev.Status
		default:
			if !runStarted {
				return nil, "", fmt.Errorf("%w: %s event before RUN_STARTED", ErrStateInconsistent, ev.Type)
			}
			st, ok := states[ev.TaskID]
			if !ok {
				return nil, "", fmt.Errorf("%w: event %d references unknown task %q", ErrStateInconsistent, i, ev.TaskID)
			}
			if err := applyTaskEvent(st, ev); err != nil {
				return nil, "", err
			}
		}
	}
	return states, runStatus, nil
}

func applyTaskEvent(st *TaskState, ev Event) error {
	now := ev.TS
	setTime := func(p **time.Time) {
		if !now.IsZero() {
			t := now
			*p = &t
		}
	}
	switch ev.Type {
	case EvTaskStarted:
		if st.Status == StatusSucceeded || st.Status == StatusFailed ||
			st.Status == StatusSkipped || st.Status == StatusCanceled {
			return fmt.Errorf("%w: TASK_STARTED for already-terminal task %s", ErrStateInconsistent, st.ID)
		}
		if ev.Attempt < st.Attempts {
			return fmt.Errorf("%w: task %s attempt went backwards (%d < %d)", ErrStateInconsistent, st.ID, ev.Attempt, st.Attempts)
		}
		st.Status = StatusRunning
		st.Attempts = ev.Attempt + 1
		if st.StartedAt == nil {
			setTime(&st.StartedAt)
		}
		setTime(&st.UpdatedAt)
	case EvTaskProgress:
		if st.Status != StatusRunning {
			return fmt.Errorf("%w: TASK_PROGRESS for non-running task %s", ErrStateInconsistent, st.ID)
		}
		st.Error = ev.Error
		st.Retryable = ev.Retryable
		setTime(&st.UpdatedAt)
	case EvTaskSucceeded:
		if st.Status != StatusRunning {
			return fmt.Errorf("%w: TASK_SUCCEEDED without in-flight attempt for task %s", ErrStateInconsistent, st.ID)
		}
		st.Status = StatusSucceeded
		st.Result = ev.Result
		st.Error = ""
		setTime(&st.UpdatedAt)
		setTime(&st.FinishedAt)
	case EvTaskFailed:
		if st.Status != StatusRunning {
			return fmt.Errorf("%w: TASK_FAILED without in-flight attempt for task %s", ErrStateInconsistent, st.ID)
		}
		st.Status = StatusFailed
		st.Error = ev.Error
		st.Retryable = ev.Retryable
		setTime(&st.UpdatedAt)
		setTime(&st.FinishedAt)
	case EvTaskSkipped:
		if st.Status == StatusSucceeded || st.Status == StatusFailed {
			return fmt.Errorf("%w: TASK_SKIPPED for already-terminal task %s", ErrStateInconsistent, st.ID)
		}
		st.Status = StatusSkipped
		if ev.Error != "" {
			st.Error = ev.Error
		} else {
			st.Error = ev.Reason
		}
		setTime(&st.UpdatedAt)
		setTime(&st.FinishedAt)
	case EvTaskCanceled:
		if st.Status == StatusSucceeded || st.Status == StatusFailed {
			return fmt.Errorf("%w: TASK_CANCELED for already-terminal task %s", ErrStateInconsistent, st.ID)
		}
		st.Status = StatusCanceled
		if ev.Error != "" {
			st.Error = ev.Error
		} else {
			st.Error = ev.Reason
		}
		setTime(&st.UpdatedAt)
		setTime(&st.FinishedAt)
	default:
		return fmt.Errorf("%w: unknown event type %q", ErrCorruptedJournal, ev.Type)
	}
	return nil
}

// RepairCorruptedJournal truncates the journal at the first bad/torn record,
// turning a partially written tail into a clean prefix. Events after the
// corruption point are discarded as they cannot be trusted. It returns the
// number of records removed.
func RepairCorruptedJournal(dir, runID string) (int, error) {
	p := journalPath(dir, runID)
	data, err := os.ReadFile(p)
	if err != nil {
		return 0, err
	}
	goodPrefix := int64(0)
	removed := 0
	lines := bytes.SplitAfter(data, []byte("\n"))
	// last element may be "" (proper ending) or a torn tail
	for idx, raw := range lines {
		isLast := idx == len(lines)-1
		if len(raw) == 0 {
			continue
		}
		line := bytes.TrimSpace(raw)
		ok := false
		if len(line) > 0 && bytes.HasSuffix(raw, []byte("\n")) {
			var env envelope
			if json.Unmarshal(line, &env) == nil && env.V == journalVersion &&
				len(env.Rec) > 0 && fmt.Sprintf("%08x", crc32.ChecksumIEEE(env.Rec)) == env.CRC {
				ok = true
			}
		}
		if ok {
			goodPrefix += int64(len(raw))
			continue
		}
		// first bad record: everything from here (including a torn tail) goes
		if !isLast || len(raw) > 0 {
			removed++
		}
		// count remaining records (best effort)
		for _, rest := range lines[idx+1:] {
			if len(bytes.TrimSpace(rest)) > 0 {
				removed++
			}
		}
		break
	}
	if err := os.Truncate(p, goodPrefix); err != nil {
		return 0, err
	}
	return removed, nil
}

// listRunIDs returns run directories present under the state directory.
func listRunIDs(dir string) ([]string, error) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, err
	}
	var ids []string
	for _, e := range entries {
		if e.IsDir() {
			if _, err := os.Stat(filepath.Join(dir, e.Name(), "journal.log")); err == nil {
				ids = append(ids, e.Name())
			}
		}
	}
	sort.Strings(ids)
	return ids, nil
}
