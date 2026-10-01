package workflow

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"log"
)

// Options configures an Engine.
type Options struct {
	Store Store
	Clock Clock
	// Logf receives the decision log: definition inputs, scheduling
	// decisions (start / retry / skip / cancel), and terminal states.
	Logf func(format string, args ...any)
}

// Engine schedules and executes workflow runs.
type Engine struct {
	store Store
	clock Clock
	logf  func(string, ...any)
}

// NewEngine constructs an Engine.
func NewEngine(opts Options) *Engine {
	cl := opts.Clock
	if cl == nil {
		cl = NewRealClock()
	}
	lf := opts.Logf
	if lf == nil {
		lf = log.Printf
	}
	return &Engine{store: opts.Store, clock: cl, logf: lf}
}

func newRunID() string {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		// crypto/rand must work on any supported platform.
		panic(fmt.Sprintf("unable to generate run id: %v", err))
	}
	return hex.EncodeToString(b[:])
}

func definitionHash(def WorkflowDef) string {
	b, _ := json.Marshal(def)
	sum := sha256.Sum256(b)
	return hex.EncodeToString(sum[:])
}

func checkExecutors(def WorkflowDef, reg Registry) error {
	for _, t := range def.Tasks {
		if _, ok := reg[t.ID]; !ok {
			return &ValidationError{TaskID: t.ID, Kind: ErrMissingFunc,
				Reason: "no executor registered for task: " + t.ID}
		}
	}
	return nil
}

func normalizeDef(def *WorkflowDef) {
	if def.Concurrency <= 0 {
		def.Concurrency = 1
	}
	if def.Concurrency > len(def.Tasks) {
		def.Concurrency = len(def.Tasks)
	}
	for i := range def.Tasks {
		t := &def.Tasks[i]
		switch t.Retry.Backoff {
		case "", BackoffFixed, BackoffLinear, BackoffExponent:
		default:
			t.Retry.Backoff = BackoffFixed
		}
		if t.Retry.MaxRetries < 0 {
			t.Retry.MaxRetries = 0
		}
	}
}

// Start validates the definition, persists a fresh run and executes it.
func (e *Engine) Start(ctx context.Context, def WorkflowDef, reg Registry) (*RunResult, error) {
	if err := ValidateDefinition(def); err != nil {
		return nil, err
	}
	if err := checkExecutors(def, reg); err != nil {
		return nil, err
	}
	normalizeDef(&def)
	state := &RunState{
		RunID:      newRunID(),
		Definition: def,
		DefHash:    definitionHash(def),
		Status:     StatusPending,
		Tasks:      make(map[string]*TaskState, len(def.Tasks)),
	}
	for _, t := range def.Tasks {
		state.Tasks[t.ID] = &TaskState{ID: t.ID, Status: StatusPending}
	}
	if err := e.store.CreateRun(ctx, state); err != nil {
		return nil, fmt.Errorf("persist run: %w", err)
	}
	e.logf("[run=%s] START input definition=%s tasks=%d concurrency=%d runTimeout=%s",
		state.RunID, def.Name, len(def.Tasks), def.Concurrency, def.RunTimeout.Std())
	return e.coordinate(ctx, state, reg)
}

// Resume continues a persisted run after a restart. Succeeded tasks are never
// re-executed; tasks recorded as Running (an interrupted attempt) are reset to
// Pending and re-attempted. A corrupted store record is rejected.
func (e *Engine) Resume(ctx context.Context, runID string, def WorkflowDef, reg Registry) (*RunResult, error) {
	state, err := e.store.LoadRun(ctx, runID)
	if err != nil {
		return nil, err
	}
	if err := ValidateDefinition(def); err != nil {
		return nil, err
	}
	if got := definitionHash(def); got != state.DefHash {
		return nil, &ValidationError{Kind: ErrDefinitionMismatch,
			Reason: fmt.Sprintf("definition hash mismatch: supplied=%s persisted=%s", got, state.DefHash)}
	}
	if err := checkExecutors(def, reg); err != nil {
		return nil, err
	}
	normalizeDef(&def)
	// Recovery rules:
	//   - the manifest only reaches a terminal status on clean shutdown, so
	//     a non-terminal manifest means the process died mid-run;
	//   - Succeeded / Failed / Skipped records are final outcomes and are
	//     never re-executed or silently altered;
	//   - Running / Ready / Pending attempts were interrupted and are reset
	//     to Pending to run again (at-least-once; executors key on AttemptKey);
	//   - a Canceled record under a non-terminal manifest can only be a torn
	//     cancel cascade, so it is rewound to Pending as well.
	manifestTerminal := state.Status == StatusSucceeded || state.Status == StatusFailed ||
		state.Status == StatusCanceled || state.Status == StatusTimedOut
	recovered := false
	for _, id := range TopoOrder(state.Definition) {
		t := state.Tasks[id]
		rewind := t.Status == StatusRunning || t.Status == StatusReady || t.Status == StatusPending
		if t.Status == StatusCanceled && !manifestTerminal {
			rewind = true
		}
		if !rewind {
			continue
		}
		e.logf("[run=%s] RECOVER task=%s status=%s -> Pending (attempts=%d preserved)",
			runID, t.ID, t.Status, t.Attempts)
		t.Status = StatusPending
		t.FailReason = ""
		if err := e.store.PutTask(ctx, runID, t); err != nil {
			return nil, fmt.Errorf("persist recovery of task %s: %w", t.ID, err)
		}
		recovered = true
	}
	if !recovered {
		e.logf("[run=%s] RESUME no interrupted tasks found", runID)
	}
	switch state.Status {
	case StatusSucceeded, StatusFailed, StatusCanceled, StatusTimedOut:
		e.logf("[run=%s] RESUME run already terminal status=%s reason=%q", runID, state.Status, state.Reason)
		return buildResult(state), nil
	}
	return e.coordinate(ctx, state, reg)
}

// buildResult assembles a RunResult in deterministic task order.
func buildResult(state *RunState) *RunResult {
	res := &RunResult{RunID: state.RunID, Status: state.Status, Reason: state.Reason}
	for _, td := range state.Definition.Tasks {
		t := state.Tasks[td.ID]
		res.Tasks = append(res.Tasks, TaskResult{
			ID: t.ID, Status: t.Status, Attempts: t.Attempts,
			Output: t.Output, Reason: t.FailReason,
		})
	}
	return res
}
