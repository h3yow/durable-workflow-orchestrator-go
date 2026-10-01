// Package workflow implements a local, dependency-driven workflow
// orchestration engine with concurrency limits, retry semantics and
// crash-recovery based solely on local file storage.
package workflow

import (
	"context"
	"time"
)

// TaskStatus is the terminal/lifecycle state of a task attempt within a run.
type TaskStatus string

const (
	StatusPending   TaskStatus = "PENDING"   // not started yet (fresh run)
	StatusRunning   TaskStatus = "RUNNING"   // an attempt is in progress
	StatusSucceeded TaskStatus = "SUCCEEDED" // finished successfully (terminal)
	StatusFailed    TaskStatus = "FAILED"    // exhausted retries, non-retryable (terminal)
	StatusSkipped   TaskStatus = "SKIPPED"   // upstream failed/canceled (terminal)
	StatusCanceled  TaskStatus = "CANCELED"  // run canceled/timed out before it ran (terminal)
)

// RunStatus is the final state of a whole workflow run.
type RunStatus string

const (
	RunRunning   RunStatus = "RUNNING"
	RunSucceeded RunStatus = "SUCCEEDED" // every task succeeded
	RunFailed    RunStatus = "FAILED"    // one or more tasks failed
	RunCanceled  RunStatus = "CANCELED"  // root context canceled or run timeout
)

// BackoffKind selects a retry backoff strategy.
type BackoffKind string

const (
	BackoffFixed    BackoffKind = "fixed"
	BackoffLinear   BackoffKind = "linear"
	BackoffExponent BackoffKind = "exponential"
)

// BackoffPolicy configures delay between attempts.
// Attempts are 0-indexed: attempt 0 is the first try, so the first retry
// wait is computed for attempt index 1.
type BackoffPolicy struct {
	Kind       BackoffKind `json:"kind"`
	Interval   Duration    `json:"interval"`             // base delay
	MaxDelay   Duration    `json:"max_delay,omitempty"`  // upper cap per wait
	Multiplier float64     `json:"multiplier,omitempty"` // for exponential (default 2)
	Jitter     bool        `json:"jitter,omitempty"`     // randomize within [d/2,d) for determinism-free retries
}

// Duration wraps time.Duration so it marshals as e.g. "2s".
type Duration time.Duration

// RetryPolicy configures per-task failure handling.
type RetryPolicy struct {
	MaxAttempts int           `json:"max_attempts"` // total attempts including the first; <=1 means no retry
	Backoff     BackoffPolicy `json:"backoff"`
}

// TaskDef declares one workflow node.
type TaskDef struct {
	ID      string      `json:"id"`
	Deps    []string    `json:"deps,omitempty"`
	Timeout Duration    `json:"timeout,omitempty"` // per-attempt timeout; 0 = no timeout
	Retry   RetryPolicy `json:"retry,omitempty"`
	Payload string      `json:"payload,omitempty"` // opaque definition data, hashed for definition match
}

// TaskContext is passed to task functions.
type TaskContext interface {
	context.Context
	TaskID() string
	Attempt() int
}

// TaskFunc is user code. Returning a *TaskError with Retryable=false makes
// the failure permanent immediately; any other non-nil error is retryable
// until MaxAttempts is exhausted.
type TaskFunc func(tc TaskContext) error

// Workflow is a validated set of task definitions.
type Workflow struct {
	Name  string
	Tasks map[string]*TaskDef
	order []string // deterministic (definition) order
}

// TaskState is the persisted state record of one task.
type TaskState struct {
	ID         string     `json:"id"`
	Status     TaskStatus `json:"status"`
	Attempts   int        `json:"attempts"` // attempts already started
	StartedAt  *time.Time `json:"started_at,omitempty"`
	UpdatedAt  *time.Time `json:"updated_at,omitempty"`
	FinishedAt *time.Time `json:"finished_at,omitempty"`
	Error      string     `json:"error,omitempty"`
	Retryable  bool       `json:"retryable,omitempty"`
	Result     string     `json:"result,omitempty"`
}

// RunResult is what callers receive after Engine.Resume/Run.
type RunResult struct {
	RunID     string
	Status    RunStatus
	Tasks     map[string]*TaskState
	StartedAt time.Time
	EndedAt   time.Time
}

// Logger prints scheduling decisions (tests assert on / inspect this output).
type Logger interface {
	Printf(format string, args ...any)
}
