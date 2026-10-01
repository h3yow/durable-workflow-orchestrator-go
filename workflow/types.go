package workflow

import "context"

// Status is the terminal/intermediate state of a task instance.
type Status string

const (
	StatusPending   Status = "Pending"
	StatusReady     Status = "Ready"
	StatusRunning   Status = "Running"
	StatusSucceeded Status = "Succeeded"
	StatusFailed    Status = "Failed"
	StatusCanceled  Status = "Canceled"
	StatusSkipped   Status = "Skipped"
	StatusTimedOut  Status = "TimedOut" // run-level only
)

// BackoffKind selects the retry backoff strategy.
type BackoffKind string

const (
	BackoffFixed    BackoffKind = "fixed"
	BackoffLinear   BackoffKind = "linear"
	BackoffExponent BackoffKind = "exponential"
)

// RetryPolicy configures timeout/retry behavior for a task.
type RetryPolicy struct {
	Timeout        Duration    `json:"timeout"`
	MaxRetries     int         `json:"max_retries"`
	Backoff        BackoffKind `json:"backoff"`
	InitialBackoff Duration    `json:"initial_backoff"`
	MaxBackoff     Duration    `json:"max_backoff"`
}

// TaskDef declares one task and its dependency edges.
type TaskDef struct {
	ID           string      `json:"id"`
	Dependencies []string    `json:"dependencies"`
	Retry        RetryPolicy `json:"retry"`
}

// WorkflowDef is the complete workflow definition.
type WorkflowDef struct {
	Name        string    `json:"name"`
	Version     string    `json:"version"`
	Concurrency int       `json:"concurrency"`
	RunTimeout  Duration  `json:"run_timeout"`
	Tasks       []TaskDef `json:"tasks"`
}

// TaskInput is passed to an executor.
type TaskInput struct {
	RunID      string
	Attempt    int
	AttemptKey string
	Outputs    map[string]string
}

// Executor runs a single task. Implementations should be idempotent keyed by
// AttemptKey, because attempts may be re-executed after a crash.
type Executor func(ctx context.Context, in TaskInput) (string, error)

// Registry maps task IDs to executors.
type Registry map[string]Executor

// TaskState is the persisted state record of one task.
type TaskState struct {
	ID         string `json:"id"`
	Status     Status `json:"status"`
	Attempts   int    `json:"attempts"`
	Output     string `json:"output,omitempty"`
	FailReason string `json:"fail_reason,omitempty"`
	Checksum   string `json:"checksum"`
}

// RunState is the persisted manifest of a workflow run.
type RunState struct {
	RunID      string                `json:"run_id"`
	Definition WorkflowDef           `json:"definition"`
	DefHash    string                `json:"definition_hash"`
	Status     Status                `json:"status"`
	Tasks      map[string]*TaskState `json:"tasks"`
	Reason     string                `json:"reason,omitempty"`
	Checksum   string                `json:"checksum"`
}

// TaskResult is an entry in a completed RunResult.
type TaskResult struct {
	ID       string
	Status   Status
	Attempts int
	Output   string
	Reason   string
}

// RunResult is returned when a run reaches a terminal state.
type RunResult struct {
	RunID  string
	Status Status
	Reason string
	Tasks  []TaskResult
}
