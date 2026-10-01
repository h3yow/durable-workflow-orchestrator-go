package workflow

import "errors"

// Sentinel errors for validation and runtime failures.
var (
	ErrDuplicateTask       = errors.New("duplicate task id")
	ErrUnknownDependency   = errors.New("dependency references unknown task")
	ErrCyclicDependency    = errors.New("cyclic dependency detected")
	ErrMissingFunc         = errors.New("no executor registered for task")
	ErrRunNotFound         = errors.New("workflow run not found")
	ErrRunCorrupted        = errors.New("workflow run state corrupted")
	ErrDefinitionMismatch  = errors.New("workflow definition does not match persisted run")
	ErrRunFinished         = errors.New("workflow run already finished")
	ErrRunCanceled         = errors.New("workflow run canceled")
	ErrRunTimeout          = errors.New("workflow run timed out")
	ErrTaskTimeout         = errors.New("task attempt timed out")
	ErrTaskPermanentFailed = errors.New("task permanently failed")
	ErrTaskSkipped         = errors.New("task skipped")
)

// ValidationError describes why a WorkflowDefinition was rejected.
type ValidationError struct {
	Kind   error
	TaskID string
	Reason string
}

func (e *ValidationError) Error() string {
	return "workflow validation failed: " + e.Reason
}

func (e *ValidationError) Unwrap() error { return e.Kind }

// CorruptionError describes a rejected/corrupted persistence record.
type CorruptionError struct {
	RunID  string
	TaskID string
	Kind   error
	Reason string
}

func (e *CorruptionError) Error() string {
	return "persisted state corrupted for run " + e.RunID + ": " + e.Reason
}

func (e *CorruptionError) Unwrap() error { return e.Kind }
