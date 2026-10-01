package workflow

import "errors"

// Distinct sentinel errors so callers (and tests) can tell rejection causes apart.
var (
	// Definition validation errors.
	ErrDuplicateTask      = errors.New("duplicate task id")
	ErrDuplicateDep       = errors.New("duplicate dependency edge")
	ErrEmptyTaskID        = errors.New("empty task id")
	ErrDanglingDep        = errors.New("dependency references a task that does not exist")
	ErrSelfDependency     = errors.New("task depends on itself")
	ErrCyclicDependency   = errors.New("cyclic dependency detected")
	ErrNoTasks            = errors.New("workflow has no tasks")
	ErrInvalidRetryConfig = errors.New("invalid retry configuration")
	ErrInvalidTimeout     = errors.New("invalid timeout configuration")

	// Persistence / recovery errors.
	ErrCorruptedJournal   = errors.New("corrupted or partially written journal record")
	ErrJournalVersion     = errors.New("unsupported journal record version")
	ErrStateInconsistent  = errors.New("recovered task state violates invariants")
	ErrDefinitionMismatch = errors.New("workflow definition does not match persisted run")
	ErrRunNotFound        = errors.New("run not found in state directory")
	ErrRunAlreadyFinished = errors.New("run already in a terminal state")
	ErrStoreLocked        = errors.New("state directory is locked by another process")
)

// ValidationError carries structured detail about a rejected definition.
type ValidationError struct {
	Kind   error  // one of the sentinel errors above
	TaskID string // involved task (may be empty)
	Detail string
}

func (e *ValidationError) Error() string {
	msg := e.Kind.Error()
	if e.TaskID != "" {
		msg += " (task=" + e.TaskID + ")"
	}
	if e.Detail != "" {
		msg += ": " + e.Detail
	}
	return msg
}

func (e *ValidationError) Unwrap() error { return e.Kind }

// IsValidation reports whether err (or any wrapped error) is a definition rejection.
func IsValidation(err error) bool {
	var ve *ValidationError
	return errors.As(err, &ve)
}
