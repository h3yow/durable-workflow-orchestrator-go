package workflow

import "fmt"

// TaskError lets task implementations explicitly mark a failure as
// retryable or permanent. A plain (non-*TaskError) error is treated as retryable.
type TaskError struct {
	Retryable bool
	Cause     error
	Message   string
}

func (e *TaskError) Error() string {
	kind := "permanent"
	if e.Retryable {
		kind = "retryable"
	}
	if e.Cause != nil {
		return fmt.Sprintf("task error (%s): %s: %v", kind, e.Message, e.Cause)
	}
	return fmt.Sprintf("task error (%s): %s", kind, e.Message)
}

func (e *TaskError) Unwrap() error { return e.Cause }

// PermanentError builds a non-retryable task error.
func PermanentError(format string, args ...any) error {
	return &TaskError{Retryable: false, Message: fmt.Sprintf(format, args...)}
}

// RetryableError builds a retryable task error.
func RetryableError(format string, args ...any) error {
	return &TaskError{Retryable: true, Message: fmt.Sprintf(format, args...)}
}

// asTaskError classifies an error returned by a task.
func classifyError(err error) (retryable bool, msg string) {
	if err == nil {
		return false, ""
	}
	var te *TaskError
	if asTaskError(err, &te) {
		return te.Retryable, te.Error()
	}
	return true, err.Error()
}

func asTaskError(err error, target **TaskError) bool {
	for err != nil {
		if te, ok := err.(*TaskError); ok {
			*target = te
			return true
		}
		u, ok := err.(interface{ Unwrap() error })
		if !ok {
			return false
		}
		err = u.Unwrap()
	}
	return false
}
