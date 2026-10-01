package workflow

import (
	"errors"
	"time"
)

// RetryableError marks an error as eligible for retry.
type RetryableError struct{ Err error }

func (e *RetryableError) Error() string { return "retryable: " + e.Err.Error() }
func (e *RetryableError) Unwrap() error { return e.Err }

// Retryable wraps err so the engine will retry the attempt (up to MaxRetries).
func Retryable(err error) error {
	if err == nil {
		return nil
	}
	return &RetryableError{Err: err}
}

// PermanentError marks an error as non-retryable.
type PermanentError struct{ Err error }

func (e *PermanentError) Error() string { return "permanent: " + e.Err.Error() }
func (e *PermanentError) Unwrap() error { return e.Err }

// Permanent wraps err so the engine fails the task immediately.
func Permanent(err error) error {
	if err == nil {
		return nil
	}
	return &PermanentError{Err: err}
}

// nextBackoff computes the delay before the retry of the given attempt.
// Attempt counts the attempt that just failed (1-based).
func nextBackoff(p RetryPolicy, attempt int) time.Duration {
	init := p.InitialBackoff.Std()
	if init <= 0 {
		return 0
	}
	var d time.Duration
	switch p.Backoff {
	case BackoffFixed:
		d = init
	case BackoffLinear:
		d = init * time.Duration(attempt)
	case BackoffExponent, "":
		d = init << (attempt - 1)
	default:
		d = init
	}
	if max := p.MaxBackoff.Std(); max > 0 && d > max {
		d = max
	}
	return d
}

// IsRetryable reports whether err was wrapped with Retryable.
func IsRetryable(err error) bool {
	var r *RetryableError
	return errors.As(err, &r)
}

// IsPermanent reports whether err was wrapped with Permanent.
func IsPermanent(err error) bool {
	var pe *PermanentError
	return errors.As(err, &pe)
}
