package workflow

import (
	"encoding/json"
	"fmt"
	"time"
)

// Duration is a time.Duration with human-friendly JSON serialization.
// JSON form is a duration string ("500ms", "2s"); a bare JSON number is
// interpreted as nanoseconds.
type Duration time.Duration

// Std converts to the standard library duration.
func (d Duration) Std() time.Duration { return time.Duration(d) }

// MarshalJSON emits the duration as a string like "2s".
func (d Duration) MarshalJSON() ([]byte, error) {
	return json.Marshal(time.Duration(d).String())
}

// UnmarshalJSON accepts a duration string ("500ms") or a number of nanoseconds.
func (d *Duration) UnmarshalJSON(b []byte) error {
	var s string
	if err := json.Unmarshal(b, &s); err == nil {
		parsed, perr := time.ParseDuration(s)
		if perr != nil {
			return fmt.Errorf("invalid duration %q: %w", s, perr)
		}
		*d = Duration(parsed)
		return nil
	}
	var n int64
	if err := json.Unmarshal(b, &n); err == nil {
		*d = Duration(time.Duration(n))
		return nil
	}
	return fmt.Errorf("invalid duration: %s", string(b))
}
