package workflow

import (
	"encoding/json"
	"time"
)

// MarshalJSON encodes Duration as a string like "250ms".
func (d Duration) MarshalJSON() ([]byte, error) {
	return json.Marshal(time.Duration(d).String())
}

// UnmarshalJSON accepts either a duration string ("2s") or a JSON number
// interpreted as nanoseconds.
func (d *Duration) UnmarshalJSON(b []byte) error {
	var v any
	if err := json.Unmarshal(b, &v); err != nil {
		return err
	}
	switch x := v.(type) {
	case string:
		td, err := time.ParseDuration(x)
		if err != nil {
			return err
		}
		*d = Duration(td)
	case float64:
		*d = Duration(time.Duration(x))
	default:
		*d = 0
	}
	return nil
}

func (d Duration) std() time.Duration { return time.Duration(d) }
