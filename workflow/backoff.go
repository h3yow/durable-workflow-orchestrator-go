package workflow

import (
	"math"
	"math/rand"
	"time"
)

// backoffDelay computes the wait before retry number `retryNo` (1-based:
// the wait after the first failed attempt is retryNo == 1).
func backoffDelay(p BackoffPolicy, retryNo int) time.Duration {
	if retryNo < 1 {
		return 0
	}
	base := time.Duration(p.Interval)
	if base <= 0 {
		return 0
	}
	var d time.Duration
	switch p.Kind {
	case BackoffLinear, "":
		d = base * time.Duration(retryNo)
	case BackoffExponent:
		mult := p.Multiplier
		if mult <= 1 {
			mult = 2
		}
		factor := math.Pow(mult, float64(retryNo-1))
		d = time.Duration(float64(base) * factor)
	default: // BackoffFixed
		d = base
	}
	if maxd := time.Duration(p.MaxDelay); maxd > 0 && d > maxd {
		d = maxd
	}
	if p.Jitter && d > 0 {
		// randomize into [d/2, d) — timing jitter only, outcomes unchanged
		half := d / 2
		d = half + time.Duration(rand.Int63n(int64(half)))
	}
	return d
}
