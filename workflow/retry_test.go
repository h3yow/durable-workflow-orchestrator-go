package workflow

import (
	"errors"
	"testing"
	"time"
)

func TestErrorClassification(t *testing.T) {
	base := errors.New("boom")
	if !IsRetryable(Retryable(base)) {
		t.Fatal("Retryable should classify as retryable")
	}
	if IsRetryable(Permanent(base)) {
		t.Fatal("Permanent should not classify as retryable")
	}
	if !IsPermanent(Permanent(base)) {
		t.Fatal("Permanent should classify as permanent")
	}
	if IsPermanent(base) {
		t.Fatal("plain error should be treated as permanent by executors but IsPermanent only matches wrapper")
	}
	if !errors.Is(Retryable(base), base) {
		t.Fatal("Retryable must unwrap to base error")
	}
	t.Logf("classification input=%v => retryable=%v permanent=%v", base, IsRetryable(Retryable(base)), IsPermanent(Permanent(base)))
}

func TestNextBackoff(t *testing.T) {
	p := RetryPolicy{InitialBackoff: Duration(100 * time.Millisecond), MaxBackoff: Duration(time.Second)}
	cases := []struct {
		kind    BackoffKind
		attempt int
		want    time.Duration
	}{
		{BackoffFixed, 1, 100 * time.Millisecond},
		{BackoffFixed, 3, 100 * time.Millisecond},
		{BackoffLinear, 1, 100 * time.Millisecond},
		{BackoffLinear, 3, 300 * time.Millisecond},
		{BackoffExponent, 1, 100 * time.Millisecond},
		{BackoffExponent, 2, 200 * time.Millisecond},
		{BackoffExponent, 3, 400 * time.Millisecond},
		{BackoffExponent, 5, 1 * time.Second}, // capped at MaxBackoff
	}
	for _, tc := range cases {
		p.Backoff = tc.kind
		got := nextBackoff(p, tc.attempt)
		if got != tc.want {
			t.Errorf("%s attempt %d: want %s got %s", tc.kind, tc.attempt, tc.want, got)
		}
		t.Logf("backoff kind=%s attempt=%d => %s", tc.kind, tc.attempt, got)
	}
}

func TestDurationJSON(t *testing.T) {
	var d Duration
	if err := d.UnmarshalJSON([]byte(`"250ms"`)); err != nil {
		t.Fatal(err)
	}
	if d.Std() != 250*time.Millisecond {
		t.Fatalf("got %v", d.Std())
	}
	if err := d.UnmarshalJSON([]byte(`1000000`)); err != nil {
		t.Fatal(err)
	}
	if d.Std() != time.Millisecond {
		t.Fatalf("numeric duration got %v", d.Std())
	}
	if err := d.UnmarshalJSON([]byte(`"not-a-duration"`)); err == nil {
		t.Fatal("expected unmarshal error")
	}
	t.Logf("duration parsing OK")
}
