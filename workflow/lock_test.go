package workflow

import (
	"errors"
	"testing"
)

func TestStateDirLockExclusive(t *testing.T) {
	dir := t.TempDir()
	eng1, err := NewEngine(Config{StateDir: dir, MaxWorkers: 1, Logger: discardLogger{}})
	if err != nil {
		t.Fatal(err)
	}
	defer eng1.Close()

	eng2, err := NewEngine(Config{StateDir: dir, MaxWorkers: 1, Logger: discardLogger{}})
	if !errors.Is(err, ErrStoreLocked) {
		if eng2 != nil {
			eng2.Close()
		}
		t.Fatalf("second engine err=%v, want ErrStoreLocked", err)
	}
	t.Logf("LOCK second concurrent engine -> REJECTED: %v", err)

	// Once released, the directory can be locked again.
	if err := eng1.Close(); err != nil {
		t.Fatal(err)
	}
	eng3, err := NewEngine(Config{StateDir: dir, MaxWorkers: 1, Logger: discardLogger{}})
	if err != nil {
		t.Fatalf("lock after release: %v", err)
	}
	eng3.Close()
}
