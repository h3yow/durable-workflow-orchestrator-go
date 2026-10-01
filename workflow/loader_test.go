package workflow

import (
	"os"
	"path/filepath"
	"testing"
)

func TestLoadDefinition(t *testing.T) {
	// Walk upward to find examples/ (test CWD is the package directory).
	dir, _ := os.Getwd()
	path := filepath.Join(dir, "..", "examples", "workflow.json")
	def, err := LoadDefinition(path)
	if err != nil {
		t.Fatalf("LoadDefinition: %v", err)
	}
	if def.Name != "etl-pipeline" || def.Concurrency != 3 {
		t.Fatalf("unexpected definition: %+v", def)
	}
	if d := def.RunTimeout.Std(); d.String() != "30s" {
		t.Fatalf("run timeout parsed as %s", d)
	}
	if p := def.Tasks[0].Retry; p.MaxRetries != 3 || p.Backoff != BackoffExponent ||
		p.InitialBackoff.Std().String() != "100ms" || p.MaxBackoff.Std().String() != "2s" {
		t.Fatalf("retry policy parsed wrong: %+v", p)
	}
	t.Logf("loaded %s: %d tasks, order=%v", def.Name, len(def.Tasks), TopoOrder(def))
}

func TestLoadDefinitionRejectsBadFile(t *testing.T) {
	p := filepath.Join(t.TempDir(), "bad.json")
	if err := os.WriteFile(p, []byte(`{"tasks":[{"id":"a","dependencies":["x"]}]}`), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := LoadDefinition(p); err == nil {
		t.Fatal("expected invalid definition to be rejected")
	}
}
