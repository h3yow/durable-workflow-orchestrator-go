package workflow

import (
	"encoding/json"
	"fmt"
	"os"
)

// LoadDefinition reads and validates a workflow definition from a JSON file.
//
// Executors are never expressed in JSON: the file describes topology and
// retry policy, while task behavior is supplied as a Registry at run time.
func LoadDefinition(path string) (WorkflowDef, error) {
	b, err := os.ReadFile(path)
	if err != nil {
		return WorkflowDef{}, fmt.Errorf("read definition %s: %w", path, err)
	}
	var def WorkflowDef
	if err := json.Unmarshal(b, &def); err != nil {
		return WorkflowDef{}, fmt.Errorf("parse definition %s: %w", path, err)
	}
	if err := ValidateDefinition(def); err != nil {
		return WorkflowDef{}, err
	}
	normalizeDef(&def)
	return def, nil
}
