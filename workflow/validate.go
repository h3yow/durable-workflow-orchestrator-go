package workflow

import "sort"

// ValidateDefinition rejects invalid workflow definitions.
// The error wraps one of ErrDuplicateTask / ErrUnknownDependency /
// ErrCyclicDependency, with a TaskID pinpointing the offending task.
func ValidateDefinition(def WorkflowDef) error {
	seen := make(map[string]bool, len(def.Tasks))
	for _, t := range def.Tasks {
		if t.ID == "" {
			return &ValidationError{TaskID: t.ID, Kind: ErrDuplicateTask,
				Reason: "task id must not be empty"}
		}
		if seen[t.ID] {
			return &ValidationError{TaskID: t.ID, Kind: ErrDuplicateTask,
				Reason: "duplicate task id: " + t.ID}
		}
		seen[t.ID] = true
	}
	for _, t := range def.Tasks {
		known := make(map[string]bool)
		for _, dep := range t.Dependencies {
			if dep == t.ID {
				return &ValidationError{TaskID: t.ID, Kind: ErrCyclicDependency,
					Reason: "task " + t.ID + " depends on itself"}
			}
			if known[dep] {
				return &ValidationError{TaskID: t.ID, Kind: ErrDuplicateTask,
					Reason: "task " + t.ID + " declares duplicate dependency " + dep}
			}
			known[dep] = true
			if !seen[dep] {
				return &ValidationError{TaskID: t.ID, Kind: ErrUnknownDependency,
					Reason: "task " + t.ID + " depends on unknown task " + dep}
			}
		}
	}
	if cycle := findCycle(def); cycle != "" {
		return &ValidationError{TaskID: cycle, Kind: ErrCyclicDependency,
			Reason: "cyclic dependency detected involving task: " + cycle}
	}
	return nil
}

// TopoOrder returns task IDs in a deterministic topological order
// (lexicographic tie break), so identical inputs always schedule identically.
func TopoOrder(def WorkflowDef) []string {
	indeg := make(map[string]int, len(def.Tasks))
	dependents := make(map[string][]string, len(def.Tasks))
	for _, t := range def.Tasks {
		indeg[t.ID] = len(t.Dependencies)
	}
	for _, t := range def.Tasks {
		for _, dep := range t.Dependencies {
			dependents[dep] = append(dependents[dep], t.ID)
		}
	}
	for id := range dependents {
		sort.Strings(dependents[id])
	}
	ready := make([]string, 0)
	for _, t := range def.Tasks {
		if indeg[t.ID] == 0 {
			ready = append(ready, t.ID)
		}
	}
	sort.Strings(ready)
	order := make([]string, 0, len(def.Tasks))
	for len(ready) > 0 {
		id := ready[0]
		ready = ready[1:]
		order = append(order, id)
		for _, next := range dependents[id] {
			indeg[next]--
			if indeg[next] == 0 {
				ready = append(ready, next)
				sort.Strings(ready)
			}
		}
	}
	return order
}

// findCycle returns a task ID participating in a dependency cycle, or "".
func findCycle(def WorkflowDef) string {
	const (
		white = 0
		gray  = 1
		black = 2
	)
	color := make(map[string]int, len(def.Tasks))
	deps := make(map[string][]string)
	for _, t := range def.Tasks {
		d := append([]string(nil), t.Dependencies...)
		sort.Strings(d)
		deps[t.ID] = d
	}
	var dfs func(id string) string
	dfs = func(id string) string {
		color[id] = gray
		for _, dep := range deps[id] {
			if color[dep] == gray {
				return dep
			}
			if color[dep] == white {
				if c := dfs(dep); c != "" {
					return c
				}
			}
		}
		color[id] = black
		return ""
	}
	ids := make([]string, 0, len(def.Tasks))
	for _, t := range def.Tasks {
		ids = append(ids, t.ID)
	}
	sort.Strings(ids)
	for _, id := range ids {
		if color[id] == white {
			if c := dfs(id); c != "" {
				return c
			}
		}
	}
	return ""
}
