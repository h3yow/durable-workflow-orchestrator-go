package workflow

// Build validates a set of task definitions and returns an immutable Workflow.
// Distinct rejection causes:
//   - ErrNoTasks / ErrEmptyTaskID / ErrDuplicateTask
//   - ErrSelfDependency / ErrDanglingDep
//   - ErrCyclicDependency
//   - ErrInvalidRetryConfig / ErrInvalidTimeout
func Build(name string, defs []TaskDef) (*Workflow, error) {
	if len(defs) == 0 {
		return nil, &ValidationError{Kind: ErrNoTasks}
	}

	wf := &Workflow{Name: name, Tasks: make(map[string]*TaskDef, len(defs))}

	for i := range defs {
		d := &defs[i]
		if d.ID == "" {
			return nil, &ValidationError{Kind: ErrEmptyTaskID, Detail: "task at index " + itoa(i)}
		}
		if _, exists := wf.Tasks[d.ID]; exists {
			return nil, &ValidationError{Kind: ErrDuplicateTask, TaskID: d.ID}
		}
		if d.Timeout < 0 {
			return nil, &ValidationError{Kind: ErrInvalidTimeout, TaskID: d.ID, Detail: "timeout must be >= 0"}
		}
		if d.Retry.MaxAttempts < 0 {
			return nil, &ValidationError{Kind: ErrInvalidRetryConfig, TaskID: d.ID, Detail: "max_attempts must be >= 0"}
		}
		if d.Retry.MaxAttempts > 1 {
			b := d.Retry.Backoff
			if b.Interval < 0 || b.MaxDelay < 0 {
				return nil, &ValidationError{Kind: ErrInvalidRetryConfig, TaskID: d.ID, Detail: "backoff delays must be >= 0"}
			}
			if b.Kind == "" {
				// default: fixed zero delay
			} else if b.Kind != BackoffFixed && b.Kind != BackoffLinear && b.Kind != BackoffExponent {
				return nil, &ValidationError{Kind: ErrInvalidRetryConfig, TaskID: d.ID, Detail: "unknown backoff kind: " + string(b.Kind)}
			}
		}
		wf.Tasks[d.ID] = d
		wf.order = append(wf.order, d.ID)
	}

	// Dependency existence + self dependency (checked before cycle detection
	// so these distinct causes are reported precisely).
	for _, id := range wf.order {
		d := wf.Tasks[id]
		seen := make(map[string]bool, len(d.Deps))
		for _, dep := range d.Deps {
			if dep == id {
				return nil, &ValidationError{Kind: ErrSelfDependency, TaskID: id}
			}
			if _, ok := wf.Tasks[dep]; !ok {
				return nil, &ValidationError{Kind: ErrDanglingDep, TaskID: id, Detail: "missing dependency: " + dep}
			}
			if seen[dep] {
				// repeated dep edge is harmless for scheduling but signals a bad
				// definition; reject it distinctly.
				return nil, &ValidationError{Kind: ErrDuplicateDep, TaskID: id, Detail: "duplicate dependency edge: " + dep}
			}
			seen[dep] = true
		}
	}

	if err := detectCycle(wf); err != nil {
		return nil, err
	}

	return wf, nil
}

func itoa(i int) string {
	if i == 0 {
		return "0"
	}
	neg := i < 0
	if neg {
		i = -i
	}
	var b [20]byte
	p := len(b)
	for i > 0 {
		p--
		b[p] = byte('0' + i%10)
		i /= 10
	}
	if neg {
		p--
		b[p] = '-'
	}
	return string(b[p:])
}

// detectCycle runs Kahn's algorithm in definition order. It returns
// ErrCyclicDependency naming the tasks that remain inside the cycle.
func detectCycle(wf *Workflow) error {
	indeg := make(map[string]int, len(wf.order))
	dependents := make(map[string][]string, len(wf.order))
	for _, id := range wf.order {
		indeg[id] = len(wf.Tasks[id].Deps)
		for _, dep := range wf.Tasks[id].Deps {
			dependents[dep] = append(dependents[dep], id)
		}
	}

	// deterministic Kahn queue: process in definition order
	queue := make([]string, 0)
	for _, id := range wf.order {
		if indeg[id] == 0 {
			queue = append(queue, id)
		}
	}
	visited := 0
	for len(queue) > 0 {
		cur := queue[0]
		queue = queue[1:]
		visited++
		// gather newly-ready nodes, then merge in definition order
		var ready []string
		for _, dn := range dependents[cur] {
			indeg[dn]--
			if indeg[dn] == 0 {
				ready = append(ready, dn)
			}
		}
		if len(ready) > 0 {
			queue = append(queue, ready...)
			// keep queue sorted by definition order for determinism
			sortByDefOrder(queue, wf.order)
		}
	}

	if visited == len(wf.order) {
		return nil
	}
	var cyclic []string
	for _, id := range wf.order {
		if indeg[id] > 0 {
			cyclic = append(cyclic, id)
		}
	}
	return &ValidationError{Kind: ErrCyclicDependency, Detail: "tasks involved: " + joinIDs(cyclic)}
}

func sortByDefOrder(ids, defOrder []string) {
	rank := make(map[string]int, len(defOrder))
	for i, id := range defOrder {
		rank[id] = i
	}
	for i := 1; i < len(ids); i++ {
		for j := i; j > 0 && rank[ids[j-1]] > rank[ids[j]]; j-- {
			ids[j-1], ids[j] = ids[j], ids[j-1]
		}
	}
}

func joinIDs(ids []string) string {
	out := ""
	for i, id := range ids {
		if i > 0 {
			out += ","
		}
		out += id
	}
	return out
}

// topoLevels returns tasks in deterministic topological layers (each layer
// only depends on earlier layers). Within a layer definition order is kept.
func (wf *Workflow) topoLayers() [][]string {
	indeg := make(map[string]int, len(wf.order))
	dependents := make(map[string][]string, len(wf.order))
	for _, id := range wf.order {
		indeg[id] = len(wf.Tasks[id].Deps)
		for _, dep := range wf.Tasks[id].Deps {
			dependents[dep] = append(dependents[dep], id)
		}
	}
	var layers [][]string
	remaining := len(wf.order)
	for remaining > 0 {
		var layer []string
		for _, id := range wf.order {
			if indeg[id] == 0 {
				layer = append(layer, id)
			}
		}
		if len(layer) == 0 {
			return nil // cycle (already rejected by Build)
		}
		layers = append(layers, layer)
		for _, id := range layer {
			indeg[id] = -1
			remaining--
			for _, dn := range dependents[id] {
				indeg[dn]--
			}
		}
	}
	return layers
}
