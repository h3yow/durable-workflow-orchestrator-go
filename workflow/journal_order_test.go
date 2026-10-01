package workflow

import (
	"context"
	"testing"
)

func TestJournalEventOrderRespectsDependencies(t *testing.T) {
	dir := t.TempDir()
	defs := []TaskDef{
		{ID: "a"},
		{ID: "b", Deps: []string{"a"}},
		{ID: "c", Deps: []string{"a"}},
		{ID: "d", Deps: []string{"b", "c"}},
	}
	wf, _ := Build("order", defs)
	eng, _ := NewEngine(Config{StateDir: dir, MaxWorkers: 4, Logger: discardLogger{}})
	defer eng.Close()
	noop := func(TaskContext) error { return nil }
	fns := map[string]TaskFunc{}
	for _, d := range defs {
		fns[d.ID] = noop
	}
	eng.Register(wf, fns)
	if _, err := eng.Run(context.Background(), "r", wf); err != nil {
		t.Fatal(err)
	}

	j, err := openJournal(dir, "r")
	if err != nil {
		t.Fatal(err)
	}
	defer j.Close()
	events, _, _, err := j.replay(wf)
	if err != nil {
		t.Fatal(err)
	}

	// position of the terminal (SUCCEEDED/...) record per task
	terminalAt := map[string]int{}
	startedAt := map[string]int{}
	for i, ev := range events {
		if ev.Type == EvTaskStarted {
			if _, ok := startedAt[ev.TaskID]; !ok {
				startedAt[ev.TaskID] = i
			}
		}
		if ev.Type == EvTaskSucceeded || ev.Type == EvTaskFailed ||
			ev.Type == EvTaskSkipped || ev.Type == EvTaskCanceled {
			if _, ok := terminalAt[ev.TaskID]; !ok {
				terminalAt[ev.TaskID] = i
			}
		}
	}
	for _, d := range defs {
		for _, dep := range d.Deps {
			if terminalAt[dep] >= startedAt[d.ID] {
				t.Fatalf("invariant violated: %s terminal@%d but %s STARTED@%d",
					dep, terminalAt[dep], d.ID, startedAt[d.ID])
			}
		}
		t.Logf("ORDER task=%s started@%d terminal@%d", d.ID, startedAt[d.ID], terminalAt[d.ID])
	}
}
