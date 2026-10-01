package workflow

import (
	"context"
	"sync"
	"testing"
	"time"
)

// TestHappyPathFanInFanOut: root -> (a,b) -> join, outputs flow to join.
func TestHappyPathFanInFanOut(t *testing.T) {
	eng, _, lb := newTestEngine(t)
	def := WorkflowDef{
		Name: "fan", Concurrency: 4,
		Tasks: []TaskDef{
			task("join", "a", "b"),
			task("a", "root"),
			task("b", "root"),
			task("root"),
		},
	}
	reg := Registry{
		"root": func(_ context.Context, in TaskInput) (string, error) { return "R", nil },
		"a":    func(_ context.Context, in TaskInput) (string, error) { return in.Outputs["root"] + "A", nil },
		"b":    func(_ context.Context, in TaskInput) (string, error) { return in.Outputs["root"] + "B", nil },
		"join": func(_ context.Context, in TaskInput) (string, error) {
			return in.Outputs["a"] + "+" + in.Outputs["b"], nil
		},
	}
	res, err := eng.Start(context.Background(), def, reg)
	if err != nil {
		t.Fatal(err)
	}
	if res.Status != StatusSucceeded {
		t.Fatalf("run status=%s reason=%s", res.Status, res.Reason)
	}
	for _, id := range []string{"root", "a", "b", "join"} {
		assertStatus(t, res, id, StatusSucceeded)
	}
	var join *TaskResult
	for i := range res.Tasks {
		if res.Tasks[i].ID == "join" {
			join = &res.Tasks[i]
		}
	}
	if join.Output != "RA+RB" {
		t.Fatalf("fan-in output=%q want RA+RB", join.Output)
	}
	t.Logf("DECISION LOG:\n%s", lb.String())
	t.Logf("final: status=%s join=%q", res.Status, join.Output)
}

// TestExactlyOnceAndDeterministicResults: every task runs exactly once, the
// concurrency cap is honored, dependencies are respected, and identical
// inputs produce identical terminal results across repeated runs.
func TestExactlyOnceAndDeterministicResults(t *testing.T) {
	const runs = 5
	var prevFP string
	for r := 0; r < runs; r++ {
		eng, _, lb := newTestEngine(t)
		def := WorkflowDef{
			Name: "once", Concurrency: 2,
			Tasks: []TaskDef{
				task("a"), task("b"), task("c", "a"),
				task("d", "a"), task("e", "b", "c"),
			},
		}
		var mu sync.Mutex
		counts := map[string]int{}
		var order []string
		maxInflight, cur := 0, 0
		reg := Registry{}
		for _, td := range def.Tasks {
			id := td.ID
			reg[id] = func(_ context.Context, in TaskInput) (string, error) {
				mu.Lock()
				counts[id]++
				cur++
				if cur > maxInflight {
					maxInflight = cur
				}
				order = append(order, id)
				mu.Unlock()
				time.Sleep(5 * time.Millisecond)
				mu.Lock()
				cur--
				mu.Unlock()
				return id, nil
			}
		}
		res, err := eng.Start(context.Background(), def, reg)
		if err != nil {
			t.Fatalf("run %d: %v", r, err)
		}
		if res.Status != StatusSucceeded {
			t.Fatalf("run %d status=%s reason=%s", r, res.Status, res.Reason)
		}
		for _, td := range def.Tasks {
			if counts[td.ID] != 1 {
				t.Fatalf("run %d task %s executed %d times, want 1", r, td.ID, counts[td.ID])
			}
		}
		if maxInflight > 2 {
			t.Fatalf("concurrency limit violated: %d > 2", maxInflight)
		}
		pos := map[string]int{}
		for i, id := range order {
			pos[id] = i
		}
		for _, td := range def.Tasks {
			for _, d := range td.Dependencies {
				if pos[d] > pos[td.ID] {
					t.Fatalf("run %d dependency order violated: %s after %s", r, d, td.ID)
				}
			}
		}
		fp := ""
		for _, tr := range res.Tasks {
			fp += tr.ID + "=" + string(tr.Status) + tr.Output + "|"
		}
		if r == 0 {
			prevFP = fp
			t.Logf("input tasks=%d concurrency=2; dispatch order=%v; max inflight=%d; fingerprint=%s",
				len(def.Tasks), order, maxInflight, fp)
			t.Logf("DECISION LOG:\n%s", lb.String())
		} else if fp != prevFP {
			t.Fatalf("non-deterministic run result:\n%v\nvs\n%v", fp, prevFP)
		}
	}
}

// TestConcurrencyOneSerializes proves the limit is honored with a wide DAG.
func TestConcurrencyOneSerializes(t *testing.T) {
	eng, _, _ := newTestEngine(t)
	def := WorkflowDef{Name: "serial", Concurrency: 1, Tasks: []TaskDef{
		task("a"), task("b"), task("c"),
	}}
	var mu sync.Mutex
	inflight, max := 0, 0
	reg := Registry{}
	for _, td := range def.Tasks {
		id := td.ID
		reg[id] = func(context.Context, TaskInput) (string, error) {
			mu.Lock()
			inflight++
			if inflight > max {
				max = inflight
			}
			mu.Unlock()
			time.Sleep(5 * time.Millisecond)
			mu.Lock()
			inflight--
			mu.Unlock()
			return id, nil
		}
	}
	res, err := eng.Start(context.Background(), def, reg)
	if err != nil {
		t.Fatal(err)
	}
	if res.Status != StatusSucceeded || max != 1 {
		t.Fatalf("status=%s maxInflight=%d", res.Status, max)
	}
}
