package workflow

import (
	"errors"
	"testing"
)

func TestValidateRejectsDistinctCauses(t *testing.T) {
	cases := []struct {
		name string
		def  WorkflowDef
		want error
	}{
		{
			name: "duplicate id",
			def: WorkflowDef{Tasks: []TaskDef{
				{ID: "a"}, {ID: "a"},
			}},
			want: ErrDuplicateTask,
		},
		{
			name: "unknown dependency",
			def: WorkflowDef{Tasks: []TaskDef{
				{ID: "a", Dependencies: []string{"ghost"}},
			}},
			want: ErrUnknownDependency,
		},
		{
			name: "direct cycle",
			def: WorkflowDef{Tasks: []TaskDef{
				{ID: "a", Dependencies: []string{"b"}},
				{ID: "b", Dependencies: []string{"a"}},
			}},
			want: ErrCyclicDependency,
		},
		{
			name: "self cycle",
			def: WorkflowDef{Tasks: []TaskDef{
				{ID: "a", Dependencies: []string{"a"}},
			}},
			want: ErrCyclicDependency,
		},
		{
			name: "long cycle",
			def: WorkflowDef{Tasks: []TaskDef{
				{ID: "a", Dependencies: []string{"b"}},
				{ID: "b", Dependencies: []string{"c"}},
				{ID: "c", Dependencies: []string{"a"}},
				{ID: "d"},
			}},
			want: ErrCyclicDependency,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			err := ValidateDefinition(tc.def)
			if err == nil {
				t.Fatalf("expected rejection, got nil")
			}
			if !errors.Is(err, tc.want) {
				var ve *ValidationError
				errors.As(err, &ve)
				t.Fatalf("expected cause %v, got %v (task=%s)", tc.want, err, ve.TaskID)
			}
			t.Logf("input=%+v => rejection: %v", tc.def, err)
		})
	}
}

func TestValidateAcceptsDAG(t *testing.T) {
	def := WorkflowDef{Tasks: []TaskDef{
		{ID: "root"},
		{ID: "a", Dependencies: []string{"root"}},
		{ID: "b", Dependencies: []string{"root"}},
		{ID: "join", Dependencies: []string{"a", "b"}},
	}}
	if err := ValidateDefinition(def); err != nil {
		t.Fatalf("valid DAG rejected: %v", err)
	}
	order := TopoOrder(def)
	t.Logf("topological order: %v", order)
	pos := map[string]int{}
	for i, id := range order {
		pos[id] = i
	}
	for _, task := range def.Tasks {
		for _, d := range task.Dependencies {
			if pos[d] > pos[task.ID] {
				t.Fatalf("order violated: %s before %s", d, task.ID)
			}
		}
	}
}
