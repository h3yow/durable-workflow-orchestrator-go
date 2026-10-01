package workflow

import (
	"errors"
	"testing"
	"time"
)

func TestBuildRejectionCauses(t *testing.T) {
	cases := []struct {
		name string
		defs []TaskDef
		want error
		task string
	}{
		{
			name: "no tasks",
			defs: nil,
			want: ErrNoTasks,
		},
		{
			name: "empty task id",
			defs: []TaskDef{{ID: ""}},
			want: ErrEmptyTaskID,
		},
		{
			name: "duplicate id",
			defs: []TaskDef{{ID: "a"}, {ID: "a"}},
			want: ErrDuplicateTask,
			task: "a",
		},
		{
			name: "self dependency",
			defs: []TaskDef{{ID: "a", Deps: []string{"a"}}},
			want: ErrSelfDependency,
			task: "a",
		},
		{
			name: "dangling dependency",
			defs: []TaskDef{{ID: "a", Deps: []string{"ghost"}}},
			want: ErrDanglingDep,
			task: "a",
		},
		{
			name: "duplicate dependency edge",
			defs: []TaskDef{{ID: "a"}, {ID: "b", Deps: []string{"a", "a"}}},
			want: ErrDuplicateDep,
			task: "b",
		},
		{
			name: "direct cycle",
			defs: []TaskDef{{ID: "a", Deps: []string{"b"}}, {ID: "b", Deps: []string{"a"}}},
			want: ErrCyclicDependency,
		},
		{
			name: "indirect cycle",
			defs: []TaskDef{
				{ID: "a", Deps: []string{"c"}},
				{ID: "b", Deps: []string{"a"}},
				{ID: "c", Deps: []string{"b"}},
			},
			want: ErrCyclicDependency,
		},
		{
			name: "bad retry config",
			defs: []TaskDef{{ID: "a", Retry: RetryPolicy{MaxAttempts: 3, Backoff: BackoffPolicy{Kind: "bogus"}}}},
			want: ErrInvalidRetryConfig,
			task: "a",
		},
		{
			name: "bad timeout",
			defs: []TaskDef{{ID: "a", Timeout: Duration(-time.Second)}},
			want: ErrInvalidTimeout,
			task: "a",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			wf, err := Build(tc.name, tc.defs)
			if err == nil {
				t.Fatalf("expected rejection, got workflow: %v", wf)
			}
			var ve *ValidationError
			if !errors.As(err, &ve) {
				t.Fatalf("error %v is not a *ValidationError", err)
			}
			if !errors.Is(err, tc.want) {
				t.Fatalf("rejection cause = %v, want %v (detail: %s)", err, tc.want, ve.Detail)
			}
			if tc.task != "" && ve.TaskID != tc.task {
				t.Fatalf("task id = %q, want %q", ve.TaskID, tc.task)
			}
			t.Logf("INPUT %+v -> REJECTED reason=%s task=%s detail=%q", tc.defs, ve.Kind, ve.TaskID, ve.Detail)
		})
	}
}

func TestBuildAcceptsValidDAG(t *testing.T) {
	wf, err := Build("ok", []TaskDef{
		{ID: "extract"},
		{ID: "validate", Deps: []string{"extract"}},
		{ID: "load-a", Deps: []string{"validate"}},
		{ID: "load-b", Deps: []string{"validate"}},
		{ID: "finish", Deps: []string{"load-a", "load-b"}},
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(wf.order) != 5 {
		t.Fatalf("order=%v", wf.order)
	}
	t.Logf("accepted DAG order=%v", wf.order)
}
