// Command wfdemo runs a fully local, durable workflow demonstration.
//
// Usage:
//
//	go run . run                 # start a fresh demo run in ./wfdata
//	go run . list                # list persisted runs
//	go run . resume <runID>      # resume a run after a restart
//	go run . crash               # start a run whose 'flaky' task simulates
//	                             # a process crash on attempt 1; resume it
//	                             # afterwards with: go run . resume <runID>
package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"sync/atomic"
	"time"

	wf "github.com/h3yow/durable-workflow-orchestrator-go/workflow"
)

const storeDir = "wfdata"

func demoDefinition() wf.WorkflowDef {
	return wf.WorkflowDef{
		Name:        "demo-pipeline",
		Version:     "1.0",
		Concurrency: 2,
		RunTimeout:  wf.Duration(30 * time.Second),
		Tasks: []wf.TaskDef{
			{ID: "fetch"},
			{ID: "validate", Dependencies: []string{"fetch"},
				Retry: wf.RetryPolicy{
					Timeout: wf.Duration(2 * time.Second), MaxRetries: 2,
					Backoff:        wf.BackoffExponent,
					InitialBackoff: wf.Duration(200 * time.Millisecond),
					MaxBackoff:     wf.Duration(time.Second),
				}},
			{ID: "normalize-a", Dependencies: []string{"validate"}},
			{ID: "normalize-b", Dependencies: []string{"validate"}},
			{ID: "aggregate", Dependencies: []string{"normalize-a", "normalize-b"}},
			{ID: "report", Dependencies: []string{"aggregate"}},
		},
	}
}

func demoRegistry(crash bool) wf.Registry {
	var flakyAttempts int32
	ok := func(_ context.Context, in wf.TaskInput) (string, error) {
		time.Sleep(50 * time.Millisecond)
		return "done:" + in.AttemptKey, nil
	}
	return wf.Registry{
		"fetch": func(_ context.Context, in wf.TaskInput) (string, error) {
			fmt.Println("  -> fetching input")
			return "raw-data", nil
		},
		"validate": func(ctx context.Context, in wf.TaskInput) (string, error) {
			n := atomic.AddInt32(&flakyAttempts, 1)
			if n < 3 {
				if crash && n == 1 {
					fmt.Println("  -> validate: simulated process crash (exiting hard)")
					os.Exit(2)
				}
				fmt.Printf("  -> validate: transient failure on attempt %d (retryable)\n", n)
				return "", wf.Retryable(errors.New("simulated transient validation error"))
			}
			fmt.Println("  -> validate: ok")
			return "valid:" + in.Outputs["fetch"], nil
		},
		"normalize-a": ok,
		"normalize-b": ok,
		"aggregate": func(_ context.Context, in wf.TaskInput) (string, error) {
			return in.Outputs["normalize-a"] + "|" + in.Outputs["normalize-b"], nil
		},
		"report": func(_ context.Context, in wf.TaskInput) (string, error) {
			fmt.Printf("  -> report: %s\n", in.Outputs["aggregate"])
			return "reported", nil
		},
	}
}

func main() {
	store, err := wf.NewFileStore(storeDir)
	if err != nil {
		log.Fatalf("open store: %v", err)
	}
	eng := wf.NewEngine(wf.Options{Store: store})
	def := demoDefinition()

	cmd := "run"
	var arg string
	if len(os.Args) > 1 {
		cmd = os.Args[1]
	}
	if len(os.Args) > 2 {
		arg = os.Args[2]
	}

	switch cmd {
	case "run", "crash":
		res, err := eng.Start(context.Background(), def, demoRegistry(cmd == "crash"))
		if err != nil {
			log.Fatalf("start: %v", err)
		}
		printSummary(res)
		fmt.Printf("\nrun state persisted under %s\n", filepath.Join(storeDir, "runs", res.RunID))
		fmt.Printf("resume with: go run . resume %s\n", res.RunID)
	case "resume":
		if arg == "" {
			log.Fatal("usage: resume <runID>")
		}
		res, err := eng.Resume(context.Background(), arg, def, demoRegistry(false))
		if err != nil {
			log.Fatalf("resume: %v", err)
		}
		printSummary(res)
	case "list":
		ids, err := store.ListRuns(context.Background())
		if err != nil {
			log.Fatalf("list: %v", err)
		}
		if len(ids) == 0 {
			fmt.Println("(no runs)")
		}
		for _, id := range ids {
			st, err := store.LoadRun(context.Background(), id)
			if err != nil {
				fmt.Printf("%s  [CORRUPT: %v]\n", id, err)
				continue
			}
			fmt.Printf("%s  status=%-9s name=%s\n", id, st.Status, st.Definition.Name)
		}
	default:
		log.Fatalf("unknown command %q (want run | crash | resume <id> | list)", cmd)
	}
}

func printSummary(res *wf.RunResult) {
	fmt.Printf("\nrun %s: %s (%s)\n", res.RunID, res.Status, res.Reason)
	for _, t := range res.Tasks {
		fmt.Printf("  %-12s %-9s attempts=%d output=%q reason=%q\n",
			t.ID, t.Status, t.Attempts, t.Output, t.Reason)
	}
}
