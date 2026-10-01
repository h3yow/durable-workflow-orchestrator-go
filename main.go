// Command workflow-demo runs a fully local demonstration of the durable
// workflow engine: dependency fan-out/fan-in, retries with backoff, and a
// real process crash followed by restart recovery from a local journal.
//
// Usage:
//
//	go run . [--state-dir DIR] [--reset]
//
// The parent process spawns child processes (--child). One task ("combine")
// kills its own process on the first attempt to simulate a crash; the parent
// restarts it and the run resumes from the durable journal. Side effects are
// appended to <state-dir>/effects.log so you can verify every task's effect
// happened exactly once.
package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"time"

	wf "github.com/h3yow/durable-workflow-orchestrator-go/workflow"
)

func main() {
	if err := run(os.Args[1:]); err != nil {
		fmt.Fprintf(os.Stderr, "demo error: %v\n", err)
		os.Exit(1)
	}
}

func run(args []string) error {
	fs := flag.NewFlagSet("workflow-demo", flag.ContinueOnError)
	stateDir := fs.String("state-dir", "./.wf-data", "local state directory")
	child := fs.Bool("child", false, "run as a (possibly restarted) child process")
	reset := fs.Bool("reset", false, "delete the state directory before running")
	if err := fs.Parse(args); err != nil {
		return err
	}

	if *reset {
		if err := os.RemoveAll(*stateDir); err != nil {
			return err
		}
	}

	if *child {
		return runChild(*stateDir)
	}
	return runParent(*stateDir)
}

func workflowDefs() []wf.TaskDef {
	return []wf.TaskDef{
		{ID: "prepare"},
		{ID: "fetch-a", Deps: []string{"prepare"}},
		{ID: "fetch-b", Deps: []string{"prepare"}},
		// "enrich" fails its first two attempts, then succeeds.
		{
			ID:      "enrich",
			Deps:    []string{"fetch-a"},
			Timeout: wf.Duration(2 * time.Second),
			Retry: wf.RetryPolicy{
				MaxAttempts: 4,
				Backoff:     wf.BackoffPolicy{Kind: wf.BackoffFixed, Interval: wf.Duration(50 * time.Millisecond)},
			},
		},
		// "combine" fans in and crashes the process on its first attempt.
		{ID: "combine", Deps: []string{"fetch-b", "enrich"}},
		{ID: "report", Deps: []string{"combine"}},
	}
}

func runChild(stateDir string) error {
	w, err := wf.Build("demo-pipeline", workflowDefs())
	if err != nil {
		return err
	}
	eng, err := wf.NewEngine(wf.Config{
		StateDir:   stateDir,
		MaxWorkers: 3,
		Logger:     wf.NewWriterLogger(os.Stdout),
	})
	if err != nil {
		return err
	}
	defer eng.Close()

	effectsPath := filepath.Join(stateDir, "effects.log")
	crashMarker := filepath.Join(stateDir, "crashed.flag")

	// effectOnce appends one durable side-effect line per task (only after a
	// successful unit of work, so a crash beforehand leaves no partial effect).
	effectOnce := func(id string) error {
		data, _ := os.ReadFile(effectsPath)
		if strings.Contains(string(data), id+"\n") {
			return nil
		}
		f, err := os.OpenFile(effectsPath, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
		if err != nil {
			return err
		}
		defer f.Close()
		_, err = fmt.Fprintf(f, "%s\n", id)
		return err
	}

	fns := map[string]wf.TaskFunc{
		"prepare": func(wf.TaskContext) error { return effectOnce("prepare") },
		"fetch-a": func(wf.TaskContext) error { return effectOnce("fetch-a") },
		"fetch-b": func(wf.TaskContext) error { return effectOnce("fetch-b") },
		"enrich": func(tc wf.TaskContext) error {
			if tc.Attempt() < 2 {
				return wf.RetryableError("simulated transient failure on attempt %d", tc.Attempt()+1)
			}
			return effectOnce("enrich")
		},
		"combine": func(wf.TaskContext) error {
			// Simulate a hard process crash during the FIRST ever execution.
			if _, err := os.Stat(crashMarker); os.IsNotExist(err) {
				if e := os.WriteFile(crashMarker, []byte("crashed during combine\n"), 0o644); e != nil {
					return e
				}
				fmt.Println(">>> SIMULATED CRASH: process exiting with code 42 during task 'combine'")
				os.Exit(42)
			}
			return effectOnce("combine")
		},
		"report": func(wf.TaskContext) error { return effectOnce("report") },
	}
	eng.Register(w, fns)

	res, err := eng.Run(context.Background(), "demo-run", w)
	if err != nil {
		return err
	}
	fmt.Printf("\n=== child finished: run=%s ===\n", res.Status)
	for _, t := range workflowDefs() {
		st := res.Tasks[t.ID]
		fmt.Printf("    %-8s %-10s attempts=%d %s\n", t.ID, st.Status, st.Attempts, st.Error)
	}
	if res.Status != wf.RunSucceeded {
		os.Exit(1)
	}
	return nil
}

func runParent(stateDir string) error {
	exe, err := os.Executable()
	if err != nil {
		return err
	}
	const maxRestarts = 3
	for attempt := 1; attempt <= maxRestarts; attempt++ {
		fmt.Printf("\n########## PROCESS LIFETIME #%d ##########\n", attempt)
		cmd := exec.Command(exe, "--child", "--state-dir", stateDir)
		cmd.Stdout = os.Stdout
		cmd.Stderr = os.Stderr
		err := cmd.Run()

		if err == nil {
			return showSideEffects(stateDir)
		}
		if exitErr, ok := err.(*exec.ExitError); ok && exitErr.ExitCode() == 42 {
			fmt.Printf(">>> parent: child crashed (42); restarting from journal in %s ...\n", stateDir)
			continue
		}
		return err
	}
	return fmt.Errorf("workflow did not succeed after %d process lifetimes", maxRestarts)
}

func showSideEffects(stateDir string) error {
	data, err := os.ReadFile(filepath.Join(stateDir, "effects.log"))
	if err != nil {
		return err
	}
	fmt.Println("\n=== durable side effects (<state-dir>/effects.log) ===")
	lines := strings.Split(strings.TrimSpace(string(data)), "\n")
	counts := map[string]int{}
	for _, l := range lines {
		counts[l]++
		fmt.Println("   ", l)
	}
	fmt.Println("=== per-task execution count (every task must be 1) ===")
	for _, t := range workflowDefs() {
		fmt.Printf("    %-8s %d\n", t.ID, counts[t.ID])
	}
	// One extra, idempotent replay to prove a finished run does nothing.
	fmt.Println("\n=== extra invocation against the finished run (must execute nothing) ===")
	exe, _ := os.Executable()
	cmd := exec.Command(exe, "--child", "--state-dir", stateDir)
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	return cmd.Run()
}
