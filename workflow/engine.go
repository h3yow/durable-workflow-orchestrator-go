package workflow

import (
	"context"
	"fmt"
	"os"
	"sync"
	"time"
)

// Config controls engine-wide scheduling behavior.
type Config struct {
	StateDir   string   // local directory for journals
	MaxWorkers int      // concurrency cap (>=1)
	RunTimeout Duration // whole-run timeout; 0 = none
	Logger     Logger
}

// Engine executes registered workflows with durable recovery.
type Engine struct {
	cfg  Config
	log  Logger
	lock *dirLock

	mu  sync.Mutex
	reg map[string]map[string]TaskFunc // workflow name -> task id -> fn
}

// NewEngine constructs an engine and acquires an exclusive lock on StateDir.
func NewEngine(cfg Config) (*Engine, error) {
	if cfg.MaxWorkers < 1 {
		cfg.MaxWorkers = 1
	}
	logger := cfg.Logger
	if logger == nil {
		logger = discardLogger{}
	}
	if cfg.StateDir == "" {
		return nil, fmt.Errorf("StateDir must not be empty")
	}
	lk, err := acquireDirLock(cfg.StateDir)
	if err != nil {
		return nil, err
	}
	return &Engine{
		cfg:  cfg,
		log:  logger,
		lock: lk,
		reg:  make(map[string]map[string]TaskFunc),
	}, nil
}

// Close releases the state-directory lock.
func (e *Engine) Close() error {
	if e.lock != nil {
		return e.lock.release()
	}
	return nil
}

// Register binds task ids to executable functions. Every task declared by wf
// must have a function.
func (e *Engine) Register(wf *Workflow, fns map[string]TaskFunc) {
	e.mu.Lock()
	defer e.mu.Unlock()
	bound := make(map[string]TaskFunc, len(wf.Tasks))
	for id := range wf.Tasks {
		fn, ok := fns[id]
		if !ok {
			panic("workflow.Register: no function registered for task " + id)
		}
		bound[id] = fn
	}
	e.reg[wf.Name] = bound
}

// Run starts (or resumes) the workflow with the given run id and blocks
// until it reaches a terminal state.
func (e *Engine) Run(ctx context.Context, runID string, wf *Workflow) (*RunResult, error) {
	if runID == "" {
		return nil, fmt.Errorf("runID must not be empty")
	}
	e.mu.Lock()
	fns := e.reg[wf.Name]
	e.mu.Unlock()
	if fns == nil {
		return nil, fmt.Errorf("workflow %q is not registered", wf.Name)
	}
	for id := range wf.Tasks {
		if fns[id] == nil {
			return nil, fmt.Errorf("task %q has no registered function", id)
		}
	}

	j, err := openJournal(e.cfg.StateDir, runID)
	if err != nil {
		return nil, err
	}
	defer j.Close()

	// A zero-byte file means a brand-new run; anything else must replay.
	jp := journalPath(e.cfg.StateDir, runID)
	fresh := false
	if fi, statErr := os.Stat(jp); statErr == nil && fi.Size() == 0 {
		fresh = true
	}

	defHash, err := DefinitionHash(wf)
	if err != nil {
		return nil, err
	}

	if fresh {
		e.log.Printf("RUN_STARTED input: run=%s workflow=%s tasks=%d defhash=%s workers=%d",
			runID, wf.Name, len(wf.order), defHash[:12], e.cfg.MaxWorkers)
		if err := j.Append(Event{
			Type:    EvRunStarted,
			RunID:   runID,
			DefHash: defHash,
			TS:      time.Now(),
		}); err != nil {
			return nil, err
		}
	}

	_, states, persistedRunStatus, err := j.replay(wf)
	if err != nil {
		return nil, err
	}

	startTime := time.Now()

	if !fresh {
		e.log.Printf("RESUME run=%s workflow=%s defhash=%s recovered tasks:", runID, wf.Name, defHash[:12])
		for _, id := range wf.order {
			e.log.Printf("  RECOVERED task=%s status=%s attempts=%d", id, states[id].Status, states[id].Attempts)
		}
	}

	// Already finished in a previous process: return the durable verdict
	// without executing anything (no duplicate side effects).
	switch persistedRunStatus {
	case RunSucceeded, RunFailed, RunCanceled:
		e.log.Printf("RUN_ALREADY_FINISHED run=%s status=%s -> no tasks executed", runID, persistedRunStatus)
		return e.buildResult(runID, states, persistedRunStatus, startTime, time.Now()), nil
	}

	// Run-level timeout wraps the caller context.
	runCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	if e.cfg.RunTimeout > 0 {
		var cancelTimeout context.CancelFunc
		runCtx, cancelTimeout = context.WithTimeout(ctx, e.cfg.RunTimeout.std())
		defer cancelTimeout()
	}

	return e.schedule(runCtx, runID, wf, fns, states, j, startTime)
}

// buildResult snapshots states into a RunResult.
func (e *Engine) buildResult(runID string, states map[string]*TaskState, status RunStatus, start, end time.Time) *RunResult {
	snap := make(map[string]*TaskState, len(states))
	for id, st := range states {
		cp := *st
		snap[id] = &cp
	}
	return &RunResult{RunID: runID, Status: status, Tasks: snap, StartedAt: start, EndedAt: end}
}

// taskExec bundles mutable per-run scheduling state.
type taskExec struct {
	def    *TaskDef
	fn     TaskFunc
	status TaskStatus
	// remainingDeps counts predecessors not yet SUCCEEDED. A task becomes
	// ready only when this reaches 0 while it is still non-terminal.
	remainingDeps int
	attempts      int // attempts started (persisted before each attempt)
}

func (e *Engine) schedule(
	ctx context.Context,
	runID string,
	wf *Workflow,
	fns map[string]TaskFunc,
	recovered map[string]*TaskState,
	j *Journal,
	startTime time.Time,
) (*RunResult, error) {

	exec := make(map[string]*taskExec, len(wf.order))
	rank := make(map[string]int, len(wf.order))
	for i, id := range wf.order {
		rank[id] = i
		d := wf.Tasks[id]
		exec[id] = &taskExec{
			def:           d,
			fn:            fns[id],
			status:        StatusPending,
			remainingDeps: len(d.Deps),
			attempts:      0,
		}
	}

	var (
		mu        sync.Mutex
		ready     []string // kept sorted ascending by rank (deterministic dispatch)
		inflight  int
		remaining = len(wf.order)
		wakeCh    = make(chan struct{})
	)
	// wakeLocked closes the current signal channel and installs a fresh one.
	// Caller must hold mu.
	wakeLocked := func() {
		close(wakeCh)
		wakeCh = make(chan struct{})
	}

	// appendLocked writes a journal record under the scheduler lock so the
	// persisted order matches the scheduling order.
	appendLocked := func(ev Event) error {
		ev.RunID = runID
		if ev.TS.IsZero() {
			ev.TS = time.Now()
		}
		return j.Append(ev)
	}

	// cascadeLocked walks descendants of a non-successful task and marks
	// them SKIPPED (downstream of failure) or CANCELED (run canceled).
	// Caller must hold mu.
	var cascadeLocked func(rootID string, canceled bool, reason string)
	cascadeLocked = func(rootID string, canceled bool, reason string) {
		queue := []string{rootID}
		seen := map[string]bool{rootID: true}
		for len(queue) > 0 {
			cur := queue[0]
			queue = queue[1:]
			curCanceled := canceled && exec[cur].status == StatusCanceled
			for _, id := range wf.order {
				t := exec[id]
				if isTerminal(t.status) || seen[id] {
					continue
				}
				if !contains(wf.Tasks[id].Deps, cur) {
					continue
				}
				seen[id] = true
				status := StatusSkipped
				msg := "downstream of " + cur + " (" + string(exec[cur].status) + "): dependency can never succeed"
				if curCanceled {
					status = StatusCanceled
					msg = "run canceled: upstream " + cur + " did not finish"
				}
				t.status = status
				remaining--
				ev := Event{Reason: reason, Error: msg}
				if status == StatusCanceled {
					ev.Reason = msg
				}
				if err := appendLocked(Event{
					Type:   eventTypeFor(status),
					TaskID: id,
					Reason: ev.Reason,
					Error:  ev.Error,
				}); err != nil {
					e.log.Printf("JOURNAL_WRITE_ERROR task=%s: %v", id, err)
					return
				}
				mirrorRecovered(recovered[id], status, t.attempts, ev)
				e.log.Printf("DECISION task=%s -> %s (cascade from %s)", id, status, cur)
				queue = append(queue, id)
			}
		}
	}

	// terminalLocked records one task's final outcome, persists it and
	// cascades. Caller must hold mu.
	terminalLocked := func(id string, status TaskStatus, ev Event) error {
		t := exec[id]
		if isTerminal(t.status) {
			return nil
		}
		t.status = status
		remaining--
		ev.Type = eventTypeFor(status)
		ev.TaskID = id
		if err := appendLocked(ev); err != nil {
			return err
		}
		mirrorRecovered(recovered[id], status, t.attempts, ev)
		e.log.Printf("DECISION task=%s -> %s (%s)", id, status, terminalReason(status, ev))
		if status == StatusSucceeded {
			// Release dependents only AFTER the SUCCEEDED record is durable,
			// so downstream STARTED events never precede this verdict.
			for _, tid := range wf.order {
				dep := exec[tid]
				if isTerminal(dep.status) {
					continue
				}
				if contains(wf.Tasks[tid].Deps, id) {
					dep.remainingDeps--
					if dep.remainingDeps == 0 {
						pushReady(&ready, tid, rank)
					}
				}
			}
		} else {
			cascadeLocked(id, status == StatusCanceled, evReason(status, ev))
		}
		if remaining == 0 {
			wakeLocked()
		}
		return nil
	}

	// Re-seed mutable scheduling state from the durable journal.
	for _, id := range wf.order {
		t := exec[id]
		st := recovered[id]
		t.attempts = st.Attempts
		switch st.Status {
		case StatusSucceeded, StatusFailed, StatusSkipped, StatusCanceled:
			t.status = st.Status
			remaining--
		case StatusRunning:
			// An attempt was in flight when the process died. It never
			// recorded a terminal event, so it must be executed again
			// (at-least-once; completed tasks are never re-run).
			e.log.Printf("REPLAN task=%s attempts=%d (interrupted by crash) -> re-execute", id, st.Attempts)
		}
	}
	// Recompute dependency counters from durable verdicts: a predecessor
	// already SUCCEEDED must count as satisfied; otherwise resumed tasks
	// would never become ready.
	for _, id := range wf.order {
		t := exec[id]
		if isTerminal(t.status) {
			continue
		}
		rem := 0
		for _, dep := range wf.Tasks[id].Deps {
			if exec[dep].status != StatusSucceeded {
				rem++
			}
		}
		t.remainingDeps = rem
	}

	sem := make(chan struct{}, e.cfg.MaxWorkers)
	var wg sync.WaitGroup

	// launch runs one task in a worker slot.
	launch := func(id string) {
		wg.Add(1)
		sem <- struct{}{}
		go func() {
			defer wg.Done()
			defer func() { <-sem }()
			e.runTask(ctx, id, exec[id],
				// persist one record outside scheduling hot path (locked)
				func(ev Event) error {
					mu.Lock()
					defer mu.Unlock()
					return appendLocked(ev)
				},
				// terminal verdict (dependents are released inside terminalLocked)
				func(status TaskStatus, ev Event) {
					mu.Lock()
					defer mu.Unlock()
					inflight--
					if err := terminalLocked(id, status, ev); err != nil {
						e.log.Printf("JOURNAL_WRITE_ERROR task=%s: %v", id, err)
					}
					wakeLocked()
				},
			)
		}()
	}

	// Main scheduling loop (single goroutine owns dispatch decisions).
	schedulerDone := make(chan struct{})
	go func() {
		defer close(schedulerDone)

		// 1. Cascade verdicts from recovered non-success terminals so their
		//    descendants are deterministically SKIPPED/CANCELED on resume.
		mu.Lock()
		for _, id := range wf.order {
			s := exec[id].status
			if s == StatusFailed || s == StatusSkipped || s == StatusCanceled {
				cascadeLocked(id, s == StatusCanceled, "recovered terminal: "+string(s))
			}
		}
		// 2. Seed the ready queue from current dependency counters.
		for _, id := range wf.order {
			t := exec[id]
			if !isTerminal(t.status) && t.remainingDeps == 0 {
				pushReady(&ready, id, rank)
			}
		}
		mu.Unlock()

		for {
			mu.Lock()
			if remaining == 0 {
				wakeLocked()
				mu.Unlock()
				return
			}
			if ctx.Err() != nil {
				// Run canceled/timed out: cascade to every non-terminal task.
				// Running tasks terminate themselves via ctx and call back;
				// waiting tasks are marked CANCELED here.
				for _, id := range wf.order {
					t := exec[id]
					if !isTerminal(t.status) && t.status != StatusRunning {
						_ = terminalLocked(id, StatusCanceled, Event{
							Reason: ctx.Err().Error(),
							Error:  "run canceled or timed out before execution",
						})
					}
				}
				if inflight == 0 {
					// Nothing still running: the next iteration observes
					// remaining == 0 and exits; do not park on a signal that
					// would never come.
					mu.Unlock()
					continue
				}
				ch := wakeCh
				mu.Unlock()
				// Running tasks observe ctx themselves and call back through
				// onTerminal (which wakes us); park until then, no busy loop.
				<-ch
				continue
			}
			if len(ready) > 0 {
				id := popReady(&ready)
				t := exec[id]
				if isTerminal(t.status) {
					mu.Unlock()
					continue
				}
				t.status = StatusRunning
				attempt := t.attempts
				t.attempts++
				// Persist STARTED at the decision instant, under the lock:
				// the event order then matches the deterministic dispatch
				// order, and a crash before the verdict leaves exactly this
				// task in RUNNING for re-execution.
				if err := appendLocked(Event{
					Type:     EvTaskStarted,
					TaskID:   id,
					Attempt:  attempt,
					Attempts: t.attempts,
				}); err != nil {
					mu.Unlock()
					e.log.Printf("JOURNAL_WRITE_ERROR task=%s: %v", id, err)
					return
				}
				recovered[id].Status = StatusRunning
				recovered[id].Attempts = t.attempts
				inflight++
				e.log.Printf("DECISION task=%s -> DISPATCH (attempt=%d/%d workers=%d)",
					id, attempt+1, maxAttemptsOf(t), e.cfg.MaxWorkers)
				mu.Unlock()
				launch(id)
				continue
			}
			if inflight == 0 {
				// Valid DAG invariant: with tasks remaining, no ready task and
				// nothing running, something is structurally wrong. Never
				// deadlock: surface as CANCELED with an explicit reason.
				e.log.Printf("SCHEDULER_STALL remaining=%d (no ready, none running)", remaining)
				for _, id := range wf.order {
					t := exec[id]
					if !isTerminal(t.status) {
						_ = terminalLocked(id, StatusCanceled, Event{
							Reason: "scheduler stall (internal invariant violated)",
						})
					}
				}
				wakeLocked()
				mu.Unlock()
				continue
			}
			ch := wakeCh
			mu.Unlock()
			select {
			case <-ch: // state changed (success unblocked dependents, etc.)
			case <-ctx.Done(): // run canceled/timed out
			}
		}
	}()

	// Wait for every task to reach a terminal state.
	select {
	case <-allDone(ctx, func() bool {
		mu.Lock()
		defer mu.Unlock()
		return remaining == 0
	}):
	case <-ctx.Done():
	}
	// Poll briefly for the cancel cascade to finish persisting.
	deadline := time.Now().Add(2 * time.Second)
	for {
		mu.Lock()
		done := remaining == 0
		mu.Unlock()
		if done || time.Now().After(deadline) {
			break
		}
		time.Sleep(time.Millisecond)
	}
	wg.Wait()
	<-schedulerDone

	endTime := time.Now()

	mu.Lock()
	finalStatus := RunSucceeded
	hasFailed := false
	hasCanceled := false
	for _, id := range wf.order {
		switch exec[id].status {
		case StatusFailed, StatusSkipped:
			hasFailed = true
		case StatusCanceled:
			hasCanceled = true
		}
	}
	switch {
	case hasFailed:
		finalStatus = RunFailed
	case hasCanceled || ctx.Err() != nil:
		finalStatus = RunCanceled
	default:
		finalStatus = RunSucceeded
	}
	if err := appendLocked(Event{Type: EvRunFinished, Status: finalStatus}); err != nil {
		mu.Unlock()
		return nil, err
	}
	mu.Unlock()

	e.log.Printf("RUN_FINISHED run=%s status=%s", runID, finalStatus)
	for _, id := range wf.order {
		t := exec[id]
		e.log.Printf("FINAL task=%s status=%s attempts=%d", id, t.status, t.attempts)
	}

	// Rebuild result states from the journal for a trustworthy snapshot.
	_, finalStates, _, err := j.replay(wf)
	if err != nil {
		return nil, err
	}
	return e.buildResult(runID, finalStates, finalStatus, startTime, endTime), nil
}

// allDone polls cond until it reports true or ctx ends; it returns a channel
// closed in either case.
func allDone(ctx context.Context, cond func() bool) chan struct{} {
	out := make(chan struct{})
	go func() {
		defer close(out)
		t := time.NewTicker(time.Millisecond)
		defer t.Stop()
		for {
			if cond() {
				return
			}
			select {
			case <-ctx.Done():
				return
			case <-t.C:
			}
		}
	}()
	return out
}

// runTask executes one task with per-attempt timeout and retry/backoff.
//   - onProgress persists a STARTED/attempt record
//   - onTerminal is called exactly once with the final verdict; the scheduler
//     releases dependents only after the SUCCEEDED verdict is durable
func (e *Engine) runTask(
	ctx context.Context,
	id string,
	t *taskExec,
	onProgress func(Event) error,
	onTerminal func(TaskStatus, Event),
) {
	def := t.def
	maxAttempts := def.Retry.MaxAttempts
	if maxAttempts <= 0 {
		maxAttempts = 1
	}

	canceledEvent := func(stage string) (TaskStatus, Event) {
		return StatusCanceled, Event{
			Reason:   ctx.Err().Error(),
			Error:    "run canceled or timed out " + stage,
			Attempts: t.attempts,
		}
	}

	for {
		if ctx.Err() != nil {
			onTerminal(canceledEvent("before attempt " + itoa(t.attempts+1)))
			return
		}

		// The current attempt's STARTED record is already durable: the
		// scheduler persisted attempt 1; retries below persist each next
		// STARTED before backing off.
		attempt := t.attempts - 1
		e.log.Printf("ATTEMPT_START task=%s attempt=%d/%d", id, attempt+1, maxAttempts)

		attemptCtx, cancelAttempt := context.WithCancel(ctx)
		if def.Timeout > 0 {
			attemptCtx, cancelAttempt = context.WithTimeout(ctx, def.Timeout.std())
		}
		tc := &taskContextImpl{Context: attemptCtx, taskID: id, attempt: attempt}

		errCh := make(chan error, 1)
		go func() {
			defer func() {
				if r := recover(); r != nil {
					errCh <- PermanentError("task panicked: %v", r)
				}
			}()
			errCh <- t.fn(tc)
		}()

		var taskErr error
		select {
		case taskErr = <-errCh:
		case <-attemptCtx.Done():
			cancelAttempt()
			if ctx.Err() != nil {
				onTerminal(canceledEvent("during attempt " + itoa(attempt+1)))
				return
			}
			taskErr = RetryableError("attempt %d timed out after %s", attempt+1, def.Timeout.std())
		}
		cancelAttempt()

		if taskErr == nil {
			e.log.Printf("ATTEMPT_OK task=%s attempt=%d", id, attempt+1)
			// Terminal verdict first (writes SUCCEEDED and releases
			// dependents); nothing depends on this task before that.
			onTerminal(StatusSucceeded, Event{Result: "ok", Attempts: t.attempts})
			return
		}

		retryable, msg := classifyError(taskErr)
		e.log.Printf("ATTEMPT_FAIL task=%s attempt=%d retryable=%v err=%q", id, attempt+1, retryable, msg)

		// Observability record for a failed (but possibly retried) attempt.
		_ = onProgress(Event{
			Type:      EvTaskProgress,
			TaskID:    id,
			Attempt:   attempt,
			Attempts:  t.attempts,
			Error:     msg,
			Retryable: retryable,
		})

		used := attempt + 1
		if !retryable || used >= maxAttempts {
			reason := "retries exhausted"
			if !retryable {
				reason = "permanent (non-retryable) failure"
			}
			e.log.Printf("RETRY_EXHAUSTED task=%s attempts=%d reason=%s", id, used, reason)
			onTerminal(StatusFailed, Event{
				Error:     msg,
				Reason:    reason,
				Retryable: retryable,
				Attempts:  t.attempts,
			})
			return
		}

		delay := backoffDelay(def.Retry.Backoff, used)
		e.log.Printf("RETRY_WAIT task=%s next_attempt=%d delay=%s", id, used+1, delay)

		// Record the next STARTED before waiting: if the process dies during
		// backoff, recovery re-runs this attempt from the durable counter.
		nextAttempt := t.attempts
		t.attempts++
		if err := onProgress(Event{
			Type:     EvTaskStarted,
			TaskID:   id,
			Attempt:  nextAttempt,
			Attempts: t.attempts,
		}); err != nil {
			onTerminal(StatusCanceled, Event{Reason: "journal write failed", Error: err.Error(), Attempts: t.attempts})
			return
		}
		if delay > 0 {
			timer := time.NewTimer(delay)
			select {
			case <-timer.C:
			case <-ctx.Done():
				timer.Stop()
				onTerminal(canceledEvent("during backoff before attempt " + itoa(used+1)))
				return
			}
		}
	}
}

func maxAttemptsOf(t *taskExec) int {
	m := t.def.Retry.MaxAttempts
	if m <= 0 {
		return 1
	}
	return m
}

// ---- small helpers ----

type taskContextImpl struct {
	context.Context
	taskID  string
	attempt int
}

func (t *taskContextImpl) TaskID() string { return t.taskID }
func (t *taskContextImpl) Attempt() int   { return t.attempt }

func isTerminal(s TaskStatus) bool {
	return s == StatusSucceeded || s == StatusFailed || s == StatusSkipped || s == StatusCanceled
}

func eventTypeFor(s TaskStatus) EventType {
	switch s {
	case StatusSucceeded:
		return EvTaskSucceeded
	case StatusFailed:
		return EvTaskFailed
	case StatusSkipped:
		return EvTaskSkipped
	case StatusCanceled:
		return EvTaskCanceled
	default:
		return ""
	}
}

func terminalReason(status TaskStatus, ev Event) string {
	switch status {
	case StatusSucceeded:
		return "attempt ok"
	case StatusFailed:
		return ev.Reason
	case StatusSkipped, StatusCanceled:
		if ev.Reason != "" {
			return ev.Reason
		}
		return ev.Error
	default:
		return ""
	}
}

func evReason(status TaskStatus, ev Event) string {
	if status == StatusSucceeded {
		return ""
	}
	return terminalReason(status, ev)
}

// mirrorRecovered updates the recovered state snapshot so callers see the
// verdict decided during this process lifetime.
func mirrorRecovered(st *TaskState, status TaskStatus, attempts int, ev Event) {
	now := timeNow(ev)
	st.Status = status
	st.Attempts = attempts
	st.Error = ev.Error
	st.Retryable = ev.Retryable
	if status == StatusSucceeded {
		st.Result = ev.Result
		st.Error = ""
	}
	st.UpdatedAt = &now
	st.FinishedAt = &now
}

func timeNow(ev Event) time.Time {
	if !ev.TS.IsZero() {
		return ev.TS
	}
	return time.Now()
}

func contains(list []string, v string) bool {
	for _, x := range list {
		if x == v {
			return true
		}
	}
	return false
}

// pushReady inserts id keeping the queue sorted ascending by rank so the
// earliest-declared runnable task is dispatched first (determinism).
func pushReady(q *[]string, id string, rank map[string]int) {
	*q = append(*q, id)
	for i := len(*q) - 1; i > 0; i-- {
		if rank[(*q)[i-1]] <= rank[(*q)[i]] {
			break
		}
		(*q)[i-1], (*q)[i] = (*q)[i], (*q)[i-1]
	}
}

func popReady(q *[]string) string {
	id := (*q)[0]
	*q = (*q)[1:]
	return id
}
