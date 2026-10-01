package workflow

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"
)

// errRunTimeout is the cancel cause used when the run-level timeout fires.
var errRunTimeout = errors.New("workflow run timeout elapsed")

// workerEvent carries one task attempt outcome back to the coordinator.
type workerEvent struct {
	taskID string
	output string
	err    error
}

// retryTimer is a scheduled retry.
type retryTimer struct {
	taskID string
	fireAt time.Time
	ch     <-chan time.Time
}

type coordinator struct {
	e         *Engine
	state     *RunState
	reg       Registry
	def       WorkflowDef
	policy    map[string]RetryPolicy
	deps      map[string][]string // task -> deps
	kick      chan struct{}
	events    chan workerEvent
	timers    map[string]retryTimer
	inflight  map[string]bool
	slots     chan struct{} // bounded concurrency
	runStatus Status
	runReason string
}

func (e *Engine) coordinate(parent context.Context, state *RunState, reg Registry) (*RunResult, error) {
	def := state.Definition
	c := &coordinator{
		e:         e,
		state:     state,
		reg:       reg,
		def:       def,
		policy:    make(map[string]RetryPolicy, len(def.Tasks)),
		deps:      make(map[string][]string),
		kick:      make(chan struct{}, 1),
		events:    make(chan workerEvent, def.Concurrency),
		timers:    make(map[string]retryTimer),
		inflight:  make(map[string]bool),
		slots:     make(chan struct{}, def.Concurrency),
		runStatus: StatusRunning,
	}
	for _, t := range def.Tasks {
		c.policy[t.ID] = t.Retry
		c.deps[t.ID] = t.Dependencies
	}

	// Cancellation with a cause so run-timeout and external cancel are
	// distinguishable terminal states.
	runCtx, cancelRun := context.WithCancelCause(parent)
	defer cancelRun(nil)
	stopParentWatch := context.AfterFunc(parent, func() {
		cause := parent.Err()
		if cause == nil {
			cause = context.Canceled
		}
		cancelRun(cause)
	})
	defer stopParentWatch()

	var runTimer *time.Timer
	if d := def.RunTimeout.Std(); d > 0 {
		runTimer = time.AfterFunc(d, func() {
			e.logf("[run=%s] RUN-TIMEOUT after %s; cascade cancel in-flight/waiting tasks", state.RunID, d)
			cancelRun(errRunTimeout)
			c.signal()
		})
	}
	defer func() {
		if runTimer != nil {
			runTimer.Stop()
		}
	}()

	var wg sync.WaitGroup
	c.signal() // prime the loop

	for {
		c.tick(runCtx, &wg)
		if c.allTerminal() {
			break
		}
		// Wait for the next state change: attempt result, retry timer fire,
		// or cancellation.
		var waitCh <-chan time.Time
		var waitID string
		var waitFire time.Time
		for _, rt := range c.timers {
			if waitCh == nil || rt.fireAt.Before(waitFire) {
				waitCh, waitID, waitFire = rt.ch, rt.taskID, rt.fireAt
			}
		}
		select {
		case <-c.kick:
		case ev := <-c.events:
			c.handleEvent(runCtx, ev)
		case <-waitCh:
			if rt, ok := c.timers[waitID]; ok && rt.ch == waitCh {
				t := c.state.Tasks[waitID]
				if t.Status == StatusPending && t.Attempts > 0 {
					t.Status = StatusReady
					c.persistTask(waitID)
				}
				delete(c.timers, waitID)
			}
		case <-runCtx.Done():
			// next tick performs the cascade
		}
	}
	wg.Wait()

	c.state.Status = c.runStatus
	c.state.Reason = c.runReason
	if err := c.e.store.UpdateRun(context.Background(), c.state); err != nil {
		return nil, fmt.Errorf("persist terminal run state: %w", err)
	}
	e.logf("[run=%s] FINAL status=%s reason=%q", state.RunID, c.runStatus, c.runReason)
	for _, td := range c.def.Tasks {
		t := c.state.Tasks[td.ID]
		e.logf("[run=%s] TERMINAL task=%s status=%s attempts=%d output=%q fail=%q",
			state.RunID, t.ID, t.Status, t.Attempts, t.Output, t.FailReason)
	}
	return buildResult(c.state), nil
}

func (c *coordinator) signal() {
	select {
	case c.kick <- struct{}{}:
	default:
	}
}

func (c *coordinator) tick(runCtx context.Context, wg *sync.WaitGroup) {
	if runCtx.Err() != nil {
		c.cascadeCancel(runCtx)
	}
	c.propagate()
	c.dispatch(runCtx, wg)
}

// cascadeCancel applies a run-level cancel/timeout to every task that is not
// already terminal. Succeeded/Failed/Skipped results are never modified.
func (c *coordinator) cascadeCancel(runCtx context.Context) {
	if c.runStatus == StatusRunning {
		if errors.Is(context.Cause(runCtx), errRunTimeout) {
			c.runStatus, c.runReason = StatusTimedOut, "workflow run timed out"
		} else {
			c.runStatus, c.runReason = StatusCanceled, "workflow run canceled"
		}
	}
	for _, id := range c.topo() {
		t := c.state.Tasks[id]
		if isTerminalTask(t.Status) {
			continue
		}
		t.Status = StatusCanceled
		t.FailReason = "canceled before execution: " + c.runReason
		c.persistTask(id)
		c.e.logf("[run=%s] CASCADE task=%s -> Canceled", c.state.RunID, id)
	}
	// Retry timers are moot now.
	for id := range c.timers {
		delete(c.timers, id)
	}
}

// propagate marks dependents of non-successful terminal tasks:
//   - downstream of Failed/Skipped -> Skipped ("upstream did not succeed")
//   - downstream of Canceled       -> Canceled
//
// It repeats until a fixpoint is reached (multi-hop fan-in).
func (c *coordinator) propagate() {
	changed := true
	for changed {
		changed = false
		for _, id := range c.topo() {
			t := c.state.Tasks[id]
			if t.Status != StatusPending && t.Status != StatusReady {
				continue
			}
			var failedUp, canceledUp string
			for _, d := range c.deps[id] {
				switch c.state.Tasks[d].Status {
				case StatusFailed, StatusSkipped:
					failedUp = d
				case StatusCanceled:
					if canceledUp == "" {
						canceledUp = d
					}
				}
			}
			if failedUp != "" {
				t.Status = StatusSkipped
				t.FailReason = fmt.Sprintf("skipped: upstream task %q did not succeed", failedUp)
				c.persistTask(id)
				c.e.logf("[run=%s] SKIP task=%s due to upstream=%s", c.state.RunID, id, failedUp)
				changed = true
			} else if canceledUp != "" {
				t.Status = StatusCanceled
				t.FailReason = fmt.Sprintf("canceled: upstream task %q was canceled", canceledUp)
				c.persistTask(id)
				c.e.logf("[run=%s] CASCADE task=%s -> Canceled due to upstream=%s", c.state.RunID, id, canceledUp)
				changed = true
			}
		}
	}
}

func (c *coordinator) dispatch(runCtx context.Context, wg *sync.WaitGroup) {
	for _, id := range c.topo() {
		t := c.state.Tasks[id]
		if t.Status != StatusReady && t.Status != StatusPending {
			continue
		}
		ready := true
		for _, d := range c.deps[id] {
			if c.state.Tasks[d].Status != StatusSucceeded {
				ready = false
				break
			}
		}
		if !ready || c.inflight[id] {
			continue
		}
		select {
		case c.slots <- struct{}{}:
		default:
			continue // concurrency limit reached; retry after the next event
		}
		c.inflight[id] = true
		t.Status = StatusRunning
		t.FailReason = ""
		c.persistTask(id)
		outputs := make(map[string]string, len(c.deps[id]))
		for _, d := range c.deps[id] {
			outputs[d] = c.state.Tasks[d].Output
		}
		attempt := t.Attempts + 1
		t.Attempts = attempt
		attemptKey := fmt.Sprintf("%s/%s/attempt-%d", c.state.RunID, id, attempt)
		c.e.logf("[run=%s] DISPATCH task=%s attempt=%d (in-flight=%d/%d)",
			c.state.RunID, id, attempt, len(c.inflight), c.def.Concurrency)
		wg.Add(1)
		go c.runTask(runCtx, id, attempt, attemptKey, outputs, wg)
	}
}

func (c *coordinator) runTask(runCtx context.Context, id string, attempt int, key string, outputs map[string]string, wg *sync.WaitGroup) {
	defer wg.Done()

	p := c.policy[id]
	exec := c.reg[id]
	var taskCtx context.Context
	var cancelTask context.CancelFunc
	if d := p.Timeout.Std(); d > 0 {
		taskCtx, cancelTask = context.WithTimeout(runCtx, d)
	} else {
		taskCtx, cancelTask = context.WithCancel(runCtx)
	}
	defer cancelTask()

	out, err := exec(taskCtx, TaskInput{
		RunID:      c.state.RunID,
		Attempt:    attempt,
		AttemptKey: key,
		Outputs:    outputs,
	})
	// Run-level cancel/timeout overrides whatever the executor returned.
	if runCtx.Err() != nil {
		err = context.Cause(runCtx)
	} else if errors.Is(taskCtx.Err(), context.DeadlineExceeded) {
		err = fmt.Errorf("%w: %v", ErrTaskTimeout, err)
	}
	c.events <- workerEvent{taskID: id, output: out, err: err}
}

func (c *coordinator) handleEvent(runCtx context.Context, ev workerEvent) {
	defer c.signal()
	delete(c.inflight, ev.taskID)
	<-c.slots // release the concurrency slot exactly when the result is consumed
	t := c.state.Tasks[ev.taskID]
	p := c.policy[ev.taskID]

	// Run-level timeout: task is canceled by the cascade, run is TimedOut.
	if errors.Is(ev.err, errRunTimeout) {
		t.Status = StatusCanceled
		t.FailReason = "canceled while running: run timeout elapsed"
		c.persistTask(ev.taskID)
		c.e.logf("[run=%s] RESULT task=%s -> Canceled (run timeout)", c.state.RunID, ev.taskID)
		return
	}
	// Run-level external cancellation always wins over the task's result.
	if errors.Is(ev.err, context.Canceled) || errors.Is(context.Cause(runCtx), context.Canceled) {
		t.Status = StatusCanceled
		t.FailReason = "canceled while running"
		c.persistTask(ev.taskID)
		c.e.logf("[run=%s] RESULT task=%s -> Canceled while running", c.state.RunID, ev.taskID)
		return
	}

	if ev.err == nil {
		t.Status = StatusSucceeded
		t.Output = ev.output
		t.FailReason = ""
		c.persistTask(ev.taskID)
		c.e.logf("[run=%s] RESULT task=%s -> Succeeded attempt=%d output=%q",
			c.state.RunID, ev.taskID, t.Attempts, ev.output)
		return
	}

	canRetry := false
	kind := "permanent"
	switch {
	case errors.Is(ev.err, ErrTaskTimeout):
		canRetry, kind = true, "timeout"
	case IsRetryable(ev.err):
		canRetry, kind = true, "retryable"
	default:
		canRetry, kind = false, "permanent"
	}
	if t.Attempts-1 >= p.MaxRetries { // attempts already done vs. extra retries
		canRetry = false
	}

	if canRetry {
		delay := nextBackoff(p, t.Attempts)
		t.Status = StatusPending
		t.FailReason = fmt.Sprintf("%s failure on attempt %d: %v (retry in %s)", kind, t.Attempts, ev.err, delay)
		c.persistTask(ev.taskID)
		c.e.logf("[run=%s] RETRY task=%s attempt=%d failed (%s): %v; backing off %s",
			c.state.RunID, ev.taskID, t.Attempts, kind, ev.err, delay)
		if delay <= 0 {
			t.Status = StatusReady
			c.persistTask(ev.taskID)
			return
		}
		ch := make(chan time.Time, 1)
		rt := retryTimer{taskID: ev.taskID, fireAt: c.e.clock.Now().Add(delay), ch: ch}
		go func() {
			if serr := c.e.clock.Sleep(runCtx, delay); serr != nil {
				return
			}
			ch <- c.e.clock.Now()
			c.signal()
		}()
		c.timers[ev.taskID] = rt
		return
	}

	t.Status = StatusFailed
	t.FailReason = fmt.Sprintf("%s failure after %d attempt(s): %v", kind, t.Attempts, ev.err)
	c.persistTask(ev.taskID)
	c.e.logf("[run=%s] RESULT task=%s -> Failed (%s, retries exhausted/not allowed): %v",
		c.state.RunID, ev.taskID, kind, ev.err)
}

func (c *coordinator) failRun(reason string, st Status) {
	c.runStatus, c.runReason = st, reason
	c.signal()
}

// persistTask writes a task record using a detached context so that run
// cancellation can never abort durability bookkeeping.
func (c *coordinator) persistTask(id string) {
	if err := c.e.store.PutTask(context.Background(), c.state.RunID, c.state.Tasks[id]); err != nil {
		c.failRun("persist task "+id+": "+err.Error(), StatusFailed)
	}
}

func (c *coordinator) allTerminal() bool {
	for _, t := range c.state.Tasks {
		if !isTerminalTask(t.Status) {
			return false
		}
	}
	if c.runStatus == StatusRunning {
		for _, t := range c.state.Tasks {
			if t.Status == StatusFailed {
				c.runStatus = StatusFailed
				c.runReason = "one or more tasks failed; their downstream tasks were skipped"
				return true
			}
		}
		c.runStatus = StatusSucceeded
		c.runReason = "all tasks reached a terminal state"
	}
	return true
}

func isTerminalTask(s Status) bool {
	switch s {
	case StatusSucceeded, StatusFailed, StatusCanceled, StatusSkipped:
		return true
	}
	return false
}

func (c *coordinator) topo() []string { return TopoOrder(c.def) }
