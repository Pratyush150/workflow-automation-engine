"""The engine: run a workflow's tasks in dependency order, and be honest about
what happened.

Sequential and thread-pool execution share one code path, so a workflow does
not behave differently because you set ``max_workers=4``.

The part worth reading is :meth:`Executor._propagate_blocked`. When a task
fails, its downstream tasks are marked ``upstream_failed`` -- not ``skipped``,
not silently dropped -- and every independent branch keeps running to
completion. Home-made automation usually gets this wrong in one of two ways:

* ``try/except: pass`` around each step, so the run is "green" and the report
  is empty; or
* abort the whole script on the first exception, so an unrelated branch that
  would have worked is left half-done and has to be untangled by hand.

Both are visible in the run state here: ``failed`` means this task raised,
``upstream_failed`` means it never got the chance, ``skipped`` means a policy
said to carry on without it. Three different incidents, three different
statuses, and :func:`~flowforge.observability.explain_failure` can tell you
which one you have.

Timeouts, honestly: Python cannot kill a thread. A task that exceeds its
timeout is marked ``timed_out``, its :class:`~flowforge.task.CancelToken` is
set, and the engine stops waiting for it. A cooperative task checks the token
and stops. A task blocked in an uninterruptible C call keeps running in the
background until it returns. That is a property of threads, not a bug here --
if you need a hard kill, run the step in a subprocess (see the ``shell``
connector, which does exactly that).
"""

from __future__ import annotations

import threading
import uuid
from concurrent.futures import FIRST_COMPLETED, Future, ThreadPoolExecutor, wait
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Set

from .clock import SYSTEM_CLOCK, Clock
from .errors import Cancelled, RetryExhausted, TaskTimeout
from .idempotency import IdempotencyGuard, digest
from .observability import RunLogger
from .retry import AttemptRecord, CircuitBreaker, retry_call
from .state import (
    ResultArchive,
    RunState,
    RunStatus,
    StateStore,
    TaskStatus,
)
from .task import CancelToken, MissingResult, OnFailure, Task, TaskContext
from .workflow import Workflow

__all__ = ["ExecutionOptions", "Executor", "TaskOutcome"]


@dataclass
class ExecutionOptions:
    """Knobs for one execution."""

    #: 1 = sequential. Anything higher uses a thread pool; the dependency graph
    #: still decides what may run at the same time.
    max_workers: int = 1
    #: Wall-clock budget for the whole run, seconds. Overrides the workflow's.
    deadline: Optional[float] = None
    #: Applied to tasks that do not set their own ``timeout``.
    default_timeout: Optional[float] = None
    #: Stop starting new work as soon as a task fails under ``on_failure=fail``.
    #: Off by default: independent branches finishing is usually more valuable
    #: than stopping half a second sooner.
    fail_fast: bool = False
    #: How often the engine wakes to check timeouts and deadlines.
    poll_interval: float = 0.02
    #: Persist state after every task transition. Turn off for a hot loop.
    persist_every_transition: bool = True


@dataclass
class TaskOutcome:
    """What one task did. Produced inside the worker thread, applied in the loop."""

    task_id: str
    status: TaskStatus
    value: Any = None
    error: Optional[BaseException] = None
    attempts: int = 0
    retry_reasons: List[str] = field(default_factory=list)
    started: float = 0.0
    ended: float = 0.0
    cached: bool = False
    idempotency_key: str = ""
    input_digest: str = ""

    @property
    def duration(self) -> float:
        return max(0.0, self.ended - self.started)


class Executor:
    """Runs a :class:`~flowforge.workflow.Workflow`."""

    def __init__(
        self,
        workflow: Workflow,
        *,
        options: Optional[ExecutionOptions] = None,
        clock: Clock = SYSTEM_CLOCK,
        logger: Optional[RunLogger] = None,
        state_store: Optional[StateStore] = None,
        idempotency: Optional[IdempotencyGuard] = None,
        breakers: Optional[Dict[str, CircuitBreaker]] = None,
    ) -> None:
        self.workflow = workflow
        self.options = options or ExecutionOptions()
        self.clock = clock
        self.logger = logger or RunLogger(enabled=False)
        self.state_store = state_store
        self.idempotency = idempotency
        self.breakers: Dict[str, CircuitBreaker] = dict(breakers or {})
        self._started: Dict[str, float] = {}
        self._tokens: Dict[str, CancelToken] = {}
        self._lock = threading.Lock()

    # ------------------------------------------------------------------ public

    def run(
        self,
        params: Optional[Dict[str, Any]] = None,
        *,
        run_id: Optional[str] = None,
        resume: Optional[RunState] = None,
    ) -> RunState:
        """Execute the workflow and return its :class:`RunState`.

        ``resume`` seeds the run from a previous state: tasks that already
        succeeded are inherited, their outputs are restored from the result
        archive, and only the rest is executed.
        """
        wf = self.workflow
        wf.validate()
        run_id = run_id or self._new_run_id()
        log = RunLogger(
            run_id,
            sink=self.logger._sink,  # noqa: SLF001 - deliberate: share the sink
            clock=self.clock,
            enabled=self.logger.enabled,
        )

        state = self._seed_state(run_id, params or {}, resume)
        results: Dict[str, Any] = {}
        archive = self._open_archive(state)
        if resume is not None:
            self._restore_results(resume, state, results, archive, log)

        self._started.clear()
        self._tokens = {task_id: CancelToken() for task_id in wf.ids}

        started_mono = self.clock.monotonic()
        state.started_at = self.clock.now().isoformat()
        state.status = RunStatus.RUNNING
        deadline_budget = self.options.deadline or wf.deadline
        deadline = started_mono + deadline_budget if deadline_budget else None

        pending: Set[str] = set()
        for task_id in wf.ids:
            record = state.record(task_id)
            record.tags = sorted(wf[task_id].tags)
            if record.status.is_success:
                continue
            record.status = TaskStatus.PENDING
            pending.add(task_id)

        log.info(
            "run.start",
            workflow=wf.name,
            tasks=len(wf),
            to_run=len(pending),
            inherited=len(wf) - len(pending),
            max_workers=self.options.max_workers,
            deadline=deadline_budget,
            resumed_from=state.resumed_from[-1] if state.resumed_from else None,
        )
        self._persist(state, always=True)

        cancelled_run = False
        futures: Dict[Future, str] = {}
        # Timed-out tasks whose thread is still running. We no longer wait for
        # them, but we do want to notice when they finish, because "the task you
        # gave up on completed 40 seconds later" is a tuning signal, not noise.
        abandoned: Dict[Future, str] = {}
        pool = ThreadPoolExecutor(
            max_workers=max(1, self.options.max_workers),
            thread_name_prefix=f"ff-{run_id}",
        )
        try:
            while True:
                self._propagate_blocked(state, pending, log)
                ready = [t for t in sorted(pending) if self._is_ready(state, t)]
                for task_id in ready:
                    pending.discard(task_id)
                    state.record(task_id).status = TaskStatus.RUNNING
                    futures[
                        pool.submit(
                            self._execute,
                            wf[task_id],
                            state,
                            dict(results),
                            deadline,
                            log,
                        )
                    ] = task_id

                if not futures:
                    if pending:
                        # Nothing running and nothing runnable: the remaining
                        # tasks are waiting on something that will never finish.
                        for task_id in sorted(pending):
                            rec = state.record(task_id)
                            rec.status = TaskStatus.CANCELLED
                            rec.error = "not reachable: no runnable upstream path"
                        pending.clear()
                    break

                done, _ = wait(
                    list(futures) + list(abandoned),
                    timeout=self._wait_timeout(futures, deadline),
                    return_when=FIRST_COMPLETED,
                )
                for future in done:
                    task_id = futures.pop(future, None) or abandoned.pop(future, None)
                    if task_id is None:  # pragma: no cover - defensive
                        continue
                    outcome = self._collect(future, task_id)
                    # _apply refuses to overwrite a task the loop already gave
                    # up on, so a late result is logged and discarded.
                    self._apply(state, outcome, results, archive, started_mono, log)

                self._enforce_timeouts(state, futures, abandoned, started_mono, log)

                if deadline is not None and self.clock.monotonic() >= deadline:
                    cancelled_run = True
                    state.error = (
                        f"run deadline of {deadline_budget:g}s exceeded"
                    )
                    log.error("run.deadline_exceeded", budget=deadline_budget)
                    self._abort(state, futures, pending, "run deadline exceeded")
                    break

                if self.options.fail_fast and self._fatal_failure(state):
                    log.warn("run.fail_fast", failed=state.failed)
                    self._abort(state, futures, pending, "fail_fast")
                    cancelled_run = False
                    break
        finally:
            pool.shutdown(wait=False, cancel_futures=True)

        state.duration = self.clock.monotonic() - started_mono
        state.ended_at = self.clock.now().isoformat()
        state.status = self._final_status(state, cancelled_run)
        log.info(
            "run.end",
            status=state.status.value,
            duration=round(state.duration, 4),
            succeeded=len(state.succeeded),
            failed=len(state.failed),
            retries=state.retry_count,
        )
        self._persist(state, always=True)
        return state

    def dry_run(self) -> List[List[str]]:
        """Levels the run would execute, without running anything."""
        return self.workflow.levels()

    # ------------------------------------------------------------- run helpers

    def _new_run_id(self) -> str:
        stamp = self.clock.now().strftime("%Y%m%dT%H%M%S")
        return f"{stamp}-{uuid.uuid4().hex[:6]}"

    def _seed_state(
        self, run_id: str, params: Dict[str, Any], resume: Optional[RunState]
    ) -> RunState:
        if resume is None:
            return RunState(run_id=run_id, workflow=self.workflow.name, params=dict(params))
        state = resume.as_resume_seed(run_id)
        state.params = {**resume.params, **params}
        return state

    def _open_archive(self, state: RunState) -> Optional[ResultArchive]:
        if self.state_store is None:
            return None
        return ResultArchive(self.state_store.archive_path(state))

    def _restore_results(
        self,
        resume: RunState,
        state: RunState,
        results: Dict[str, Any],
        archive: Optional[ResultArchive],
        log: RunLogger,
    ) -> None:
        """Bring forward the outputs of tasks that already succeeded."""
        source: Optional[ResultArchive] = None
        if self.state_store is not None:
            source = ResultArchive(self.state_store.archive_path(resume))
        for task_id in resume.completed_tasks():
            if source is not None and source.has(task_id):
                value = source.get(task_id)
                results[task_id] = value
                if archive is not None:
                    archive.put(task_id, value)
                continue
            results[task_id] = MissingResult(
                task_id,
                "its output was not persisted by the previous run "
                "(no state store, or the value was not JSON-serialisable)",
            )
            log.warn("resume.result_unavailable", task_id=task_id)

    def _persist(self, state: RunState, *, always: bool = False) -> None:
        if self.state_store is None:
            return
        if always or self.options.persist_every_transition:
            self.state_store.save(state)

    # ---------------------------------------------------------------- planning

    def _is_ready(self, state: RunState, task_id: str) -> bool:
        """True when every dependency has reached a state that lets us start."""
        for dep in self.workflow[task_id].depends_on:
            status = state.status_of(dep)
            if status.is_success:
                continue
            if (
                status.is_failure
                and self.workflow[dep].on_failure is OnFailure.CONTINUE
            ):
                continue
            return False
        return True

    def _propagate_blocked(
        self, state: RunState, pending: Set[str], log: RunLogger
    ) -> None:
        """Mark tasks that can never run, and say precisely why.

        Runs to a fixpoint so the blast radius of one failure is marked in a
        single pass, however deep the chain is.
        """
        changed = True
        while changed:
            changed = False
            for task_id in sorted(pending):
                blocker_failed: Optional[str] = None
                blocker_skipped: Optional[str] = None
                for dep in self.workflow[task_id].depends_on:
                    status = state.status_of(dep)
                    if status.is_failure:
                        policy = self.workflow[dep].on_failure
                        if policy is OnFailure.FAIL:
                            blocker_failed = blocker_failed or dep
                        elif policy is OnFailure.SKIP:
                            blocker_skipped = blocker_skipped or dep
                        # CONTINUE: downstream runs anyway.
                    elif status in (TaskStatus.UPSTREAM_FAILED, TaskStatus.CANCELLED):
                        blocker_failed = blocker_failed or dep
                    elif status is TaskStatus.SKIPPED:
                        blocker_skipped = blocker_skipped or dep
                if blocker_failed is None and blocker_skipped is None:
                    continue
                record = state.record(task_id)
                if blocker_failed is not None:
                    record.status = TaskStatus.UPSTREAM_FAILED
                    record.blocked_by = blocker_failed
                else:
                    record.status = TaskStatus.SKIPPED
                    record.blocked_by = blocker_skipped or ""
                pending.discard(task_id)
                changed = True
                log.warn(
                    "task.blocked",
                    task_id=task_id,
                    status=record.status.value,
                    blocked_by=record.blocked_by,
                )
        if changed:
            self._persist(state)

    def _wait_timeout(
        self, futures: Dict[Future, str], deadline: Optional[float]
    ) -> Optional[float]:
        """How long to block in ``wait`` before re-checking timeouts."""
        now = self.clock.monotonic()
        candidates: List[float] = []
        for task_id in futures.values():
            limit = self._timeout_for(task_id)
            if limit is None:
                continue
            with self._lock:
                started = self._started.get(task_id)
            candidates.append(
                (started + limit - now) if started else self.options.poll_interval
            )
        if deadline is not None:
            candidates.append(deadline - now)
        if not candidates:
            return None
        return max(0.0, min(candidates))

    def _timeout_for(self, task_id: str) -> Optional[float]:
        return self.workflow[task_id].timeout or self.options.default_timeout

    def _enforce_timeouts(
        self,
        state: RunState,
        futures: Dict[Future, str],
        abandoned: Dict[Future, str],
        run_started: float,
        log: RunLogger,
    ) -> None:
        """Abandon tasks that overran, and record them as ``timed_out``."""
        now = self.clock.monotonic()
        for future in list(futures):
            task_id = futures[future]
            limit = self._timeout_for(task_id)
            if limit is None:
                continue
            with self._lock:
                started = self._started.get(task_id)
            if started is None or (now - started) < limit:
                continue
            futures.pop(future, None)
            self._tokens[task_id].cancel(f"timeout after {limit:g}s")
            if not future.cancel():
                # Already running: Python cannot kill it, so we stop waiting and
                # keep a handle to log the fact if it ever finishes.
                abandoned[future] = task_id
            record = state.record(task_id)
            record.status = TaskStatus.TIMED_OUT
            record.attempts = max(record.attempts, 1)
            record.start_offset = started - run_started
            record.duration = now - started
            record.ended_at = self.clock.now().isoformat()
            record.error = f"exceeded timeout of {limit:g}s"
            record.error_type = TaskTimeout.__name__
            log.error(
                "task.timeout", task_id=task_id, timeout=limit, elapsed=now - started
            )
            self._persist(state)

    def _abort(
        self,
        state: RunState,
        futures: Dict[Future, str],
        pending: Set[str],
        reason: str,
    ) -> None:
        """Cancel everything still outstanding and record why."""
        for token in self._tokens.values():
            token.cancel(reason)
        for future, task_id in list(futures.items()):
            future.cancel()
            record = state.record(task_id)
            if not record.status.is_terminal:
                record.status = TaskStatus.CANCELLED
                record.error = reason
        futures.clear()
        for task_id in sorted(pending):
            record = state.record(task_id)
            record.status = TaskStatus.CANCELLED
            record.error = reason
        pending.clear()
        self._persist(state)

    def _fatal_failure(self, state: RunState) -> bool:
        for task_id, record in state.tasks.items():
            if record.status.is_failure and (
                self.workflow[task_id].on_failure is OnFailure.FAIL
            ):
                return True
        return False

    def _final_status(self, state: RunState, cancelled: bool) -> RunStatus:
        if cancelled:
            return RunStatus.CANCELLED
        if self._fatal_failure(state):
            return RunStatus.FAILED
        degraded = any(
            record.status
            in (
                TaskStatus.FAILED,
                TaskStatus.TIMED_OUT,
                TaskStatus.SKIPPED,
                TaskStatus.UPSTREAM_FAILED,
                TaskStatus.CANCELLED,
            )
            for record in state.tasks.values()
        )
        return RunStatus.DEGRADED if degraded else RunStatus.SUCCEEDED

    # --------------------------------------------------------------- execution

    def _breaker_for(self, task: Task) -> Optional[CircuitBreaker]:
        """Tasks opt into a shared circuit breaker with a ``circuit:<name>`` tag."""
        for tag in sorted(task.tags):
            if tag.startswith("circuit:"):
                name = tag.split(":", 1)[1]
                if name not in self.breakers:
                    self.breakers[name] = CircuitBreaker(name, clock=self.clock)
                return self.breakers[name]
        return None

    def _execute(
        self,
        task: Task,
        state: RunState,
        upstream: Dict[str, Any],
        deadline: Optional[float],
        log: RunLogger,
    ) -> TaskOutcome:
        """Run one task. Never raises: failures come back as an outcome.

        Runs in a worker thread. It touches no shared mutable state except
        ``_started`` (under a lock); everything else is returned to the loop.
        """
        started = self.clock.monotonic()
        with self._lock:
            self._started[task.id] = started

        # A task sees only its declared dependencies. Reaching sideways into a
        # task you did not declare is a race waiting to happen, so it is simply
        # not possible here.
        visible = {dep: upstream.get(dep) for dep in task.depends_on}
        input_digest = digest({"params": state.params, "deps": visible})
        token = self._tokens[task.id]
        task_log = log.bind(task_id=task.id)
        ctx = TaskContext(
            run_id=state.run_id,
            task_id=task.id,
            params=state.params,
            results=visible,
            deadline=deadline,
            cancel=token,
            log=task_log.event,
        )

        key = ""
        try:
            key = task.key_for(ctx) or ""
        except Exception as exc:  # noqa: BLE001 - a bad key is a task failure
            return TaskOutcome(
                task.id,
                TaskStatus.FAILED,
                error=exc,
                attempts=0,
                started=started,
                ended=self.clock.monotonic(),
                input_digest=input_digest,
            )

        if key and self.idempotency is not None:
            try:
                begin = self.idempotency.begin(
                    key, run_id=state.run_id, task_id=task.id
                )
            except Exception as exc:  # noqa: BLE001 - AmbiguousReplay et al
                return TaskOutcome(
                    task.id,
                    TaskStatus.FAILED,
                    error=exc,
                    attempts=0,
                    started=started,
                    ended=self.clock.monotonic(),
                    idempotency_key=key,
                    input_digest=input_digest,
                )
            if begin.cached:
                record = begin.record
                task_log.info("task.cached", idempotency_key=key)
                return TaskOutcome(
                    task.id,
                    TaskStatus.CACHED,
                    value=(record.value if record and record.value_available else None),
                    attempts=0,
                    started=started,
                    ended=self.clock.monotonic(),
                    cached=True,
                    idempotency_key=key,
                    input_digest=input_digest,
                )

        def attempt(number: int) -> Any:
            token.raise_if_cancelled()
            ctx.attempt = number
            task_log.info("task.attempt", attempt=number)
            return task(ctx)

        def on_attempt(record: AttemptRecord) -> None:
            if record.error is None:
                return
            task_log.warn(
                "task.attempt_failed",
                attempt=record.attempt,
                error_type=type(record.error).__name__,
                error=str(record.error),
                decision=record.reason,
                delay=record.delay,
            )

        task_log.info("task.start", attempts_allowed=task.retry.max_attempts)
        try:
            value, records = retry_call(
                attempt,
                task.retry,
                clock=self.clock,
                deadline=deadline,
                breaker=self._breaker_for(task),
                on_attempt=on_attempt,
            )
        except BaseException as exc:  # noqa: BLE001 - reported as an outcome
            underlying = exc.last_error if isinstance(exc, RetryExhausted) else exc
            attempts = exc.attempts if isinstance(exc, RetryExhausted) else 1
            if key and self.idempotency is not None:
                self.idempotency.fail(key, underlying, run_id=state.run_id)
            status = (
                TaskStatus.CANCELLED
                if isinstance(underlying, Cancelled)
                else TaskStatus.TIMED_OUT
                if isinstance(underlying, TaskTimeout)
                else TaskStatus.FAILED
            )
            ended = self.clock.monotonic()
            task_log.error(
                "task.failed",
                status=status.value,
                attempts=attempts,
                error_type=type(underlying).__name__,
                error=str(underlying),
                duration=round(ended - started, 4),
            )
            return TaskOutcome(
                task.id,
                status,
                error=underlying,
                attempts=attempts,
                started=started,
                ended=ended,
                idempotency_key=key,
                input_digest=input_digest,
            )

        if key and self.idempotency is not None:
            self.idempotency.complete(
                key, value, run_id=state.run_id, task_id=task.id
            )
        ended = self.clock.monotonic()
        task_log.info(
            "task.succeeded",
            attempts=len(records),
            duration=round(ended - started, 4),
        )
        return TaskOutcome(
            task.id,
            TaskStatus.SUCCEEDED,
            value=value,
            attempts=len(records),
            retry_reasons=[r.reason for r in records],
            started=started,
            ended=ended,
            idempotency_key=key,
            input_digest=input_digest,
        )

    def _collect(self, future: Future, task_id: str) -> TaskOutcome:
        """Turn a finished future into an outcome, even if the engine itself broke."""
        try:
            return future.result()
        except Exception as exc:  # pragma: no cover - engine bug path
            now = self.clock.monotonic()
            return TaskOutcome(
                task_id,
                TaskStatus.FAILED,
                error=exc,
                attempts=1,
                started=self._started.get(task_id, now),
                ended=now,
            )

    def _apply(
        self,
        state: RunState,
        outcome: TaskOutcome,
        results: Dict[str, Any],
        archive: Optional[ResultArchive],
        run_started: float,
        log: RunLogger,
    ) -> None:
        """Fold a task outcome into the run state. Main thread only."""
        record = state.record(outcome.task_id)
        if record.status is TaskStatus.TIMED_OUT:
            # The loop already gave up on this task; a late result does not
            # resurrect it. Recording it as succeeded here would mean the state
            # disagrees with what downstream tasks were told.
            log.warn("task.late_result_ignored", task_id=outcome.task_id)
            return
        record.status = outcome.status
        record.attempts = outcome.attempts
        record.start_offset = outcome.started - run_started
        record.duration = outcome.duration
        record.ended_at = self.clock.now().isoformat()
        record.started_at = record.started_at or state.started_at
        record.input_digest = outcome.input_digest
        record.idempotency_key = outcome.idempotency_key
        record.retry_reasons = outcome.retry_reasons
        if outcome.error is not None:
            record.error = str(outcome.error)
            record.error_type = type(outcome.error).__name__
        if outcome.status.is_success:
            record.output_digest = digest(outcome.value)
            results[outcome.task_id] = outcome.value
            if archive is not None:
                if not archive.put(outcome.task_id, outcome.value):
                    log.warn(
                        "task.output_not_persistable",
                        task_id=outcome.task_id,
                        type=type(outcome.value).__name__,
                    )
        self._persist(state)
