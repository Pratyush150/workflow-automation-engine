"""flowforge -- a workflow engine built around what breaks in production.

Not a DAG picture with a scheduler bolted on. The parts that get attention here
are the ones that actually page you: retries that know what is worth retrying,
idempotency so a re-run does not re-send, partial-failure semantics that tell
``failed`` apart from ``never got the chance``, durable state you can resume
from, and enough observability to answer "what happened last night" from a
file.

Stdlib only. Everything below runs offline.

    from flowforge import Workflow, Executor, ExponentialBackoff

    wf = Workflow("nightly")

    @wf.task("extract", retry=ExponentialBackoff(max_attempts=3))
    def extract(ctx):
        return [{"id": 1, "amount": 10}]

    @wf.task("report", depends_on=["extract"])
    def report(ctx):
        rows = ctx.result("extract", expect=list)
        return {"rows": len(rows)}

    state = Executor(wf).run()
    print(state.status.value, state.tasks["report"].output_digest)
"""

from .clock import Clock, ManualClock, SystemClock
from .dag import Dag
from .errors import (
    Cancelled,
    CircuitOpen,
    ConnectorError,
    CycleError,
    DeadlineExceeded,
    FlowForgeError,
    MissingDependency,
    PermanentError,
    RetryExhausted,
    TaskTimeout,
    TransientError,
    UnknownTaskError,
    ValidationError,
)
from .executor import ExecutionOptions, Executor, TaskOutcome
from .idempotency import (
    IdempotencyGuard,
    IdempotencyStore,
    JsonFileStore,
    MemoryStore,
    content_key,
)
from .observability import (
    RunLogger,
    RunMetrics,
    explain_failure,
    metrics,
    render_gantt,
    summarise,
    timeline,
)
from .retry import (
    NO_RETRY,
    CircuitBreaker,
    ExponentialBackoff,
    FixedDelay,
    JitteredExponentialBackoff,
    RetryPolicy,
)
from .schedule import CronSchedule, IntervalSchedule, OneShot, next_runs
from .state import ResultArchive, RunState, RunStatus, StateStore, TaskRecord, TaskStatus
from .task import CancelToken, OnFailure, Step, Task, TaskContext, task
from .workflow import Workflow

__version__ = "0.1.0"

__all__ = [
    "CancelToken",
    "Cancelled",
    "CircuitBreaker",
    "CircuitOpen",
    "Clock",
    "ConnectorError",
    "CronSchedule",
    "CycleError",
    "Dag",
    "DeadlineExceeded",
    "ExecutionOptions",
    "Executor",
    "ExponentialBackoff",
    "FixedDelay",
    "FlowForgeError",
    "IdempotencyGuard",
    "IdempotencyStore",
    "IntervalSchedule",
    "JitteredExponentialBackoff",
    "JsonFileStore",
    "ManualClock",
    "MemoryStore",
    "MissingDependency",
    "NO_RETRY",
    "OnFailure",
    "OneShot",
    "PermanentError",
    "ResultArchive",
    "RetryExhausted",
    "RetryPolicy",
    "RunLogger",
    "RunMetrics",
    "RunState",
    "RunStatus",
    "StateStore",
    "Step",
    "SystemClock",
    "Task",
    "TaskContext",
    "TaskOutcome",
    "TaskRecord",
    "TaskStatus",
    "TaskTimeout",
    "TransientError",
    "UnknownTaskError",
    "ValidationError",
    "Workflow",
    "__version__",
    "content_key",
    "explain_failure",
    "metrics",
    "next_runs",
    "render_gantt",
    "summarise",
    "task",
    "timeline",
]
