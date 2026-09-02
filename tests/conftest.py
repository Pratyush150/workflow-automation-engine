"""Shared fixtures.

Every test in this suite is offline and deterministic. Where time matters, a
:class:`flowforge.clock.ManualClock` is injected so backoff sequences are
asserted exactly rather than slept through. The only real waiting anywhere is
in the timeout tests, and it is capped at tens of milliseconds.
"""

from __future__ import annotations

import os
import sys
import pytest

REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
SRC = os.path.join(REPO_ROOT, "src")
if SRC not in sys.path:
    sys.path.insert(0, SRC)

from flowforge.clock import ManualClock  # noqa: E402
from flowforge.idempotency import IdempotencyGuard, MemoryStore  # noqa: E402
from flowforge.observability import ListSink, RunLogger  # noqa: E402
from flowforge.state import StateStore  # noqa: E402


@pytest.fixture()
def repo_root() -> str:
    return REPO_ROOT


@pytest.fixture()
def clock() -> ManualClock:
    return ManualClock()


@pytest.fixture()
def sink() -> ListSink:
    return ListSink()


@pytest.fixture()
def logger(sink: ListSink) -> RunLogger:
    return RunLogger("test-run", sink=sink, enabled=True)


@pytest.fixture()
def guard() -> IdempotencyGuard:
    return IdempotencyGuard(MemoryStore())


@pytest.fixture()
def store(tmp_path) -> StateStore:
    return StateStore(str(tmp_path / "runs"))
