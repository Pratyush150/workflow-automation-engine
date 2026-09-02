"""Injectable clocks.

Every timing decision in flowforge -- backoff sleeps, circuit-breaker reset
windows, run deadlines, task durations -- goes through a :class:`Clock`. Tests
inject :class:`ManualClock`, so a policy that would wait 90 seconds in
production is exercised in microseconds with the exact same code path.
"""

from __future__ import annotations

import threading
import time
from datetime import datetime, timezone
from typing import List, Protocol, runtime_checkable


@runtime_checkable
class Clock(Protocol):
    """Time source. ``monotonic`` for durations, ``now`` for timestamps."""

    def monotonic(self) -> float:
        """Seconds from an arbitrary origin. Never goes backwards."""

    def now(self) -> datetime:
        """Current timezone-aware UTC wall time."""

    def sleep(self, seconds: float) -> None:
        """Block for ``seconds``."""


class SystemClock:
    """The real clock."""

    __slots__ = ()

    def monotonic(self) -> float:
        return time.monotonic()

    def now(self) -> datetime:
        return datetime.now(timezone.utc)

    def sleep(self, seconds: float) -> None:
        if seconds > 0:
            time.sleep(seconds)


class ManualClock:
    """A clock that only moves when you move it.

    ``sleep`` advances virtual time instead of blocking and records the request,
    so a test can assert on the exact backoff sequence a policy produced.
    """

    def __init__(self, start: float = 0.0, wall: datetime | None = None) -> None:
        self._t = float(start)
        self._wall = wall or datetime(2026, 1, 1, tzinfo=timezone.utc)
        self.sleeps: List[float] = []
        self._lock = threading.Lock()

    def monotonic(self) -> float:
        with self._lock:
            return self._t

    def now(self) -> datetime:
        from datetime import timedelta

        with self._lock:
            return self._wall + timedelta(seconds=self._t)

    def sleep(self, seconds: float) -> None:
        if seconds <= 0:
            return
        with self._lock:
            self.sleeps.append(float(seconds))
            self._t += float(seconds)

    def advance(self, seconds: float) -> None:
        """Move virtual time forward without recording a sleep."""
        with self._lock:
            self._t += float(seconds)


SYSTEM_CLOCK = SystemClock()
