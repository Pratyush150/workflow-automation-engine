"""Schedules: a hand-written cron parser, intervals, and one-shots.

Why implement cron rather than import a parser: the interesting part is not
parsing ``*/15``, it is what happens on the two nights a year when local time
jumps. A scheduler that gets that wrong runs the nightly billing job twice in
November and not at all in March, and nobody notices until the reconciliation.

So the rules here are explicit and tested:

* All matching happens in the schedule's own timezone (``zoneinfo``), not UTC
  and not "whatever the server is set to".
* **Spring forward.** A local time that does not exist (02:30 in a zone that
  jumps 02:00 to 03:00) fires once, at the first instant that does exist. It is
  not skipped and it is not run twice.
* **Fall back.** A local time that happens twice fires on the **first**
  occurrence only. The second pass over 01:30 is not a second run.
* ``next_run_after`` is strictly after the instant you give it, so feeding a
  fire time back in gives you the following one and cannot loop.

Supported syntax (standard 5-field ``minute hour day-of-month month day-of-week``):

===================  ==========================================================
``*``                every value
``*/n``              every nth value from the start of the range
``a-b``              inclusive range
``a-b/n``            inclusive range, every nth
``a,b,c``            list; elements may themselves be ranges or steps
``mon``, ``jan``     three-letter day and month names, any case
``0`` or ``7`` (dow) both mean Sunday
===================  ==========================================================

Day-of-month and day-of-week follow the traditional cron rule: if **both** are
restricted, a day matches when **either** matches. ``0 0 13 * fri`` is "the
13th, and every Friday", not "Friday the 13th". That surprises people, so it is
tested.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Dict, FrozenSet, List, Optional, Protocol, Tuple

from .errors import ValidationError

try:  # pragma: no cover - availability depends on the platform's tz database
    from zoneinfo import ZoneInfo, ZoneInfoNotFoundError
except ImportError:  # pragma: no cover - Python < 3.9
    ZoneInfo = None  # type: ignore[assignment]

    class ZoneInfoNotFoundError(Exception):  # type: ignore[no-redef]
        pass


__all__ = [
    "CronField",
    "CronSchedule",
    "IntervalSchedule",
    "OneShot",
    "Schedule",
    "next_runs",
    "parse_cron",
]

MONTH_NAMES: Dict[str, int] = {
    name: i + 1
    for i, name in enumerate(
        "jan feb mar apr may jun jul aug sep oct nov dec".split()
    )
}
#: cron day-of-week is Sunday-first.
DOW_NAMES: Dict[str, int] = {
    name: i for i, name in enumerate("sun mon tue wed thu fri sat".split())
}

MACROS: Dict[str, str] = {
    "@yearly": "0 0 1 1 *",
    "@annually": "0 0 1 1 *",
    "@monthly": "0 0 1 * *",
    "@weekly": "0 0 * * 0",
    "@daily": "0 0 * * *",
    "@midnight": "0 0 * * *",
    "@hourly": "0 * * * *",
}

# Bound the search so a pathological expression (29 February in a non-leap
# century, say) fails loudly instead of spinning.
_MAX_DAY_STEPS = 366 * 8


class Schedule(Protocol):
    """Anything that can say when it next fires."""

    def next_run_after(self, after: datetime) -> Optional[datetime]: ...


def _tzinfo(name_or_tz: object):
    """Resolve a timezone name to a tzinfo, with a useful error."""
    if name_or_tz is None:
        return timezone.utc
    if isinstance(name_or_tz, str):
        if name_or_tz.upper() == "UTC":
            return timezone.utc
        if ZoneInfo is None:  # pragma: no cover
            raise ValidationError(
                "zoneinfo is unavailable; use 'UTC' or a fixed offset",
                key="timezone",
            )
        try:
            return ZoneInfo(name_or_tz)
        except (ZoneInfoNotFoundError, KeyError, ValueError) as exc:
            raise ValidationError(
                f"unknown timezone {name_or_tz!r}: {exc}", key="timezone"
            ) from None
    return name_or_tz


@dataclass(frozen=True)
class CronField:
    """One parsed cron field: the set of values it matches."""

    name: str
    values: FrozenSet[int]
    #: True when the field was ``*`` (needed for the day-of-month/week rule).
    wildcard: bool = False
    raw: str = "*"

    def matches(self, value: int) -> bool:
        return value in self.values


def _parse_field(
    raw: str,
    name: str,
    low: int,
    high: int,
    names: Optional[Dict[str, int]] = None,
) -> CronField:
    """Parse one field into an explicit value set."""
    text = raw.strip().lower()
    if not text:
        raise ValidationError("empty field", key=name)
    wildcard = text == "*"
    values: set = set()
    for part in text.split(","):
        part = part.strip()
        if not part:
            raise ValidationError(
                f"empty list element in {raw!r}", key=name
            )
        step = 1
        if "/" in part:
            part, _, step_text = part.partition("/")
            try:
                step = int(step_text)
            except ValueError:
                raise ValidationError(
                    f"step must be an integer, got {step_text!r}", key=name
                ) from None
            if step < 1:
                raise ValidationError(f"step must be >= 1, got {step}", key=name)
        if part in ("*", ""):
            start, end = low, high
        elif "-" in part[1:]:
            left, _, right = part.partition("-")
            start = _value(left, name, low, high, names)
            end = _value(right, name, low, high, names)
            if end < start:
                raise ValidationError(
                    f"range {part!r} runs backwards", key=name
                )
        else:
            start = _value(part, name, low, high, names)
            end = high if step > 1 else start
        values.update(range(start, end + 1, step))
    if not values:
        raise ValidationError(f"field {raw!r} matches nothing", key=name)
    out = set(values)
    if name == "day_of_week":
        # 7 and 0 are both Sunday.
        out = {0 if v == 7 else v for v in out}
    for value in out:
        if not low <= value <= high:
            raise ValidationError(
                f"value {value} out of range {low}-{high}", key=name
            )
    return CronField(name, frozenset(out), wildcard, raw.strip())


def _value(
    text: str, name: str, low: int, high: int, names: Optional[Dict[str, int]]
) -> int:
    text = text.strip().lower()
    if names and text[:3] in names and not text.isdigit():
        return names[text[:3]]
    try:
        value = int(text)
    except ValueError:
        allowed = f" or one of {sorted(names)}" if names else ""
        raise ValidationError(
            f"{text!r} is not a number{allowed}", key=name
        ) from None
    if name == "day_of_week" and value == 7:
        value = 0
    if not low <= value <= high:
        raise ValidationError(
            f"value {value} out of range {low}-{high}", key=name
        )
    return value


def parse_cron(expression: str) -> Tuple[CronField, ...]:
    """Parse a 5-field cron expression (or a ``@macro``) into its fields."""
    text = expression.strip()
    if text.lower() in MACROS:
        text = MACROS[text.lower()]
    parts = text.split()
    if len(parts) != 5:
        raise ValidationError(
            f"expected 5 fields (minute hour day-of-month month day-of-week), "
            f"got {len(parts)} in {expression!r}",
            key="schedule",
        )
    minute, hour, dom, month, dow = parts
    return (
        _parse_field(minute, "minute", 0, 59),
        _parse_field(hour, "hour", 0, 23),
        _parse_field(dom, "day_of_month", 1, 31),
        _parse_field(month, "month", 1, 12, MONTH_NAMES),
        _parse_field(dow, "day_of_week", 0, 7, DOW_NAMES),
    )


class CronSchedule:
    """A timezone-aware cron schedule."""

    def __init__(self, expression: str, tz: object = "UTC") -> None:
        self.expression = expression.strip()
        self.tz = _tzinfo(tz)
        self.tz_name = tz if isinstance(tz, str) else str(tz)
        (
            self.minute,
            self.hour,
            self.day_of_month,
            self.month,
            self.day_of_week,
        ) = parse_cron(self.expression)

    def __repr__(self) -> str:  # pragma: no cover - debugging aid
        return f"CronSchedule({self.expression!r}, tz={self.tz_name!r})"

    # ---------------------------------------------------------------- matching

    def _day_matches(self, moment: datetime) -> bool:
        """Traditional cron day rule: OR when both day fields are restricted."""
        dom_ok = self.day_of_month.matches(moment.day)
        # Python: Monday=0. cron: Sunday=0.
        dow_ok = self.day_of_week.matches((moment.weekday() + 1) % 7)
        if self.day_of_month.wildcard and self.day_of_week.wildcard:
            return True
        if self.day_of_month.wildcard:
            return dow_ok
        if self.day_of_week.wildcard:
            return dom_ok
        return dom_ok or dow_ok

    def matches(self, moment: datetime) -> bool:
        """True if ``moment`` (to the minute) is a fire time."""
        local = self._to_local(moment)
        return (
            self.month.matches(local.month)
            and self._day_matches(local)
            and self.hour.matches(local.hour)
            and self.minute.matches(local.minute)
        )

    def next_run_after(self, after: datetime) -> datetime:
        """The first fire time strictly after ``after``."""
        local = self._to_local(after)
        naive = local.replace(tzinfo=None, second=0, microsecond=0) + timedelta(minutes=1)
        day_steps = 0
        while day_steps <= _MAX_DAY_STEPS:
            if not self.month.matches(naive.month):
                naive = _start_of_next_month(naive)
                day_steps += 28
                continue
            if not self._day_matches(naive):
                naive = (naive + timedelta(days=1)).replace(hour=0, minute=0)
                day_steps += 1
                continue
            if not self.hour.matches(naive.hour):
                nxt = naive + timedelta(hours=1)
                if nxt.date() != naive.date():
                    naive = nxt.replace(hour=0, minute=0)
                    day_steps += 1
                else:
                    naive = nxt.replace(minute=0)
                continue
            if not self.minute.matches(naive.minute):
                naive = naive + timedelta(minutes=1)
                continue
            return self._localise(naive)
        raise ValidationError(
            f"cron expression {self.expression!r} has no run time within "
            f"{_MAX_DAY_STEPS // 366} years",
            key="schedule",
        )

    def next_runs(self, after: datetime, count: int) -> List[datetime]:
        return next_runs(self, after, count)

    def describe(self) -> str:
        """Compact human summary of the parsed fields."""
        return (
            f"{self.expression} [{self.tz_name}] "
            f"minute={self.minute.raw} hour={self.hour.raw} "
            f"dom={self.day_of_month.raw} month={self.month.raw} "
            f"dow={self.day_of_week.raw}"
        )

    # ------------------------------------------------------------------ tz bits

    def _to_local(self, moment: datetime) -> datetime:
        if moment.tzinfo is None:
            return moment.replace(tzinfo=self.tz)
        return moment.astimezone(self.tz)

    def _localise(self, naive: datetime) -> datetime:
        """Attach the schedule's timezone, resolving DST edges explicitly."""
        aware = naive.replace(tzinfo=self.tz, fold=0)
        if _exists(aware, naive):
            return aware
        # Spring-forward gap: walk to the first local time that exists.
        probe = naive
        for _ in range(180):
            probe = probe + timedelta(minutes=1)
            candidate = probe.replace(tzinfo=self.tz, fold=0)
            if _exists(candidate, probe):
                return candidate
        raise ValidationError(  # pragma: no cover - a >3h gap does not exist
            f"could not resolve local time {naive.isoformat()} in {self.tz_name}",
            key="timezone",
        )


def _exists(aware: datetime, naive: datetime) -> bool:
    """False when a local wall time is inside a DST gap."""
    round_trip = aware.astimezone(timezone.utc).astimezone(aware.tzinfo)
    return round_trip.replace(tzinfo=None) == naive


def _start_of_next_month(moment: datetime) -> datetime:
    if moment.month == 12:
        return moment.replace(
            year=moment.year + 1, month=1, day=1, hour=0, minute=0
        )
    return moment.replace(month=moment.month + 1, day=1, hour=0, minute=0)


@dataclass
class IntervalSchedule:
    """Fire every ``seconds``, aligned to ``anchor``.

    Anchored, not "sleep(n) in a loop". A loop that sleeps drifts by however
    long the work took, so a five-minute job slowly becomes a six-minute job.
    Anchoring keeps the grid fixed, and a run that overshoots simply misses a
    slot instead of shifting every future slot.
    """

    seconds: float
    anchor: Optional[datetime] = None
    tz: object = "UTC"

    def __post_init__(self) -> None:
        if self.seconds <= 0:
            raise ValidationError("interval must be > 0 seconds", key="interval")
        self._tz = _tzinfo(self.tz)
        if self.anchor is not None and self.anchor.tzinfo is None:
            self.anchor = self.anchor.replace(tzinfo=self._tz)

    def next_run_after(self, after: datetime) -> datetime:
        moment = after if after.tzinfo else after.replace(tzinfo=self._tz)
        anchor = self.anchor or moment
        elapsed = (moment - anchor).total_seconds()
        if elapsed < 0:
            return anchor.astimezone(self._tz)
        steps = int(elapsed // self.seconds) + 1
        return (anchor + timedelta(seconds=steps * self.seconds)).astimezone(self._tz)


@dataclass
class OneShot:
    """Fire once, at ``when``. Returns ``None`` once it is in the past."""

    when: datetime
    tz: object = "UTC"

    def __post_init__(self) -> None:
        self._tz = _tzinfo(self.tz)
        if self.when.tzinfo is None:
            self.when = self.when.replace(tzinfo=self._tz)

    def next_run_after(self, after: datetime) -> Optional[datetime]:
        moment = after if after.tzinfo else after.replace(tzinfo=self._tz)
        return self.when if self.when > moment else None


def next_runs(schedule: Schedule, after: datetime, count: int) -> List[datetime]:
    """The next ``count`` fire times, feeding each result back in.

    Feeding results back is the point: it exercises the same "strictly after"
    contract the scheduler itself relies on, so a schedule that would loop
    forever on one instant shows up immediately.
    """
    out: List[datetime] = []
    cursor = after
    for _ in range(max(0, count)):
        nxt = schedule.next_run_after(cursor)
        if nxt is None:
            break
        out.append(nxt)
        cursor = nxt
    return out


def schedule_from_spec(spec: object) -> Schedule:
    """Build a schedule from the DSL's ``schedule:`` block.

    ``"*/15 * * * *"``                      -> cron in UTC
    ``{"cron": "0 3 * * *", "tz": "..."}``  -> cron in a zone
    ``{"every": 300}``                      -> interval
    ``{"at": "2026-01-01T03:00:00+00:00"}`` -> one-shot
    """
    if isinstance(spec, str):
        return CronSchedule(spec)
    if isinstance(spec, dict):
        tz = spec.get("tz") or spec.get("timezone") or "UTC"
        if "cron" in spec:
            return CronSchedule(str(spec["cron"]), tz)
        if "every" in spec:
            return IntervalSchedule(float(spec["every"]), tz=tz)
        if "at" in spec:
            return OneShot(datetime.fromisoformat(str(spec["at"])), tz=tz)
        raise ValidationError(
            "schedule needs one of 'cron', 'every' or 'at'", key="schedule"
        )
    raise ValidationError(f"cannot build a schedule from {spec!r}", key="schedule")
