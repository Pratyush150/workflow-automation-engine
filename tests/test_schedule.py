"""Cron parsing and next-run times, checked against hand-computed answers."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone

import pytest

from flowforge.errors import ValidationError
from flowforge.schedule import (
    CronSchedule,
    IntervalSchedule,
    OneShot,
    next_runs,
    parse_cron,
    schedule_from_spec,
)

try:
    from zoneinfo import ZoneInfo

    NEW_YORK = ZoneInfo("America/New_York")
    LONDON = ZoneInfo("Europe/London")
    HAVE_TZDATA = True
except Exception:  # pragma: no cover - platform without a tz database
    HAVE_TZDATA = False

needs_tzdata = pytest.mark.skipif(
    not HAVE_TZDATA, reason="no IANA timezone database on this machine"
)


def utc(*args: int) -> datetime:
    return datetime(*args, tzinfo=timezone.utc)


def test_every_fifteen_minutes():
    schedule = CronSchedule("*/15 * * * *")
    assert next_runs(schedule, utc(2026, 1, 1, 0, 3), 5) == [
        utc(2026, 1, 1, 0, 15),
        utc(2026, 1, 1, 0, 30),
        utc(2026, 1, 1, 0, 45),
        utc(2026, 1, 1, 1, 0),
        utc(2026, 1, 1, 1, 15),
    ]


def test_exact_minute_is_not_returned_again():
    """next_run_after is strictly after, so feeding a result back cannot loop."""
    schedule = CronSchedule("*/15 * * * *")
    first = schedule.next_run_after(utc(2026, 1, 1, 0, 0))
    assert first == utc(2026, 1, 1, 0, 15)
    assert schedule.next_run_after(first) == utc(2026, 1, 1, 0, 30)


def test_ranges_and_steps_inside_a_range():
    schedule = CronSchedule("0 9-17/4 * * *")
    assert next_runs(schedule, utc(2026, 6, 1, 0, 0), 4) == [
        utc(2026, 6, 1, 9, 0),
        utc(2026, 6, 1, 13, 0),
        utc(2026, 6, 1, 17, 0),
        utc(2026, 6, 2, 9, 0),
    ]


def test_lists_of_values():
    schedule = CronSchedule("0,30 6,18 * * *")
    assert next_runs(schedule, utc(2026, 6, 1, 0, 0), 5) == [
        utc(2026, 6, 1, 6, 0),
        utc(2026, 6, 1, 6, 30),
        utc(2026, 6, 1, 18, 0),
        utc(2026, 6, 1, 18, 30),
        utc(2026, 6, 2, 6, 0),
    ]


def test_day_of_week_names_are_case_insensitive():
    # 2026-06-01 is a Monday.
    weekdays = CronSchedule("0 8 * * MON-fri")
    runs = next_runs(weekdays, utc(2026, 6, 5, 9, 0), 3)
    assert runs == [utc(2026, 6, 8, 8, 0), utc(2026, 6, 9, 8, 0), utc(2026, 6, 10, 8, 0)]
    assert runs[0].weekday() == 0


def test_sunday_is_both_zero_and_seven():
    assert CronSchedule("0 0 * * 0").day_of_week.values == frozenset({0})
    assert CronSchedule("0 0 * * 7").day_of_week.values == frozenset({0})
    assert CronSchedule("0 0 * * sun").day_of_week.values == frozenset({0})


def test_month_names_and_month_field():
    schedule = CronSchedule("0 0 1 jan,jul *")
    assert next_runs(schedule, utc(2026, 3, 1), 2) == [utc(2026, 7, 1), utc(2027, 1, 1)]


def test_day_of_month_and_day_of_week_are_ORed():
    """Traditional cron: restricted dom + restricted dow means either matches."""
    schedule = CronSchedule("0 0 13 * mon")
    # March 2026: Mondays are 2, 9, 16, 23, 30 -- and the 13th is a Friday.
    runs = next_runs(schedule, utc(2026, 3, 1), 4)
    assert runs == [utc(2026, 3, 2), utc(2026, 3, 9), utc(2026, 3, 13), utc(2026, 3, 16)]


def test_macros():
    assert CronSchedule("@daily").next_run_after(utc(2026, 5, 5, 4)) == utc(2026, 5, 6)
    assert CronSchedule("@hourly").next_run_after(utc(2026, 5, 5, 4, 30)) == utc(
        2026, 5, 5, 5
    )


def test_matches_is_consistent_with_next_run_after():
    schedule = CronSchedule("*/20 * * * *")
    moment = schedule.next_run_after(utc(2026, 1, 1, 3, 5))
    assert schedule.matches(moment) is True
    assert schedule.matches(moment + timedelta(minutes=1)) is False


@needs_tzdata
def test_spring_forward_fires_once_at_the_first_real_instant():
    """US DST 2026-03-08: 02:00 -> 03:00. A 02:30 job has no 02:30 to run at."""
    schedule = CronSchedule("30 2 * * *", "America/New_York")
    runs = next_runs(schedule, datetime(2026, 3, 7, 12, 0, tzinfo=NEW_YORK), 3)

    # Exactly one run on the transition day, pushed to the first local time
    # that exists: 03:00 EDT.
    transition = [r for r in runs if r.date() == datetime(2026, 3, 8).date()]
    assert len(transition) == 1
    assert (transition[0].hour, transition[0].minute) == (3, 0)
    assert transition[0].utcoffset() == timedelta(hours=-4)
    # 07:00 UTC: one hour after 02:00 EST, i.e. the instant the clocks jumped.
    assert transition[0].astimezone(timezone.utc) == utc(2026, 3, 8, 7, 0)
    # The days either side are ordinary 02:30 runs.
    assert runs == [
        transition[0],
        datetime(2026, 3, 9, 2, 30, tzinfo=NEW_YORK),
        datetime(2026, 3, 10, 2, 30, tzinfo=NEW_YORK),
    ]


@needs_tzdata
def test_fall_back_fires_once_on_the_first_pass():
    """US DST 2026-11-01: 02:00 -> 01:00. A 01:30 job must not run twice."""
    schedule = CronSchedule("30 1 * * *", "America/New_York")
    runs = next_runs(schedule, datetime(2026, 10, 31, 12, 0, tzinfo=NEW_YORK), 3)
    on_the_day = [r for r in runs if r.date() == datetime(2026, 11, 1).date()]
    assert len(on_the_day) == 1
    # First pass: still on daylight time (UTC-4), i.e. 05:30 UTC.
    assert on_the_day[0].utcoffset() == timedelta(hours=-4)
    assert on_the_day[0].astimezone(timezone.utc) == utc(2026, 11, 1, 5, 30)


@needs_tzdata
def test_a_job_inside_the_gap_every_half_hour_does_not_double_fire():
    schedule = CronSchedule("*/30 * * * *", "America/New_York")
    runs = next_runs(schedule, datetime(2026, 3, 8, 1, 0, tzinfo=NEW_YORK), 4)
    locals_ = [(r.hour, r.minute) for r in runs]
    assert locals_ == [(1, 30), (3, 0), (3, 30), (4, 0)]
    assert len(set(runs)) == len(runs)


@needs_tzdata
def test_schedules_are_evaluated_in_their_own_timezone():
    schedule = CronSchedule("0 3 * * *", "Europe/London")
    # 02:00 UTC in July is 03:00 BST.
    run = schedule.next_run_after(utc(2026, 7, 1, 0, 0))
    assert run.astimezone(timezone.utc) == utc(2026, 7, 1, 2, 0)
    assert run.hour == 3


def test_interval_schedule_is_anchored_and_does_not_drift():
    anchor = utc(2026, 1, 1, 0, 0)
    schedule = IntervalSchedule(300, anchor=anchor)
    assert schedule.next_run_after(utc(2026, 1, 1, 0, 7)) == utc(2026, 1, 1, 0, 10)
    # A late call still lands on the grid rather than shifting it.
    assert schedule.next_run_after(utc(2026, 1, 1, 0, 9, 59)) == utc(2026, 1, 1, 0, 10)
    assert next_runs(schedule, anchor, 3) == [
        utc(2026, 1, 1, 0, 5),
        utc(2026, 1, 1, 0, 10),
        utc(2026, 1, 1, 0, 15),
    ]


def test_one_shot_fires_once_then_never():
    when = utc(2026, 4, 1, 9, 0)
    schedule = OneShot(when)
    assert schedule.next_run_after(utc(2026, 3, 31)) == when
    assert schedule.next_run_after(when) is None
    assert next_runs(schedule, utc(2026, 5, 1), 3) == []


def test_parse_errors_name_the_field():
    with pytest.raises(ValidationError) as excinfo:
        CronSchedule("0 25 * * *")
    assert "hour" in str(excinfo.value)
    with pytest.raises(ValidationError) as excinfo:
        CronSchedule("* * * *")
    assert "5 fields" in str(excinfo.value)
    with pytest.raises(ValidationError) as excinfo:
        CronSchedule("0 0 * * funday")
    assert "day_of_week" in str(excinfo.value)
    with pytest.raises(ValidationError):
        CronSchedule("*/0 * * * *")
    with pytest.raises(ValidationError):
        CronSchedule("0 17-9 * * *")


def test_parse_cron_exposes_the_field_sets():
    minute, hour, dom, month, dow = parse_cron("*/15 0 1 * *")
    assert minute.values == frozenset({0, 15, 30, 45})
    assert hour.values == frozenset({0})
    assert dom.values == frozenset({1})
    assert month.wildcard is True
    assert dow.wildcard is True


def test_schedule_from_spec_covers_the_dsl_forms():
    assert isinstance(schedule_from_spec("0 3 * * *"), CronSchedule)
    assert isinstance(schedule_from_spec({"cron": "0 3 * * *"}), CronSchedule)
    assert isinstance(schedule_from_spec({"every": 60}), IntervalSchedule)
    assert isinstance(schedule_from_spec({"at": "2026-01-01T00:00:00+00:00"}), OneShot)
    with pytest.raises(ValidationError):
        schedule_from_spec({"nope": 1})
