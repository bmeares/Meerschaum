#! /usr/bin/env python3
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

"""
Test the scheduling functions.
"""

import asyncio
import threading
import time
from datetime import datetime, timezone
from dateutil.tz import gettz
import pytest

@pytest.mark.parametrize(
    "schedule,expected_datetimes",
    [
        ("every 10 seconds starting 2024-05-01", [
            datetime(2024, 5, 1, 0, 0, 0, tzinfo=timezone.utc),
            datetime(2024, 5, 1, 0, 0, 10, tzinfo=timezone.utc),
            datetime(2024, 5, 1, 0, 0, 20, tzinfo=timezone.utc),
        ]),
        ("* * * * * starting 2024-05-01", [
            datetime(2024, 5, 1, 0, 0, 0, tzinfo=timezone.utc),
            datetime(2024, 5, 1, 0, 1, 0, tzinfo=timezone.utc),
            datetime(2024, 5, 1, 0, 2, 0, tzinfo=timezone.utc),
        ]),
        ("daily starting May 1, 2024", [
            datetime(2024, 5, 1, 0, 0, 0, tzinfo=timezone.utc),
            datetime(2024, 5, 2, 0, 0, 0, tzinfo=timezone.utc),
            datetime(2024, 5, 3, 0, 0, 0, tzinfo=timezone.utc),
        ]),
        ("weekly starting 2024-05-01", [
            datetime(2024, 5, 1, 0, 0, 0, tzinfo=timezone.utc),
            datetime(2024, 5, 8, 0, 0, 0, tzinfo=timezone.utc),
            datetime(2024, 5, 15, 0, 0, 0, tzinfo=timezone.utc),
        ]),
        ("monthly starting 2024-05-01", [
            datetime(2024, 5, 1, 0, 0, 0, tzinfo=timezone.utc),
            datetime(2024, 6, 1, 0, 0, 0, tzinfo=timezone.utc),
            datetime(2024, 7, 1, 0, 0, 0, tzinfo=timezone.utc),
        ]),
        ("hourly starting 2024-05-01", [
            datetime(2024, 5, 1, 0, 0, 0, tzinfo=timezone.utc),
            datetime(2024, 5, 1, 1, 0, 0, tzinfo=timezone.utc),
            datetime(2024, 5, 1, 2, 0, 0, tzinfo=timezone.utc),
        ]),
        ("minutely starting 2024-05-01", [
            datetime(2024, 5, 1, 0, 0, 0, tzinfo=timezone.utc),
            datetime(2024, 5, 1, 0, 1, 0, tzinfo=timezone.utc),
            datetime(2024, 5, 1, 0, 2, 0, tzinfo=timezone.utc),
        ]),
        ("secondly starting 2024-05-01", [
            datetime(2024, 5, 1, 0, 0, 0, tzinfo=timezone.utc),
            datetime(2024, 5, 1, 0, 0, 1, tzinfo=timezone.utc),
            datetime(2024, 5, 1, 0, 0, 2, tzinfo=timezone.utc),
        ]),
        ("every 3 seconds starting 2024-01-23 01:23:34", [
            datetime(2024, 1, 23, 1, 23, 34, tzinfo=timezone.utc),
            datetime(2024, 1, 23, 1, 23, 37, tzinfo=timezone.utc),
            datetime(2024, 1, 23, 1, 23, 40, tzinfo=timezone.utc),
        ]),
        ("weekly & daily starting 2024-05-01", [
            datetime(2024, 5, 1, 0, 0, 0, tzinfo=timezone.utc),
            datetime(2024, 5, 8, 0, 0, 0, tzinfo=timezone.utc),
            datetime(2024, 5, 15, 0, 0, 0, tzinfo=timezone.utc),
        ]),
        ("every 3 days & every 5 days starting 2024-05-01", [
            datetime(2024, 5, 1, 0, 0, 0, tzinfo=timezone.utc),
            datetime(2024, 5, 16, 0, 0, 0, tzinfo=timezone.utc),
            datetime(2024, 5, 31, 0, 0, 0, tzinfo=timezone.utc),
        ]),
        ("every 7 minutes and every 3 days starting 2024-05-01", [
            datetime(2024, 5, 1, 0, 0, 0, tzinfo=timezone.utc),
            datetime(2024, 5, 22, 0, 0, 0, tzinfo=timezone.utc),
            datetime(2024, 6, 12, 0, 0, 0, tzinfo=timezone.utc),
        ]),
        ("every 13 minutes & every 17 minutes starting 2024-05-01", [
            datetime(2024, 5, 1, 0, 0, 0, tzinfo=timezone.utc),
            datetime(2024, 5, 1, 3, 41, 0, tzinfo=timezone.utc),
            datetime(2024, 5, 1, 7, 22, 0, tzinfo=timezone.utc),
        ]),
        ("every 13 minutes | every 17 minutes starting 2024-05-01", [
            datetime(2024, 5, 1, 0, 0, 0, tzinfo=timezone.utc),
            datetime(2024, 5, 1, 0, 13, 0, tzinfo=timezone.utc),
            datetime(2024, 5, 1, 0, 17, 0, tzinfo=timezone.utc),
        ]),
        ("every 13 days | every 17 days starting 2024-05-01", [
            datetime(2024, 5, 1, 0, 0, 0, tzinfo=timezone.utc),
            datetime(2024, 5, 14, 0, 0, 0, tzinfo=timezone.utc),
            datetime(2024, 5, 18, 0, 0, 0, tzinfo=timezone.utc),
        ]),
        ("mon-fri and daily starting 2024-05-03", [
            datetime(2024, 5, 3, 0, 0, 0, tzinfo=timezone.utc),
            datetime(2024, 5, 6, 0, 0, 0, tzinfo=timezone.utc),
            datetime(2024, 5, 7, 0, 0, 0, tzinfo=timezone.utc),
        ]),
        ("mon-fri and every 5 minutes starting 2024-05-03", [
            datetime(2024, 5, 3, 0, 0, 0, tzinfo=timezone.utc),
            datetime(2024, 5, 3, 0, 5, 0, tzinfo=timezone.utc),
            datetime(2024, 5, 3, 0, 10, 0, tzinfo=timezone.utc),
        ]),
        ("mon-fri and every 2 days starting 2024-05-13", [
            datetime(2024, 5, 13, 0, 0, 0, tzinfo=timezone.utc),
            datetime(2024, 5, 15, 0, 0, 0, tzinfo=timezone.utc),
            datetime(2024, 5, 17, 0, 0, 0, tzinfo=timezone.utc),
            datetime(2024, 5, 21, 0, 0, 0, tzinfo=timezone.utc),
        ]),
        ("every 3 hours and every 5 hours starting 2024-05-03", [
            datetime(2024, 5, 3, 0, 0, 0, tzinfo=timezone.utc),
            datetime(2024, 5, 3, 15, 0, 0, tzinfo=timezone.utc),
            datetime(2024, 5, 4, 6, 0, 0, tzinfo=timezone.utc),
        ]),
        ("daily starting tomorrow", [
            datetime(2024, 5, 2, 0, 0, 0, tzinfo=timezone.utc),
            datetime(2024, 5, 3, 0, 0, 0, tzinfo=timezone.utc),
            datetime(2024, 5, 4, 0, 0, 0, tzinfo=timezone.utc),
        ]),
        ("daily starting 11:00 tomorrow", [
            datetime(2024, 5, 2, 11, 0, 0, tzinfo=timezone.utc),
            datetime(2024, 5, 3, 11, 0, 0, tzinfo=timezone.utc),
            datetime(2024, 5, 4, 11, 0, 0, tzinfo=timezone.utc),
        ]),
        ("daily starting 11:00 today", [
            datetime(2024, 5, 1, 11, 0, 0, tzinfo=timezone.utc),
            datetime(2024, 5, 2, 11, 0, 0, tzinfo=timezone.utc),
            datetime(2024, 5, 3, 11, 0, 0, tzinfo=timezone.utc),
        ]),
        ("daily starting 12:30 tomorrow", [
            datetime(2024, 5, 2, 12, 30, 0, tzinfo=timezone.utc),
            datetime(2024, 5, 3, 12, 30, 0, tzinfo=timezone.utc),
            datetime(2024, 5, 4, 12, 30, 0, tzinfo=timezone.utc),
        ]),
    ],
)
def test_parse_schedule(schedule, expected_datetimes):
    """
    Test various schedule string formats.
    """
    from meerschaum.utils.schedule import parse_schedule
    now = datetime(2024, 5, 1, 12, 31, 52, tzinfo=timezone.utc)
    trigger = parse_schedule(schedule, now=now)
    for expected_dt in expected_datetimes:
        next_dt = trigger.next()
        assert next_dt == expected_dt


def test_calendar_schedules_skip_invalid_dates():
    """Month- and year-based schedules retain their original calendar day."""
    from meerschaum.utils.schedule import parse_schedule

    monthly = parse_schedule('monthly starting 2024-01-31')
    assert [monthly.next() for _ in range(3)] == [
        datetime(2024, 1, 31, tzinfo=timezone.utc),
        datetime(2024, 3, 31, tzinfo=timezone.utc),
        datetime(2024, 5, 31, tzinfo=timezone.utc),
    ]

    yearly = parse_schedule('yearly starting 2024-02-29')
    assert [yearly.next() for _ in range(2)] == [
        datetime(2024, 2, 29, tzinfo=timezone.utc),
        datetime(2028, 2, 29, tzinfo=timezone.utc),
    ]

    monthly_at_time = parse_schedule('monthly starting 2024-01-01 12:34:56')
    assert monthly_at_time.next() == datetime(2024, 1, 1, tzinfo=timezone.utc)


def test_cron_skips_nonexistent_dst_time():
    """A wall-clock time skipped by spring-forward must not be fabricated."""
    from meerschaum.utils.schedule import parse_schedule

    eastern = gettz('America/New_York')
    trigger = parse_schedule(
        '30 2 * * *',
        now=datetime(2024, 3, 9, 2, 30, tzinfo=eastern),
    )
    assert trigger.next() == datetime(2024, 3, 9, 2, 30, tzinfo=eastern)
    assert trigger.next() == datetime(2024, 3, 11, 2, 30, tzinfo=eastern)


def test_cron_repeats_ambiguous_dst_time():
    """A wall-clock time repeated by fall-back must fire for both occurrences."""
    from meerschaum.utils.schedule import parse_schedule

    eastern = gettz('America/New_York')
    trigger = parse_schedule(
        '30 1 * * *',
        now=datetime(2024, 11, 3, 1, 30, tzinfo=eastern),
    )
    first = trigger.next()
    second = trigger.next()
    assert first == datetime(2024, 11, 3, 1, 30, tzinfo=eastern, fold=0)
    assert second == datetime(2024, 11, 3, 1, 30, tzinfo=eastern, fold=1)
    assert second.timestamp() - first.timestamp() == 3600


def test_cron_numeric_weekdays_retain_crontab_semantics():
    """Numeric cron weekdays use 0 and 7 for Sunday, while 1 is Monday."""
    from meerschaum.utils.schedule import parse_schedule

    sunday = parse_schedule('0 0 * * 0', now=datetime(2024, 1, 1, tzinfo=timezone.utc))
    monday = parse_schedule('0 0 * * 1', now=datetime(2024, 1, 1, tzinfo=timezone.utc))
    assert sunday.next() == datetime(2024, 1, 7, tzinfo=timezone.utc)
    assert monday.next() == datetime(2024, 1, 1, tzinfo=timezone.utc)


def test_cron_day_fields_and_steps_retain_crontab_semantics():
    """Restricted day fields are ORed, and numeric steps begin with Sunday."""
    from meerschaum.utils.schedule import parse_schedule

    day_or_friday = parse_schedule(
        '0 0 13 * 5',
        now=datetime(2024, 5, 1, tzinfo=timezone.utc),
    )
    stepped_weekdays = parse_schedule(
        '0 0 * * */2',
        now=datetime(2024, 5, 1, tzinfo=timezone.utc),
    )
    stepped_days = parse_schedule(
        '0 0 */2 * *',
        now=datetime(2024, 5, 1, tzinfo=timezone.utc),
    )

    assert day_or_friday.next() == datetime(2024, 5, 3, tzinfo=timezone.utc)
    assert stepped_weekdays.next() == datetime(2024, 5, 2, tzinfo=timezone.utc)
    assert stepped_days.next() == datetime(2024, 5, 1, tzinfo=timezone.utc)
    assert stepped_days.next() == datetime(2024, 5, 3, tzinfo=timezone.utc)


def test_crontab_reference_examples():
    """Examples from crontab(5) retain their standard field behavior."""
    from meerschaum.utils.schedule import parse_schedule

    every_other_hour = parse_schedule(
        '23 0-23/2 * * *',
        now=datetime(2024, 5, 1, tzinfo=timezone.utc),
    )
    sunday = parse_schedule(
        '5 4 * * sun',
        now=datetime(2024, 5, 1, tzinfo=timezone.utc),
    )
    first_fifteenth_or_friday = parse_schedule(
        '30 4 1,15 * 5',
        now=datetime(2024, 5, 1, tzinfo=timezone.utc),
    )

    assert [every_other_hour.next() for _ in range(2)] == [
        datetime(2024, 5, 1, 0, 23, tzinfo=timezone.utc),
        datetime(2024, 5, 1, 2, 23, tzinfo=timezone.utc),
    ]
    assert sunday.next() == datetime(2024, 5, 5, 4, 5, tzinfo=timezone.utc)
    assert [first_fifteenth_or_friday.next() for _ in range(3)] == [
        datetime(2024, 5, 1, 4, 30, tzinfo=timezone.utc),
        datetime(2024, 5, 3, 4, 30, tzinfo=timezone.utc),
        datetime(2024, 5, 10, 4, 30, tzinfo=timezone.utc),
    ]


def test_sparse_cron_schedules():
    """Sparse schedules jump across rejected dates without changing results."""
    from meerschaum.utils.schedule import parse_schedule

    annual = parse_schedule('0 0 1 1 *', now=datetime(2024, 1, 2, tzinfo=timezone.utc))
    leap_day = parse_schedule('0 0 29 2 *', now=datetime(2024, 3, 1, tzinfo=timezone.utc))
    assert annual.next() == datetime(2025, 1, 1, tzinfo=timezone.utc)
    assert leap_day.next() == datetime(2028, 2, 29, tzinfo=timezone.utc)


def test_schedule_function_stops_without_overlapping_runs():
    """Stopping wakes the scheduler, and slow functions never overlap themselves."""
    from meerschaum.utils.schedule import schedule_function, _stop_scheduler

    state = {'active': 0, 'max_active': 0, 'calls': 0}
    started = threading.Event()

    def slow_function():
        state['active'] += 1
        state['max_active'] = max(state['max_active'], state['active'])
        state['calls'] += 1
        started.set()
        time.sleep(0.03)
        state['active'] -= 1

    thread = threading.Thread(
        target=schedule_function,
        args=(slow_function, 'every 0.01 seconds'),
        daemon=True,
    )
    thread.start()
    assert started.wait(1.0)
    time.sleep(0.08)
    asyncio.run(_stop_scheduler())
    thread.join(1.0)

    assert not thread.is_alive()
    assert state['calls'] >= 2
    assert state['max_active'] == 1
