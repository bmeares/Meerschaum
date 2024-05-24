#! /usr/bin/env python3
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

"""
Test the scheduling functions.
"""

from datetime import datetime, timezone
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
