#! /usr/bin/env python3
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

"""
Schedule processes and threads.
"""

from __future__ import annotations
import signal
import traceback
from datetime import datetime, timezone, timedelta
import meerschaum as mrsm
from meerschaum.utils.typing import Callable, Any, Optional, List, Dict
from meerschaum.utils.warnings import warn, error

STARTING_KEYWORD: str = 'starting'
INTERVAL_UNITS: List[str] = ['months', 'weeks', 'days', 'hours', 'minutes', 'seconds', 'years']
FREQUENCY_ALIASES: Dict[str, str] = {
    'daily': 'every 1 day',
    'hourly': 'every 1 hour',
    'minutely': 'every 1 minute',
    'weekly': 'every 1 week',
    'monthly': 'every 1 month',
    'secondly': 'every 1 second',
    'yearly': 'every 1 year',
}
LOGIC_ALIASES: Dict[str, str] = {
    'and': '&',
    'or': '|',
    ' through ': '-',
    ' thru ': '-',
    ' - ': '-',
    'beginning': STARTING_KEYWORD,
}
CRON_DAYS_OF_WEEK: List[str] = ['mon', 'tue', 'wed', 'thu', 'fri', 'sat', 'sun']
CRON_DAYS_OF_WEEK_ALIASES: Dict[str, str] = {
    'monday': 'mon',
    'tuesday': 'tue',
    'tues': 'tue',
    'wednesday': 'wed',
    'thursday': 'thu',
    'thurs': 'thu',
    'friday': 'fri',
    'saturday': 'sat',
    'sunday': 'sun',
}
CRON_MONTHS: List[str] = [
    'jan', 'feb', 'mar', 'apr', 'may', 'jun',
    'jul', 'aug', 'sep', 'oct', 'nov', 'dec',
]
CRON_MONTHS_ALIASES: Dict[str, str] = {
    'january': 'jan',
    'february': 'feb',
    'march': 'mar',
    'april': 'apr',
    'may': 'may',
    'june': 'jun',
    'july': 'jul',
    'august': 'aug',
    'september': 'sep',
    'october': 'oct',
    'november': 'nov',
    'december': 'dec',
}
SCHEDULE_ALIASES: Dict[str, str] = {
    **FREQUENCY_ALIASES,
    **LOGIC_ALIASES,
    **CRON_DAYS_OF_WEEK_ALIASES,
    **CRON_MONTHS_ALIASES,
}

_scheduler = None
def schedule_function(
    function: Callable[[Any], Any],
    schedule: str,
    *args,
    debug: bool = False,
    **kw
) -> mrsm.SuccessTuple:
    """
    Block the process and execute the function intermittently according to the frequency.
    https://meerschaum.io/reference/background-jobs/#-schedules

    Parameters
    ----------
    function: Callable[[Any], Any]
        The function to execute.

    schedule: str
        The frequency schedule at which `function` should be executed (e.g. `'daily'`).

    Returns
    -------
    A `SuccessTuple` upon exit.
    """
    import asyncio
    from meerschaum.utils.misc import filter_keywords

    global _scheduler
    kw['debug'] = debug
    kw = filter_keywords(function, **kw)

    _ = mrsm.attempt_import('attrs', lazy=False)
    apscheduler = mrsm.attempt_import('apscheduler', lazy=False)
    now = datetime.now(timezone.utc)
    trigger = parse_schedule(schedule, now=now)
    _scheduler = apscheduler.AsyncScheduler(identity='mrsm-scheduler')
    try:
        loop = asyncio.get_running_loop()
    except RuntimeError:
        loop = asyncio.new_event_loop()

    async def run_scheduler():
        async with _scheduler:
            job = await _scheduler.add_schedule(
                function,
                trigger,
                **filter_keywords(
                    _scheduler.add_schedule,
                    args=args,
                    kwargs=kw,
                    max_running_jobs=1,
                    conflict_policy=apscheduler.ConflictPolicy.replace,
                )
            )
            try:
                await _scheduler.run_until_stopped()
            except (KeyboardInterrupt, SystemExit) as e:
                await _stop_scheduler()
                raise e

    try:
        loop.run_until_complete(run_scheduler())
    except (KeyboardInterrupt, SystemExit):
        loop.run_until_complete(_stop_scheduler())

    return True, "Success"


def parse_schedule(schedule: str, now: Optional[datetime] = None):
    """
    Parse a schedule string (e.g. 'daily') into a Trigger object.
    """
    from meerschaum.utils.misc import items_str, is_int, filter_keywords
    (
        apscheduler_triggers_cron,
        apscheduler_triggers_interval,
        apscheduler_triggers_calendarinterval,
        apscheduler_triggers_combining,
    ) = (
        mrsm.attempt_import(
            'apscheduler.triggers.cron',
            'apscheduler.triggers.interval',
            'apscheduler.triggers.calendarinterval',
            'apscheduler.triggers.combining',
            lazy = False,
        )
    )

    starting_ts = parse_start_time(schedule, now=now)
    schedule = schedule.split(STARTING_KEYWORD, maxsplit=1)[0].strip()
    for alias_keyword, true_keyword in SCHEDULE_ALIASES.items():
        schedule = schedule.replace(alias_keyword, true_keyword)

    ### TODO Allow for combining `and` + `or` logic.
    if '&' in schedule and '|' in schedule:
        raise ValueError("Cannot accept both 'and' + 'or' logic in the schedule frequency.")

    join_str = '|' if '|' in schedule else '&'
    join_trigger = (
        apscheduler_triggers_combining.OrTrigger
        if join_str == '|'
        else apscheduler_triggers_combining.AndTrigger
    )
    join_kwargs = {
        'max_iterations': 1_000_000,
        'threshold': 0,
    } if join_str == '&' else {}

    schedule_parts = [part.strip() for part in schedule.split(join_str)]
    triggers = []

    has_seconds = 'second' in schedule
    has_minutes = 'minute' in schedule

    for schedule_part in schedule_parts:

        ### Intervals must begin with 'every' (after alias substitution).
        if schedule_part.lower().startswith('every '):
            schedule_num_str, schedule_unit = (
                schedule_part[len('every '):].split(' ', maxsplit=1)
            )
            schedule_unit = schedule_unit.rstrip('s') + 's'
            if schedule_unit not in INTERVAL_UNITS:
                raise ValueError(
                    f"Invalid interval '{schedule_unit}'.\n"
                    + f"    Accepted values are {items_str(INTERVAL_UNITS)}."
                )

            schedule_num = (
                int(schedule_num_str)
                if is_int(schedule_num_str)
                else float(schedule_num_str)
            )

            trigger = (
                apscheduler_triggers_interval.IntervalTrigger(
                    **filter_keywords(
                        apscheduler_triggers_interval.IntervalTrigger.__init__,
                        **{
                            schedule_unit: schedule_num,
                            'start_time': starting_ts,
                            'start_date': starting_ts,
                        }
                    )
                )
                if schedule_unit not in ('months', 'years') else (
                    apscheduler_triggers_calendarinterval.CalendarIntervalTrigger(
                        **{
                            schedule_unit: schedule_num,
                            'start_date': starting_ts,
                            'timezone': starting_ts.tzinfo,
                        }
                    )
                )
            )

        ### Determine whether this is a pure cron string or a cron subset (e.g. 'may-aug')_.
        else:
            first_three_prefix = schedule_part[:3].lower()
            first_four_prefix = schedule_part[:4].lower()
            cron_kw = {}
            if first_three_prefix in CRON_DAYS_OF_WEEK:
                cron_kw['day_of_week'] = schedule_part
            elif first_three_prefix in CRON_MONTHS:
                cron_kw['month'] = schedule_part
            elif is_int(first_four_prefix) and len(first_four_prefix) == 4:
                cron_kw['year'] = int(first_four_prefix)
            trigger = (
                apscheduler_triggers_cron.CronTrigger(
                    **{
                        **cron_kw,
                        'hour': '*',
                        'minute': '*' if has_minutes else starting_ts.minute,
                        'second': '*' if has_seconds else starting_ts.second,
                        'start_time': starting_ts,
                        'timezone': starting_ts.tzinfo,
                    }
                )
                if cron_kw
                else apscheduler_triggers_cron.CronTrigger.from_crontab(
                    schedule_part, 
                    timezone = starting_ts.tzinfo,
                )
            )
            ### Explicitly set the `start_time` after building with `from_crontab`.
            if trigger.start_time != starting_ts:
                trigger.start_time = starting_ts

        triggers.append(trigger)

    return (
        join_trigger(triggers, **join_kwargs)
        if len(triggers) != 1
        else triggers[0]
    )


def parse_start_time(schedule: str, now: Optional[datetime] = None) -> datetime:
    """
    Return the datetime to use for the given schedule string.

    Parameters
    ----------
    schedule: str
        The schedule frequency to be parsed into a starting datetime.

    now: Optional[datetime], default None
        If provided, use this value as a default if no start time is explicitly stated.

    Returns
    -------
    A `datetime` object, either `now` or the datetime embedded in the schedule string.

    Examples
    --------
    >>> parse_start_time('daily starting 2024-01-01')
    datetime.datetime(2024, 1, 1, 0, 0, tzinfo=datetime.timezone.utc)
    >>> parse_start_time('monthly starting 1st')
    datetime.datetime(2024, 5, 1, 0, 0, tzinfo=datetime.timezone.utc)
    >>> parse_start_time('hourly starting 00:30')
    datetime.datetime(2024, 5, 13, 0, 30, tzinfo=datetime.timezone.utc)
    """
    from meerschaum.utils.misc import round_time
    dateutil_parser = mrsm.attempt_import('dateutil.parser')
    starting_parts = schedule.split(STARTING_KEYWORD)
    starting_str = ('now' if len(starting_parts) == 1 else starting_parts[-1]).strip()
    now = now or datetime.now(timezone.utc)
    try:
        if starting_str == 'now':
            starting_ts = now
        elif starting_str.startswith('in '):
            delta_vals = starting_str.replace('in ', '').split(' ', maxsplit=1)
            delta_unit = delta_vals[-1].rstrip('s') + 's'
            delta_num = float(delta_vals[0])
            starting_ts = now + timedelta(**{delta_unit: delta_num})
        elif 'tomorrow' in starting_str or 'today' in starting_str:
            today = round_time(now, timedelta(days=1))
            tomorrow = today + timedelta(days=1)
            is_tomorrow = 'tomorrow' in starting_str
            time_str = starting_str.replace('tomorrow', '').replace('today', '').strip()
            time_ts = dateutil_parser.parse(time_str) if time_str else today
            starting_ts = (
                (tomorrow if is_tomorrow else today)
                + timedelta(hours=time_ts.hour)
                + timedelta(minutes=time_ts.minute)
            )
        else:
            starting_ts = dateutil_parser.parse(starting_str)
        schedule_parse_error = None
    except Exception as e:
        warn(f"Unable to parse starting time from '{starting_str}'.", stack=False)
        schedule_parse_error = str(e)
    if schedule_parse_error:
        error(schedule_parse_error, ValueError, stack=False)
    if not starting_ts.tzinfo:
        starting_ts = starting_ts.replace(tzinfo=timezone.utc)
    return starting_ts


async def _stop_scheduler():
    if _scheduler is None:
        return
    await _scheduler.stop()
    await _scheduler.wait_until_stopped()
