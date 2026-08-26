#! /usr/bin/env python3
"""Parse schedules and run functions at their next occurrence."""

from __future__ import annotations

import threading
import traceback
from datetime import date, datetime, time, timezone, timedelta

import meerschaum as mrsm
from meerschaum.utils.typing import Callable, Any, Optional, List, Dict
from meerschaum.utils.warnings import warn, error


STARTING_KEYWORD: str = 'starting'
INTERVAL_UNITS: List[str] = ['months', 'weeks', 'days', 'hours', 'minutes', 'seconds', 'years']
FREQUENCY_ALIASES: Dict[str, str] = {
    'daily': 'every 1 day', 'hourly': 'every 1 hour',
    'minutely': 'every 1 minute', 'weekly': 'every 1 week',
    'monthly': 'every 1 month', 'secondly': 'every 1 second',
    'yearly': 'every 1 year',
}
LOGIC_ALIASES: Dict[str, str] = {
    'and': '&', 'or': '|', ' through ': '-', ' thru ': '-', ' - ': '-',
    'beginning': STARTING_KEYWORD,
}
CRON_DAYS_OF_WEEK: List[str] = ['mon', 'tue', 'wed', 'thu', 'fri', 'sat', 'sun']
CRON_DAYS_OF_WEEK_ALIASES: Dict[str, str] = {
    'monday': 'mon', 'tuesday': 'tue', 'tues': 'tue', 'wednesday': 'wed',
    'thursday': 'thu', 'thurs': 'thu', 'friday': 'fri', 'saturday': 'sat',
    'sunday': 'sun',
}
CRON_MONTHS: List[str] = [
    'jan', 'feb', 'mar', 'apr', 'may', 'jun',
    'jul', 'aug', 'sep', 'oct', 'nov', 'dec',
]
CRON_MONTHS_ALIASES: Dict[str, str] = {
    'january': 'jan', 'february': 'feb', 'march': 'mar', 'april': 'apr',
    'may': 'may', 'june': 'jun', 'july': 'jul', 'august': 'aug',
    'september': 'sep', 'october': 'oct', 'november': 'nov', 'december': 'dec',
}
SCHEDULE_ALIASES: Dict[str, str] = {
    **FREQUENCY_ALIASES, **LOGIC_ALIASES,
    **CRON_DAYS_OF_WEEK_ALIASES, **CRON_MONTHS_ALIASES,
}


class _IntervalTrigger:
    def __init__(self, start_time: datetime, **interval: float):
        self.start_time = start_time
        self._interval = timedelta(**interval)
        if self._interval.total_seconds() <= 0:
            raise ValueError("The time interval must be positive")
        self._last_fire_time = None

    def next(self) -> datetime:
        self._last_fire_time = (
            self.start_time
            if self._last_fire_time is None
            else self._last_fire_time + self._interval
        )
        return self._last_fire_time


class _CalendarIntervalTrigger:
    """Month/year intervals which retain the original day and skip invalid dates."""

    def __init__(self, start_time: datetime, years: int = 0, months: int = 0):
        if years < 0 or months < 0 or years == months == 0:
            raise ValueError("The calendar interval must be positive")
        self.start_time = start_time
        self.start_date = start_time.date()
        self.timezone = start_time.tzinfo
        self.years = years
        self.months = months
        # APScheduler's calendar trigger defaulted to midnight; retain that behavior.
        self._time = time(tzinfo=self.timezone)
        self._last_fire_date = None

    def next(self) -> datetime:
        previous_date = self._last_fire_date
        while True:
            if previous_date is None:
                next_date = self.start_date
            else:
                year, month = previous_date.year, previous_date.month
                while True:
                    month += self.months
                    year += self.years + ((month - 1) // 12)
                    month = ((month - 1) % 12) + 1
                    try:
                        next_date = date(year, month, previous_date.day)
                    except ValueError:
                        continue
                    break
            candidate = datetime.fromtimestamp(
                datetime.combine(next_date, self._time).timestamp(), self.timezone,
            )
            if candidate.timetz() != self._time:
                previous_date = candidate.date()
                continue
            self._last_fire_date = next_date
            return candidate


def _expand_cron_field(
    expression: str,
    minimum: int,
    maximum: int,
    names: Optional[List[str]] = None,
) -> set[int]:
    values = set()
    names_map = {name: i + minimum for i, name in enumerate(names or [])}

    def as_int(value: str) -> int:
        return names_map.get(value.lower(), int(value) if value.lstrip('-').isdigit() else -1)

    for part in expression.lower().split(','):
        range_part, separator, step_str = part.partition('/')
        step = int(step_str) if step_str else 1
        if step <= 0:
            raise ValueError(f"Invalid cron step '{step}'.")
        if range_part == '*':
            start, stop = minimum, maximum
        elif '-' in range_part:
            start_str, stop_str = range_part.split('-', 1)
            start, stop = as_int(start_str), as_int(stop_str)
        else:
            start = as_int(range_part)
            stop = maximum if separator else start
        if start < minimum or stop > maximum or start > stop:
            raise ValueError(f"Invalid cron field '{expression}'.")
        values.update(range(start, stop + 1, step))
    return values


def _expand_cron_weekdays(expression: str) -> set[int]:
    """Return Python weekdays while retaining cron's 0/7 = Sunday convention."""
    values = set()
    names_map = {
        'sun': 0,
        **{name: i + 1 for i, name in enumerate(CRON_DAYS_OF_WEEK[:-1])},
    }

    def as_cron_int(value: str) -> int:
        return names_map.get(
            value.lower(),
            int(value) if value.isdigit() else -1,
        )

    for part in expression.lower().split(','):
        range_part, separator, step_str = part.partition('/')
        step = int(step_str) if separator else 1
        if range_part == '*':
            cron_values = range(0, 7, step)
        else:
            endpoints = [as_cron_int(value) for value in range_part.split('-', 1)]
            if (
                any(value < 0 or value > 7 for value in endpoints)
                or step <= 0
                or (len(endpoints) == 2 and endpoints[0] > endpoints[1])
            ):
                raise ValueError(f"Invalid cron field '{expression}'.")
            cron_values = (
                range(endpoints[0], endpoints[1] + 1, step)
                if len(endpoints) == 2
                else (range(endpoints[0], 8, step) if separator else endpoints)
            )
        if step <= 0:
            raise ValueError(f"Invalid cron field '{expression}'.")
        values.update(6 if value in (0, 7) else value - 1 for value in cron_values)
    return values


class _CronTrigger:
    """The five-field cron subset accepted by :func:`parse_schedule`."""

    def __init__(
        self,
        start_time: datetime,
        minute: str = '*',
        hour: str = '*',
        day: str = '*',
        month: str = '*',
        day_of_week: str = '*',
        year: str = '*',
        second: str = '0',
    ):
        self.start_time = start_time
        self.timezone = start_time.tzinfo
        self._minutes = _expand_cron_field(str(minute), 0, 59)
        self._hours = _expand_cron_field(str(hour), 0, 23)
        self._days = _expand_cron_field(str(day), 1, 31)
        self._days_have_wildcard = '*' in str(day)
        self._months = _expand_cron_field(str(month), 1, 12, CRON_MONTHS)
        self._weekdays = _expand_cron_weekdays(str(day_of_week))
        self._weekdays_have_wildcard = '*' in str(day_of_week)
        self._years = None if str(year) == '*' else _expand_cron_field(str(year), 1, 9999)
        self._seconds = _expand_cron_field(str(second), 0, 59)
        self._step_seconds = 1 if len(self._seconds) > 1 else 60
        self._last_fire_time = None

    def _matches(self, candidate: datetime) -> bool:
        return (
            self._date_matches(candidate)
            and candidate.hour in self._hours
            and candidate.minute in self._minutes
            and candidate.second in self._seconds
        )

    def _date_matches(self, candidate: datetime) -> bool:
        day_matches = candidate.day in self._days
        weekday_matches = candidate.weekday() in self._weekdays
        if self._days_have_wildcard or self._weekdays_have_wildcard:
            day_matches = day_matches and weekday_matches
        else:
            day_matches = day_matches or weekday_matches
        return (
            (self._years is None or candidate.year in self._years)
            and candidate.month in self._months
            and day_matches
        )

    def next(self) -> Optional[datetime]:
        if self._last_fire_time is None:
            candidate = self.start_time.replace(microsecond=0)
            if self._step_seconds == 60:
                candidate = candidate.replace(second=min(self._seconds))
                if candidate < self.start_time:
                    candidate = datetime.fromtimestamp(candidate.timestamp() + 60, self.timezone)
        else:
            candidate = datetime.fromtimestamp(
                self._last_fire_time.timestamp() + self._step_seconds, self.timezone,
            )

        # ponytail: skip rejected dates but keep simple stepping within an eligible day.
        max_iterations = 11 * 366 * 24 * 60 * (60 if self._step_seconds == 1 else 1)
        for _ in range(max_iterations):
            if self._matches(candidate):
                self._last_fire_time = candidate
                return candidate
            if not self._date_matches(candidate):
                candidate = datetime.fromtimestamp(
                    datetime.combine(
                        candidate.date() + timedelta(days=1),
                        candidate.timetz(),
                    ).timestamp(),
                    self.timezone,
                )
                continue
            candidate = datetime.fromtimestamp(
                candidate.timestamp() + self._step_seconds, self.timezone,
            )
        return None


class _OrTrigger:
    def __init__(self, triggers):
        self.triggers = triggers
        self._next_fire_times = []

    def next(self) -> Optional[datetime]:
        if not self._next_fire_times:
            self._next_fire_times = [trigger.next() for trigger in self.triggers]
        earliest = min((ts for ts in self._next_fire_times if ts is not None), default=None)
        if earliest is not None:
            for i, fire_time in enumerate(self._next_fire_times):
                if fire_time == earliest:
                    self._next_fire_times[i] = self.triggers[i].next()
        return earliest


class _AndTrigger:
    def __init__(self, triggers, max_iterations: int = 1_000_000):
        self.triggers = triggers
        self.max_iterations = max_iterations
        self._next_fire_times = []

    def next(self) -> Optional[datetime]:
        if not self._next_fire_times:
            self._next_fire_times = [trigger.next() for trigger in self.triggers]
        for _ in range(self.max_iterations):
            if any(ts is None for ts in self._next_fire_times):
                return None
            earliest, latest = min(self._next_fire_times), max(self._next_fire_times)
            for i, fire_time in enumerate(self._next_fire_times):
                if fire_time == earliest:
                    self._next_fire_times[i] = self.triggers[i].next()
            if latest == earliest:
                return earliest
        raise RuntimeError("Maximum iterations reached while combining schedules.")


class _Scheduler:
    def __init__(self):
        self.stop_event = threading.Event()

    async def stop(self):
        self.stop_event.set()

    async def wait_until_stopped(self):
        return None


_scheduler = None
def schedule_function(
    function: Callable[[Any], Any],
    schedule: str,
    *args,
    debug: bool = False,
    **kw
) -> mrsm.SuccessTuple:
    """Block the process and execute ``function`` according to ``schedule``."""
    from meerschaum.utils.misc import filter_keywords

    global _scheduler
    kw['debug'] = debug
    kw = filter_keywords(function, **kw)
    trigger = parse_schedule(schedule, now=datetime.now(timezone.utc))
    scheduler = _scheduler = _Scheduler()
    pending_next_time = None
    schedule_finished = False
    try:
        while not scheduler.stop_event.is_set():
            next_time = pending_next_time or trigger.next()
            pending_next_time = None
            if next_time is None:
                break
            now = datetime.now(next_time.tzinfo or timezone.utc)
            if next_time <= now:
                while True:
                    candidate = trigger.next()
                    if candidate is None or candidate > now:
                        pending_next_time = candidate
                        schedule_finished = candidate is None
                        break
                    next_time = candidate
            if scheduler.stop_event.wait(max(0.0, (next_time - now).total_seconds())):
                break
            try:
                function(*args, **kw)
            except Exception:
                warn(f"Scheduled function failed:\n{traceback.format_exc()}", stack=False)
            if schedule_finished:
                break
    except (KeyboardInterrupt, SystemExit):
        scheduler.stop_event.set()
    return True, "Success"


def parse_schedule(schedule: str, now: Optional[datetime] = None):
    """Parse a schedule string into a stateful object with a ``next()`` method."""
    from meerschaum.utils.misc import items_str, is_int

    starting_ts = parse_start_time(schedule, now=now)
    schedule = schedule.split(STARTING_KEYWORD, maxsplit=1)[0].strip()
    for alias_keyword, true_keyword in SCHEDULE_ALIASES.items():
        schedule = schedule.replace(alias_keyword, true_keyword)
    if '&' in schedule and '|' in schedule:
        raise ValueError("Cannot accept both 'and' + 'or' logic in the schedule frequency.")

    join_str = '|' if '|' in schedule else '&'
    schedule_parts = [part.strip() for part in schedule.split(join_str)]
    triggers = []
    has_seconds = 'second' in schedule
    has_minutes = 'minute' in schedule
    for schedule_part in schedule_parts:
        if schedule_part.lower().startswith('every '):
            schedule_num_str, schedule_unit = schedule_part[len('every '):].split(' ', maxsplit=1)
            schedule_unit = schedule_unit.rstrip('s') + 's'
            if schedule_unit not in INTERVAL_UNITS:
                raise ValueError(
                    f"Invalid interval '{schedule_unit}'.\n"
                    + f"    Accepted values are {items_str(INTERVAL_UNITS)}."
                )
            schedule_num = int(schedule_num_str) if is_int(schedule_num_str) else float(schedule_num_str)
            if schedule_unit in ('months', 'years'):
                if not float(schedule_num).is_integer():
                    raise ValueError(f"Calendar interval '{schedule_num}' must be an integer.")
                trigger = _CalendarIntervalTrigger(
                    starting_ts, **{schedule_unit: int(schedule_num)},
                )
            else:
                trigger = _IntervalTrigger(starting_ts, **{schedule_unit: schedule_num})
        else:
            first_three_prefix = schedule_part[:3].lower()
            first_four_prefix = schedule_part[:4].lower()
            cron_kw = {}
            if first_three_prefix in CRON_DAYS_OF_WEEK:
                cron_kw['day_of_week'] = schedule_part
            elif first_three_prefix in CRON_MONTHS:
                cron_kw['month'] = schedule_part
            elif is_int(first_four_prefix) and len(first_four_prefix) == 4:
                cron_kw['year'] = schedule_part
            if cron_kw:
                trigger = _CronTrigger(
                    starting_ts,
                    **cron_kw,
                    hour='*',
                    minute='*' if has_minutes else str(starting_ts.minute),
                    second='*' if has_seconds else str(starting_ts.second),
                )
            else:
                cron_parts = schedule_part.split()
                if len(cron_parts) != 5:
                    raise ValueError(f"Invalid cron schedule '{schedule_part}'.")
                trigger = _CronTrigger(
                    starting_ts,
                    minute=cron_parts[0], hour=cron_parts[1], day=cron_parts[2],
                    month=cron_parts[3], day_of_week=cron_parts[4],
                )
        triggers.append(trigger)

    if len(triggers) == 1:
        return triggers[0]
    return _OrTrigger(triggers) if join_str == '|' else _AndTrigger(triggers)


def parse_start_time(schedule: str, now: Optional[datetime] = None) -> datetime:
    """Return the explicit starting datetime in ``schedule``, or ``now``."""
    from meerschaum.utils.dtypes import round_time
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
