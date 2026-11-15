#! /usr/bin/env python3
# vim:fenc=utf-8

"""
Generate the `meerschaum-jobs` schedule fixtures (`schedule.json`) from
`meerschaum.utils.schedule.parse_schedule` (the `-s` / `--schedule` grammar).

    MRSM_PLUGINS_DIR=$(mktemp -d) scripts/parity/run.sh \
        crates/meerschaum-jobs/tests/fixtures/gen_schedule.py

or against the working tree:

    PYTHONPATH=$(pwd) python crates/meerschaum-jobs/tests/fixtures/gen_schedule.py

v4.1.4 implements its own triggers (`_IntervalTrigger`, `_CalendarIntervalTrigger`,
`_CronTrigger`, `_OrTrigger`, `_AndTrigger`); APScheduler is no longer used.

The corpus is synthetic: every frequency form (aliases, `every N <unit>`, weekday / month / year
names, ranges, five-field cron), `and` / `or` combinations, `starting` / `beginning` clauses
(relative, `today` / `tomorrow`, absolute with and without offsets), case variants and invalid
inputs, each evaluated at several fixed `now` values. For each case it records either the error
message or the first 10 fire times (`trigger.next()`), plus `next_after()` from a fresh trigger
for an `after` later than `now` and one earlier than the start.

Starting clauses which depend on the real current date (a bare time such as `starting 10:00`,
which dateutil fills in with today's date) are not part of the corpus.

The process timezone is pinned: the main corpus runs under `TZ=UTC`, and the zone cases at the end
(`zone_corpus()`) each name their own `tz`, which the generator sets with `time.tzset()` around
that case and restores afterwards, so the output does not depend on the machine. They cover a
zone abbreviation after a time (dateutil reads either abbreviation of `time.tzname` as
`tzlocal()`, any other abbreviation with a warning as a naive time, which Meerschaum makes UTC)
and sequences of fire times across the 2027 daylight-saving changes of `America/New_York`
(spring forward 2027-03-14, fall back 2027-11-07), where v4's triggers keep the local wall clock.
A case records its `tz`, its own `num_fires` when it is not the default, and the `warnings` the
parse raised.
"""

import itertools
import json
import os
import pathlib
import sys
from datetime import datetime, timedelta, timezone

HERE = pathlib.Path(__file__).resolve().parent
REPO = HERE.parent.parent.parent.parent
OUT = HERE / 'schedule.json'
NUM_FIRES = 10

NOWS = [
    datetime(2025, 3, 14, 15, 26, 53, 589793, tzinfo=timezone.utc),
    datetime(2024, 1, 31, 23, 59, 59, 999999, tzinfo=timezone.utc),
    datetime(2025, 12, 31, 12, 0, 0, tzinfo=timezone.utc),
]

FREQUENCIES = [
    'daily', 'hourly', 'minutely', 'weekly', 'monthly', 'yearly', 'secondly',
    'Daily', 'EVERY 5 MINUTES',
    'every 1 minute', 'every 5 minutes', 'every 5 minute', 'every 15 minutes', 'every 90 seconds',
    'every 10 seconds', 'every 0.5 minutes', 'every 1.5 hours', 'every 1 hours', 'every 6 hours',
    'every 2 days', 'every 1 day', 'every 3 weeks', 'every 1 week', 'every 1 month',
    'every 2 months', 'every 13 months', 'every 1 year', 'every 2 years', 'every 0.25 days',
]

WEEKDAYS = [
    'mon', 'monday', 'Monday', 'tue', 'tuesday', 'tues', 'wed', 'thu', 'thursday', 'thurs',
    'fri', 'sat', 'sun', 'sunday', 'mon-fri', 'monday through friday', 'monday thru friday',
    'monday - friday', 'tue,thu', 'wed-sat', 'sun-sat', 'mon-sun', 'tue/2', 'mon-fri/2',
]

MONTHS = ['jan', 'january', 'march', 'jun-aug', 'dec', 'feb', 'september', 'jan,jul', 'oct-dec']

YEARS = ['2026', '2025-2030', '2030']

CRONS = [
    '0 9 * * *', '*/15 * * * *', '30 2 1 * *', '0 0 * * mon', '0 12 1,15 * *', '0 0 13 * fri',
    '5 4 * * sun', '0 0 1 jan *', '*/5 9-17 * * mon-fri', '0 0 29 2 *', '0 22 * * 1-5',
    '0 0 * * 7', '0 0 31 * *', '0 0 * * 0/2', '0 0 * * */2', '1-10/3 * * * *', '59 23 * * *',
    '0 */6 * * *', '15,45 * * * *', '0 0 1 */3 *', '0 0 1-7 * mon', '0 0 * jan-mar sat',
    '0 8 * * 0', '0 0 15 * 5', '* * * * *', '0 0 1 1 *', '0 12 * * 6-7', '0 0 30 1/2 *',
]

COMBOS = [
    'daily and mon', 'every 1 hour and mon-fri', 'every 10 minutes and sat', 'mon or wed',
    'every 2 hours or every 3 hours', 'daily and every 12 hours', 'every 5 minutes or daily',
    'every 30 seconds and tue', 'hourly and monday through friday', 'weekly or monthly',
    'every 1 hour and every 90 minutes', 'mon or fri or sun', 'daily and monthly',
    'every 6 hours and jan', 'every 1 day and 2026', 'sat or sun and daily',
    'every 15 minutes or 0 9 * * *', 'every 1 hour or 30 * * * *',
    'daily or every 1 hour and mon',
]

ERRORS = [
    '', 'every 0 minutes', 'every 1.5 months', 'every 5 fortnights', 'every five minutes',
    'every -1 days', 'every 1', '60 * * * *', '* * * *', '0 0 L * *', 'sat-sun', 'mon, wed',
    'whenever', '0 0 32 * *', '0 24 * * *', '0 0 * 13 *', '0 0 * * 8', '*/0 * * * *',
    'tue/0', 'every 1 month and every 1.5 months',
]

STARTING_BASES = [
    'daily', 'every 5 minutes', 'hourly', 'mon-fri', '0 9 * * *', 'monthly',
    'every 1 hour and mon-fri', 'yearly', 'every 10 seconds or every 7 minutes',
]

STARTINGS = [
    'starting in 5 minutes', 'starting in 2 hours', 'starting in 1 day', 'starting in 0.5 hours',
    'starting in 3 weeks', 'starting in 90 seconds', 'starting tomorrow',
    'starting tomorrow 10:00', 'starting today 08:30', 'starting tomorrow 7pm',
    'starting today 23:59:59', 'starting 2025-01-01', 'starting 2025-03-20 06:00',
    'starting 2025-03-20T06:00:00+02:00', 'starting 2025-03-20 06:00:00 UTC',
    'starting 2025-03-20T06:00:00Z', 'beginning 2025-04-01', 'Starting 2025-04-01 12:30',
    'starting 2025-01-31', 'starting 2024-02-29', 'starting 2026-01-01 00:00:00.250000',
    'starting 2025-06-15 18:45:30-05:00', 'starting now', 'BEGINNING 2025-05-05',
    'starting 2025/03/20', 'starting 2025-03-20 6:05', 'starting 2025-03-20 06:00 PM',
    'starting yesterday', 'starting in x days', 'starting in 2 fortnights',
    'starting 2025-03-20 25:00', 'starting Tomorrow', 'starting 2025-02-30',
]


def corpus():
    exprs = []
    exprs += FREQUENCIES + WEEKDAYS + MONTHS + YEARS + CRONS + COMBOS + ERRORS
    exprs += [f'{base} {start}' for base, start in itertools.product(STARTING_BASES, STARTINGS)]
    seen = set()
    out = []
    for expr in exprs:
        if expr not in seen:
            seen.add(expr)
            out.append(expr)
    return out


SPRING_NOW = datetime(2027, 3, 12, 12, 0, 0, tzinfo=timezone.utc)
FALL_NOW = datetime(2027, 11, 5, 12, 0, 0, tzinfo=timezone.utc)


def zone_corpus():
    """`(tz, schedule, now, num_fires)` cases: first fire times of zone abbreviations and fire
    sequences across daylight-saving changes. Starting dates are always explicit."""
    cases = []
    new_york = 'America/New_York'

    ### Zone abbreviations and offsets after a time, in zones with and without those names.
    forms = [
        '2026-07-01 21:00 EDT', '2026-07-01 21:00 EST', '2026-01-01 21:00 EDT',
        '2026-01-01 21:00 EST', '2026-07-01 21:00 PDT', '2026-07-01 21:00 UTC',
        '2026-07-01 21:00 GMT', '2026-07-01 21:00 Z', '2026-07-01 21:00 z',
        '2026-07-01 21:00Z', '2026-07-01 21:00EDT', '2026-07-01T21:00:00 EDT',
        '2026-07-01 21:00 edt', '2026-07-01 21:00 Edt', '2026-07-01 21:00 utc',
        '2026-07-01 21:00 gmt', '2026-07-01 21:00 ABCDE', '2026-07-01 21:00 ABCDEF',
        '2026-07-01 21:00 E', '2026-07-01 21:00 CEST', '2026-07-01 21:00 JST',
        '2026-07-01 21:00 -04:00', '2026-07-01 21:00-0400', '2026-07-01 21:00 -04',
        '2026-07-01 21:00 +5:30', '2026-07-01 21:00 +530', '2026-07-01 21:00 UTC+3',
        '2026-07-01 21:00 UTC-3', '2026-07-01 21:00 UTC+03:00', '2026-07-01 21:00 GMT+3',
        '2026-07-01 21:00 EDT+3', '2026-07-01 21:00 EDT +3', '2026-07-01 21:00 PDT-3',
        '2026-07-01 21:00 UTC+0', '2026-07-01 21:00 UTC +3', '2026-07-01 21:00 -0400 (EDT)',
        '2026-07-01 21:00 -0400 (PDT)', '2026-07-01 21:00 -0400 EDT',
        '2026-07-01 21:00 EDT EST', '2026-07-01 21:00 EDT-', '2026-07-01 21:00 EDT.',
        '2026-07-01 21:00 EDT at', '2026-07-01 9pm EDT', '2026-07-01 9 pm EDT',
        '2026-07-01 9PM EDT', '2026-07-01 9PM edt', '2026-07-01 9am Z',
        '2026-07-01 21:00:30.25 EDT', '2026-07-01 EDT', '2026-07-01Z', '2026-07-01 UTC',
        '2026-07-01 21:00 UTC (EDT)', '2026/07/01 21:00 EDT', '2026-07-01t21:00 EDT',
        '2026-03-08 02:30 EDT', '2026-03-08 02:30 EST', '2026-11-01 01:30 EDT',
        '2026-11-01 01:30 EST', '2026-11-01 01:30',
        'tomorrow 21:00 EDT', 'tomorrow 21:00 PDT', 'tomorrow 21:00 ABCDEF', 'today 9pm PST',
        'tomorrow 21:00 GMT+3',
    ]
    zones = [new_york, 'UTC', 'Europe/London', 'Australia/Sydney', 'Asia/Kolkata']
    for tz in zones:
        for form in forms:
            cases.append((tz, f'daily starting {form}', NOWS[0], 4))
    for tz in (new_york, 'UTC'):
        for freq in ['every 1 day', 'every 5 minutes', 'monthly', 'mon', 'mon-fri', '0 9 * * *',
                     'daily and mon', 'daily or mon', 'yearly']:
            cases.append((tz, f'{freq} starting 2026-07-01 21:00 EDT', NOWS[0], 4))
    cases.append((new_york, 'every day starting 2026-07-01 21:00 EDT', NOWS[0], 4))
    cases.append((new_york, '2026-07-01 21:00 EDT', NOWS[0], 4))
    cases.append((new_york, 'beginning 2026-07-01 21:00 EDT', NOWS[0], 4))
    cases.append((new_york, 'daily Starting 2026-07-01 21:00 EDT', NOWS[0], 4))

    ### Fire sequences across the daylight-saving changes (spring forward 2027-03-14, fall back
    ### 2027-11-07), each with the zone's abbreviation, an explicit offset, and under TZ=UTC.
    def sequences(suffixes):
        spring = [
            ('daily starting 2027-03-12 21:00 {z}', 10),
            ('daily starting 2027-03-12 02:30 {z}', 10),
            ('daily starting 2027-03-12 01:30 {z}', 10),
            ('weekly starting 2027-03-01 21:00 {z}', 10),
            ('every 12 hours starting 2027-03-13 09:00 {z}', 10),
            ('every 2 days starting 2027-03-10 21:00 {z}', 10),
            ('hourly starting 2027-03-13 22:30 {z}', 12),
            ('monthly starting 2027-02-20 21:00 {z}', 10),
            ('monthly starting 2027-02-14 02:30 {z}', 10),
            ('yearly starting 2026-03-14 02:30 {z}', 4),
            ('30 2 * * * starting 2027-03-12 00:00 {z}', 8),
            ('0 21 * * * starting 2027-03-12 00:00 {z}', 8),
            ('mon-fri starting 2027-03-12 21:00 {z}', 10),
            ('daily and mon-fri starting 2027-03-11 21:00 {z}', 8),
            ('every 1 day and mon starting 2027-03-08 21:00 {z}', 8),
            ('daily or mon starting 2027-03-12 21:00 {z}', 12),
        ]
        fall = [
            ('daily starting 2027-11-05 21:00 {z}', 10),
            ('daily starting 2027-11-05 02:30 {z}', 10),
            ('daily starting 2027-11-05 01:30 {z}', 10),
            ('weekly starting 2027-10-25 21:00 {z}', 10),
            ('every 12 hours starting 2027-11-06 09:00 {z}', 10),
            ('every 2 days starting 2027-11-03 21:00 {z}', 10),
            ('hourly starting 2027-11-06 22:30 {z}', 12),
            ('monthly starting 2027-10-07 01:30 {z}', 10),
            ('30 1 * * * starting 2027-11-05 00:00 {z}', 8),
            ('mon-fri starting 2027-11-05 21:00 {z}', 10),
            ('daily and mon-fri starting 2027-11-04 21:00 {z}', 8),
        ]
        out = []
        for tz, z in suffixes:
            for template, n in spring:
                out.append((tz, template.format(z=z), SPRING_NOW, n))
            for template, n in fall:
                out.append((tz, template.format(z=z), FALL_NOW, n))
        return out

    cases += sequences([(new_york, 'EST'), (new_york, 'EDT'), (new_york, '-05:00'), ('UTC', 'EST')])
    cases.append((new_york, 'hourly starting 2027-03-13 00:30 EST', SPRING_NOW, 30))
    cases.append((new_york, 'hourly starting 2027-11-06 00:30 EST', FALL_NOW, 30))
    return cases


def iso(dt):
    return dt.isoformat() if dt is not None else None


def evaluate(parse_schedule, expr, now, num_fires=NUM_FIRES):
    case = {'schedule': expr, 'now': now.isoformat()}
    try:
        trigger = parse_schedule(expr, now=now)
        case['type'] = type(trigger).__name__
        case['fires'] = [iso(trigger.next()) for _ in range(num_fires)]
        later = now + timedelta(hours=3, minutes=7, seconds=11, microseconds=500000)
        earlier = now - timedelta(days=400)
        case['next_after'] = []
        for after in (later, earlier):
            trigger = parse_schedule(expr, now=now)
            first = trigger.next_after(after)
            case['next_after'].append({
                'after': after.isoformat(),
                'fires': [iso(first)] + [iso(trigger.next()) for _ in range(2)],
            })
    except Exception as e:
        case['error'] = {'type': type(e).__name__, 'message': str(e)}
    return case


def main():
    if not os.environ.get('MRSM_PARITY_REFERENCE'):
        sys.path.insert(0, str(REPO))
    import warnings
    warnings.simplefilter('ignore')
    import meerschaum as mrsm
    from meerschaum.utils.schedule import parse_schedule
    if os.environ.get('MRSM_PARITY_REFERENCE'):
        assert mrsm.__version__ == os.environ['MRSM_PARITY_REFERENCE'], mrsm.__version__

    ### `warn()` prints to stderr for unparseable starting times; keep the output quiet.
    import time
    import meerschaum.utils.schedule as schedule_module
    schedule_module.warn = lambda *args, **kwargs: None

    def pin_tz(name):
        """Set the process zone (`time.tzname`, `tzlocal()`) and return the one to restore."""
        previous = os.environ.get('TZ')
        os.environ['TZ'] = name
        time.tzset()
        return previous

    def restore_tz(previous):
        if previous is None:
            os.environ.pop('TZ', None)
        else:
            os.environ['TZ'] = previous
        time.tzset()

    def parse_recording(expr, now):
        """`parse_schedule` for the first call of a case, recording dateutil's warnings."""
        with warnings.catch_warnings(record=True) as caught:
            warnings.simplefilter('always')
            try:
                return schedule_module.parse_schedule(expr, now=now), caught
            except Exception:
                return None, caught

    cases = []
    previous = pin_tz('UTC')
    try:
        for expr in corpus():
            for now in NOWS:
                cases.append(evaluate(parse_schedule, expr, now))
        for tz, expr, now, num_fires in zone_corpus():
            pin_tz(tz)
            case = evaluate(parse_schedule, expr, now, num_fires)
            case['tz'] = tz
            if num_fires != NUM_FIRES:
                case['num_fires'] = num_fires
            _, caught = parse_recording(expr, now)
            case['warnings'] = [str(w.message) for w in caught]
            if not case['warnings']:
                del case['warnings']
            cases.append(case)
    finally:
        restore_tz(previous)

    data = {'num_fires': NUM_FIRES, 'cases': cases}
    OUT.write_text(json.dumps(data, indent=1, ensure_ascii=False) + '\n', encoding='utf-8')
    num_errors = sum(1 for case in cases if 'error' in case)
    print(f"Wrote {OUT.relative_to(REPO)} ({len(cases)} cases, {num_errors} errors).")


if __name__ == '__main__':
    main()
