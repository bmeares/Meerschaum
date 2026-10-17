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


def iso(dt):
    return dt.isoformat() if dt is not None else None


def evaluate(parse_schedule, expr, now):
    case = {'schedule': expr, 'now': now.isoformat()}
    try:
        trigger = parse_schedule(expr, now=now)
        case['type'] = type(trigger).__name__
        case['fires'] = [iso(trigger.next()) for _ in range(NUM_FIRES)]
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
    import meerschaum.utils.schedule as schedule_module
    schedule_module.warn = lambda *args, **kwargs: None

    cases = []
    for expr in corpus():
        for now in NOWS:
            cases.append(evaluate(parse_schedule, expr, now))

    data = {'num_fires': NUM_FIRES, 'cases': cases}
    OUT.write_text(json.dumps(data, indent=1, ensure_ascii=False) + '\n', encoding='utf-8')
    num_errors = sum(1 for case in cases if 'error' in case)
    print(f"Wrote {OUT.relative_to(REPO)} ({len(cases)} cases, {num_errors} errors).")


if __name__ == '__main__':
    main()
