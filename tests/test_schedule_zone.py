#! /usr/bin/env python3
# vim:fenc=utf-8

"""
A zone abbreviation after a `starting` time, as 4.x reads it with `dateutil`: either abbreviation
of the process's own zone (`EST` and `EDT` in `America/New_York`) is the local zone, `UTC`, `GMT`
and `Z` are UTC, any other abbreviation is accepted with dateutil's warning and read as UTC, and
lowercase names are an error. The grammar is `crates/meerschaum-jobs/src/schedule/`; the
expected values are the ones v4.1.4 returns (`crates/meerschaum-jobs/tests/fixtures/schedule.json`).
This is the `-s` / `--schedule` end of it (`mrsm show schedules` parses the schedule the way a
scheduled job does).
"""

import os
import subprocess
import sys

import pytest

REPO = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


def _show_schedules(schedule: str, tz: str, tmp_path, count: int = 2, nopretty: bool = True):
    root = tmp_path / 'root'
    plugins = tmp_path / 'plugins'
    root.mkdir(exist_ok=True)
    plugins.mkdir(exist_ok=True)
    env = {
        key: val for key, val in os.environ.items()
        if not key.startswith('MRSM_') and key not in ('PYTHONPATH', 'TMUX', 'TMUX_PANE', 'TZ')
    }
    env.update({
        'TZ': tz,
        'PYTHONPATH': REPO,
        'MRSM_ROOT_DIR': str(root),
        'MRSM_PLUGINS_DIR': str(plugins),
        'MRSM_NO_AUTO_INSTALL': '1',
        'MRSM_NO_UPDATE_CHECK': '1',
    })
    return subprocess.run(
        [sys.executable, '-m', 'meerschaum', 'show', 'schedules', schedule, str(count)]
        + (['--nopretty'] if nopretty else []),
        capture_output=True, text=True, env=env, timeout=120, cwd=str(root),
    )


@pytest.mark.parametrize('schedule, first_fire', [
    ### Either abbreviation of the local zone is the local zone: the offset follows the date.
    ('daily starting 2026-07-01 21:00 EDT', '2026-07-01 21:00:00-04:00'),
    ('daily starting 2026-07-01 21:00 EST', '2026-07-01 21:00:00-04:00'),
    ('daily starting 2026-01-05 21:00 EDT', '2026-01-05 21:00:00-05:00'),
    ('daily starting 2026-01-05 21:00 EST', '2026-01-05 21:00:00-05:00'),
    ('every 1 day starting 2026-07-01 21:00 EDT', '2026-07-01 21:00:00-04:00'),
    ('daily and mon starting 2026-07-06 21:00 EDT', '2026-07-06 21:00:00-04:00'),
    ('mon starting 2026-07-06 21:00 EST', '2026-07-06 21:00:00-04:00'),
    ### UTC names, with dateutil's reversed sign after them.
    ('daily starting 2026-07-01 21:00 UTC', '2026-07-01 21:00:00+00:00'),
    ('daily starting 2026-07-01 21:00 GMT', '2026-07-01 21:00:00+00:00'),
    ('daily starting 2026-07-01 21:00Z', '2026-07-01 21:00:00+00:00'),
    ('daily starting 2026-07-01 21:00 UTC+3', '2026-07-01 21:00:00-03:00'),
    ('daily starting 2026-07-01 21:00 -0400', '2026-07-01 21:00:00-04:00'),
])
def test_zone_abbreviations_read_like_dateutil(schedule, first_fire, tmp_path):
    result = _show_schedules(schedule, 'America/New_York', tmp_path)
    assert result.returncode == 0, result.stdout + result.stderr
    assert first_fire in result.stdout, result.stdout
    assert 'identified but not understood' not in result.stderr


@pytest.mark.parametrize('tz, schedule, abbreviation', [
    ### An abbreviation that is not one of the zone's: a warning, read as UTC.
    ('America/New_York', 'daily starting 2026-07-01 21:00 PDT', 'PDT'),
    ### EST is not an abbreviation of UTC: the same warning.
    ('UTC', 'daily starting 2026-07-01 21:00 EST', 'EST'),
    ('UTC', 'daily starting 2026-07-01 21:00 EDT', 'EDT'),
])
def test_other_abbreviations_warn_and_read_utc(tz, schedule, abbreviation, tmp_path):
    result = _show_schedules(schedule, tz, tmp_path)
    assert result.returncode == 0, result.stdout + result.stderr
    assert '2026-07-01 21:00:00+00:00' in result.stdout, result.stdout
    assert f'tzname {abbreviation} identified but not understood' in result.stderr


@pytest.mark.parametrize('schedule', [
    'daily starting 2026-07-01 21:00 edt',
    'daily starting 2026-07-01 21:00 Edt',
    'daily starting 2026-07-01 21:00 utc',
    'daily starting 2026-07-01 21:00 ABCDEF',
])
def test_lowercase_and_overlong_zone_names_are_an_error(schedule, tmp_path):
    ### `--nopretty` prints no failure message.
    result = _show_schedules(schedule, 'America/New_York', tmp_path, nopretty=False)
    assert result.returncode != 0
    assert 'Unknown string format' in result.stdout + result.stderr


### The `tz <zone>` clause (v5): the schedule steps on that zone's wall clock. The expected values
### are written by hand (v4 has no such clause); see `crates/meerschaum-jobs/tests/integration/schedule_zone.rs`.

@pytest.mark.parametrize('tz, schedule, fires', [
    ### `tz local` is the process's zone (`TZ`).
    ('America/New_York', 'daily starting 2027-03-13 21:00 tz local',
     ['2027-03-13 21:00:00-05:00', '2027-03-14 21:00:00-04:00']),
    ('Asia/Tokyo', 'daily starting 2027-03-13 21:00 tz local jitter 0',
     ['2027-03-13 21:00:00+09:00', '2027-03-14 21:00:00+09:00']),
    ### A named zone does not depend on the process's zone.
    ('UTC', '0 21 * * mon-fri starting 2027-03-12 00:00 tz America/New_York',
     ['2027-03-12 21:00:00-05:00', '2027-03-15 21:00:00-04:00']),
    ('UTC', 'hourly starting 2027-11-07 00:30 tz america/new_york',
     ['2027-11-07 00:30:00-04:00', '2027-11-07 01:30:00-04:00', '2027-11-07 01:30:00-05:00']),
    ### `tz utc` is the default.
    ('America/New_York', 'daily starting 2027-03-13 21:00 tz utc',
     ['2027-03-13 21:00:00+00:00', '2027-03-14 21:00:00+00:00']),
    ### An abbreviation of the process's zone keeps the wall-clock time across the change, as 4.x.
    ('America/New_York', 'daily starting 2027-03-13 21:00 EST',
     ['2027-03-13 21:00:00-05:00', '2027-03-14 21:00:00-04:00']),
])
def test_tz_clause_in_show_schedules(tz, schedule, fires, tmp_path):
    result = _show_schedules(schedule, tz, tmp_path, count=len(fires))
    assert result.returncode == 0, result.stdout + result.stderr
    for fire in fires:
        assert fire in result.stdout, result.stdout
    assert result.stdout.index(fires[0]) < result.stdout.index(fires[-1])


@pytest.mark.parametrize('schedule, message', [
    ('daily tz Nowhere/Land', "Unknown time zone 'Nowhere/Land'"),
    ('daily tz local starting 21:00', "must come at the end"),
])
def test_tz_clause_errors(schedule, message, tmp_path):
    result = _show_schedules(schedule, 'UTC', tmp_path, nopretty=False)
    assert result.returncode != 0
    assert message in result.stdout + result.stderr, result.stdout + result.stderr


def test_python_helpers_accept_the_tz_clause():
    from datetime import datetime, timezone, timedelta
    from meerschaum.utils.schedule import parse_schedule, parse_start_time

    now = datetime(2027, 3, 1, tzinfo=timezone.utc)
    trigger = parse_schedule('daily starting 2027-03-13 21:00 tz America/New_York', now=now)
    assert trigger.zone == 'America/New_York'
    first, second = trigger.next(), trigger.next()
    assert first == datetime(2027, 3, 14, 2, tzinfo=timezone.utc)
    assert first.utcoffset() == timedelta(hours=-5)
    assert second == datetime(2027, 3, 15, 1, tzinfo=timezone.utc)
    assert second.utcoffset() == timedelta(hours=-4)
    assert trigger.next_after(first) == second
    ### A naive `after` is UTC.
    assert trigger.next_after(datetime(2027, 3, 14, 2)) == second

    jittered = parse_schedule('every 5 minutes starting 2027-03-13 21:00 jitter 0 tz Asia/Tokyo', now=now)
    assert jittered.next() == datetime(2027, 3, 13, 12, tzinfo=timezone.utc)

    start = parse_start_time('daily starting 2027-03-13 21:00 tz Asia/Tokyo', now=now)
    assert start == datetime(2027, 3, 13, 12, tzinfo=timezone.utc)

    with pytest.raises(ValueError, match='Unknown time zone'):
        parse_schedule('daily tz Nowhere/Land', now=now)

    ### Schedules without the v5 clauses keep 4.x's own triggers.
    assert type(parse_schedule('daily', now=now)).__name__ == '_IntervalTrigger'


def test_scheduled_job_fires_in_its_zone(tmp_path):
    """`mrsm <action> -s '<schedule> tz <zone>' -d`: the job runs on schedule and records its
    fire times with the zone's offset."""
    import json
    import time
    import uuid

    root = tmp_path / 'root'
    plugins = tmp_path / 'plugins'
    root.mkdir()
    plugins.mkdir()
    env = {
        key: val for key, val in os.environ.items()
        if not key.startswith('MRSM_') and key not in ('PYTHONPATH', 'TMUX', 'TMUX_PANE', 'TZ')
    }
    env.update({
        'TZ': 'UTC',
        'PYTHONPATH': REPO,
        'MRSM_ROOT_DIR': str(root),
        'MRSM_PLUGINS_DIR': str(plugins),
        'MRSM_NO_AUTO_INSTALL': '1',
        'MRSM_NO_UPDATE_CHECK': '1',
        ### A scratch instance: nothing here should connect to one.
        'MRSM_SQL_MAIN': f"sqlite:///{tmp_path / 'main.db'}",
    })
    name = f"tz-{uuid.uuid4().hex[:8]}"

    def mrsm(*args):
        return subprocess.run(
            [sys.executable, '-m', 'meerschaum', *args],
            capture_output=True, text=True, env=env, timeout=120, cwd=str(root),
        )

    started = mrsm(
        'show', 'version', '-s', 'every 2 seconds tz Asia/Tokyo',
        '--name', name, '-d', '--no-stagger', '-y',
    )
    try:
        assert started.returncode == 0, started.stdout + started.stderr
        state_dir = root / '.internal' / 'schedules'
        deadline = time.monotonic() + 60
        state = None
        while time.monotonic() < deadline:
            files = list(state_dir.glob('*.json')) if state_dir.exists() else []
            if files:
                state = json.loads(files[0].read_text())
                if state.get('next_fire'):
                    break
            time.sleep(0.2)
        assert state is not None and state.get('next_fire'), mrsm('show', 'logs', name, '--nopretty').stdout
        assert state['schedule'] == 'every 2 seconds tz Asia/Tokyo'
        assert state['next_fire'].endswith('+09:00'), state
        assert state['last_fire'].endswith('+09:00'), state
    finally:
        mrsm('delete', 'job', name, '-f', '-y')


@pytest.mark.skipif(sys.platform == 'win32', reason='requires POSIX TZ and tzset')
@pytest.mark.parametrize('abbreviation', ['EST', 'EDT'])
def test_python_local_abbreviation_honors_both_clock_changes(abbreviation, tmp_path):
    from datetime import datetime, timezone
    import time
    from meerschaum.utils.schedule import parse_schedule

    previous = os.environ.get('TZ')
    try:
        os.environ['TZ'] = 'America/New_York'
        time.tzset()
        cases = [
            ('daily starting 2027-03-13 02:30', ['2027-03-13T07:30', '2027-03-14T07:00']),
            ('hourly starting 2027-11-07 00:30',
             ['2027-11-07T04:30', '2027-11-07T05:30', '2027-11-07T06:30']),
        ]
        for stem, expected in cases:
            schedule = f'{stem} {abbreviation}'
            trigger = parse_schedule(schedule)
            instants = [trigger.next() for _ in expected]
            assert [t.astimezone(timezone.utc).isoformat(timespec='minutes') for t in instants] == [
                t + '+00:00' for t in expected
            ]
            cli = _show_schedules(schedule, 'America/New_York', tmp_path, count=len(expected))
            assert cli.returncode == 0, cli.stderr
            assert all(t.isoformat(sep=' ') in cli.stdout for t in instants)
        named = parse_schedule(f'daily starting July 1, 2027 09:00 {abbreviation}')
        assert named.next() == datetime(2027, 7, 1, 13, tzinfo=timezone.utc)
        between = datetime(2027, 11, 7, 6, tzinfo=timezone.utc)
        assert parse_schedule(schedule).next_after(between) == datetime(
            2027, 11, 7, 6, 30, tzinfo=timezone.utc,
        )
    finally:
        if previous is None:
            del os.environ['TZ']
        else:
            os.environ['TZ'] = previous
        time.tzset()
