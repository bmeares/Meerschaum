#! /usr/bin/env python3
# vim:fenc=utf-8

"""
A zone abbreviation after a `starting` time, as 4.x reads it with `dateutil`: either abbreviation
of the process's own zone (`EST` and `EDT` in `America/New_York`) is the local zone, `UTC`, `GMT`
and `Z` are UTC, any other abbreviation is accepted with dateutil's warning and read as UTC, and
lowercase names are an error. The grammar is `crates/meerschaum-jobs/src/schedule.rs`; the
expected values are the ones v4.1.4 returns (`crates/meerschaum-jobs/tests/fixtures/schedule.json`).
This is the `-s` / `--schedule` end of it (`mrsm show schedules` parses the schedule the way a
scheduled job does).
"""

import os
import subprocess
import sys

import pytest

REPO = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


def _show_schedules(schedule: str, tz: str, tmp_path, count: int = 2):
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
        [sys.executable, '-m', 'meerschaum', 'show', 'schedules', schedule, str(count), '--nopretty'],
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
    result = _show_schedules(schedule, 'America/New_York', tmp_path)
    assert 'Unknown string format' in result.stdout + result.stderr
