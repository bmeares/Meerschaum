#! /usr/bin/env python3
# vim:fenc=utf-8

"""
`--loop` for any action (rc15, Q23): a plugin action repeats like `sync pipes --loop`, so a
plugin does not need its own `while True` loop, and the job reports its laps (`job.health`).
Each lap calls the action once with `loop=False`; an action whose function takes `loop` keeps
4.1's contract (it receives `loop=True` and repeats by itself). A plugin action's `-s` schedule
runs in the host, which records each run.
"""

import os
import pathlib
import subprocess
import sys

REPO = pathlib.Path(__file__).resolve().parent.parent

PLUGIN = (
    "import time\n"
    "from meerschaum.plugins import make_action\n"
    "__version__ = '0.0.1'\n"
    "@make_action\n"
    "def e2loop(**kw):\n"
    "    print(f\"E2LOOP lap loop={kw.get('loop')}\", flush=True)\n"
    "    return True, 'lap done'\n"
    "@make_action\n"
    "def e2self(loop: bool = False, **kw):\n"
    "    count = 0\n"
    "    while True:\n"
    "        count += 1\n"
    "        print(f'E2SELF iteration {count} loop={loop}', flush=True)\n"
    "        if not loop:\n"
    "            break\n"
    "        time.sleep(0.2)\n"
    "    return True, f'Ran {count} times.'\n"
    "@make_action\n"
    "def e2tick(**kw):\n"
    "    print('E2TICK run', flush=True)\n"
    "    return True, 'ticked'\n"
)


def run_job(tmp_path, sysargs, until, name='e2-job'):
    """Start a job running `sysargs`, wait for `until` (Python source over `logs` and `health`),
    then print its logs' markers and health."""
    code = (
        "import time\n"
        "import meerschaum as mrsm\n"
        f"job = mrsm.Job({name!r}, {sysargs!r}, executor_keys='local')\n"
        "assert job.start()[0]\n"
        "try:\n"
        "    deadline = time.time() + 120\n"
        "    while time.time() < deadline:\n"
        "        logs, health = job.get_logs() or '', job.health\n"
        f"        if {until}:\n"
        "            break\n"
        "        time.sleep(0.25)\n"
        "    logs, health = job.get_logs() or '', job.health\n"
        "    for line in logs.splitlines():\n"
        "        if 'E2' in line:\n"
        "            print('LOG', line.strip(), flush=True)\n"
        "    print('HEALTH', health['state'], '|', health['message'], flush=True)\n"
        "finally:\n"
        "    job.stop()\n"
        "    job.delete()\n"
    )
    (tmp_path / 'root').mkdir(exist_ok=True)
    plugins = tmp_path / 'plugins'
    plugins.mkdir(exist_ok=True)
    (plugins / 'e2loop.py').write_text(PLUGIN)
    env = {
        key: value
        for key, value in os.environ.items()
        if not key.startswith('MRSM_') and key not in ('TMUX', 'TMUX_PANE')
    }
    env.update({
        'PYTHONPATH': str(REPO),
        'MRSM_ROOT_DIR': str(tmp_path / 'root'),
        'MRSM_PLUGINS_DIR': str(plugins),
        'MRSM_CONFIG': '{"jobs": {"startup": {"enabled": false}}}',
    })
    result = subprocess.run(
        [sys.executable, '-c', code], capture_output=True, text=True, env=env, timeout=240,
    )
    assert result.returncode == 0, result.stderr[-3000:]
    return result.stdout, result.stdout + result.stderr[-2000:]


def test_a_plugin_action_loops_in_a_job_and_reports_its_laps(tmp_path):
    stdout, context = run_job(
        tmp_path,
        ['e2loop', '--loop', '--min-seconds', '0.2'],
        "logs.count('E2LOOP lap') >= 3 and health['state'] == 'healthy'",
    )
    laps = [line for line in stdout.splitlines() if 'E2LOOP lap' in line]
    assert len(laps) >= 3, context
    # Each lap runs the action once: it never sees `loop=True`.
    assert all('loop=False' in line for line in laps), context
    assert 'HEALTH healthy | last good run' in stdout, context


def test_a_plugin_action_that_takes_loop_repeats_by_itself(tmp_path):
    stdout, context = run_job(
        tmp_path,
        ['e2self', '--loop', '--min-seconds', '0.2'],
        "logs.count('E2SELF iteration') >= 4",
    )
    iterations = [line for line in stdout.splitlines() if 'E2SELF iteration' in line]
    assert len(iterations) >= 4, context
    # One call that loops by itself (4.1's contract), not laps of single runs.
    assert all('loop=True' in line for line in iterations), context
    assert 'E2SELF iteration 4 loop=True' in stdout, context
    assert 'HEALTH stalled' not in stdout, context


def test_a_scheduled_plugin_action_records_each_run(tmp_path):
    stdout, context = run_job(
        tmp_path,
        ['e2tick', '-s', 'every 1 second'],
        "logs.count('E2TICK run') >= 2 and health['state'] == 'healthy'",
    )
    runs = [line for line in stdout.splitlines() if 'E2TICK run' in line]
    assert len(runs) >= 2, context
    # The host runs the schedule and records each run (the worker no longer schedules again
    # inside the first run).
    assert 'HEALTH healthy | last good run' in stdout, context
