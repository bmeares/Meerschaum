"""
A native plugin's job executor through the Python façade: `mrsm.Job(..., executor_keys='procs:<label>')`
starts, reports, logs, stops and deletes a job run by the SDK example `native_executor`
(`crates/meerschaum-plugin/examples/native_executor.rs`), which starts each job with the command
the host gives it (`python -P -m meerschaum` here, not the interpreter alone).

The calls run in a subprocess with its own root and plugins directory (the native plugin is not
one of `tests/plugins`). Unix only (the example executor detaches jobs with `sh`).
"""

import json
import os
import pathlib
import shutil
import subprocess
import sys
import textwrap

import pytest

REPO = pathlib.Path(__file__).resolve().parent.parent

pytestmark = pytest.mark.skipif(sys.platform == 'win32', reason="the example executor uses `sh`")


def _example_library(name: str) -> pathlib.Path:
    """Build the SDK example (once) and return its library."""
    cargo = shutil.which('cargo') or os.path.expanduser('~/.cargo/bin/cargo')
    subprocess.run(
        [cargo, 'build', '-p', 'meerschaum-plugin', '--example', name],
        cwd=REPO,
        check=True,
        capture_output=True,
    )
    suffix = {'darwin': 'dylib'}.get(sys.platform, 'so')
    return REPO / 'target' / 'debug' / 'examples' / f'lib{name}.{suffix}'


SCRIPT = textwrap.dedent(
    """
    import json, sys, time
    import meerschaum as mrsm

    out = {}
    job = mrsm.Job('pyjob', ['show', 'version', '-s', 'every 1 seconds'], executor_keys='procs:py')
    out['exists_before'] = job.exists()
    out['start'] = list(job.start())
    out['status'] = job.status
    deadline = time.time() + 60
    while time.time() < deadline and (job.get_logs() or '').count('Meerschaum v') < 2:
        time.sleep(0.5)
    out['logs'] = job.get_logs() or ''
    executor = job.executor
    out['executor'] = [type(executor).__name__, str(executor)]
    out['executor_names'] = executor.get_job_names()
    out['executor_status'] = executor.get_job_status('pyjob')
    out['pause'] = list(job.pause())
    out['stop'] = list(job.stop())
    out['status_after_stop'] = job.status
    out['delete'] = list(job.delete())
    out['exists_after'] = job.exists()
    print('RESULT ' + json.dumps(out))
    """
)


def test_python_job_on_native_executor(tmp_path):
    library = _example_library('native_executor')
    root = tmp_path / 'root'
    plugins = tmp_path / 'plugins' / 'native_executor'
    root.mkdir()
    plugins.mkdir(parents=True)
    (plugins / 'plugin.toml').write_text('name = "native_executor"\nversion = "1.0.0"\n')
    shutil.copy(library, plugins / library.name)

    env = {
        key: value
        for key, value in os.environ.items()
        if not key.startswith('MRSM_') and key not in ('TMUX', 'TMUX_PANE')
    }
    env.update({
        'PYTHONPATH': str(REPO),
        'MRSM_ROOT_DIR': str(root),
        'MRSM_PLUGINS_DIR': str(tmp_path / 'plugins'),
        'MRSM_NO_UPDATE_CHECK': '1',
        'MRSM_NOASK': 'true',
    })
    proc = subprocess.run(
        [sys.executable, '-c', SCRIPT],
        env=env,
        capture_output=True,
        text=True,
        timeout=180,
    )
    lines = [line for line in proc.stdout.splitlines() if line.startswith('RESULT ')]
    assert lines, proc.stdout + proc.stderr
    result = json.loads(lines[-1][len('RESULT '):])

    assert result['exists_before'] is False
    assert result['start'][0] is True, result
    assert result['status'] == 'running', result
    assert result['logs'].count('Meerschaum v') >= 2, result
    # `Job.executor` is the native façade, not a failed Python connector lookup.
    assert result['executor'] == ['NativeExecutor', 'procs:py'], result
    assert result['executor_names'] == ['pyjob'], result
    assert result['executor_status'] == 'running', result
    assert 'Cannot create Connector' not in proc.stdout + proc.stderr
    assert result['pause'] == [
        False, "`pause_job` is not supported by the 'procs:py' executor."
    ], result
    assert result['stop'][0] is True, result
    assert result['status_after_stop'] == 'stopped', result
    assert result['delete'][0] is True, result
    assert result['exists_after'] is False, result
    assert not (root / 'procs_jobs' / 'py' / 'pyjob').exists()
