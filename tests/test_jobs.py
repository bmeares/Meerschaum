#! /usr/bin/env python3
# vim:fenc=utf-8

"""
Test Meerschaum jobs.
"""

import time
from types import SimpleNamespace
import pytest

import meerschaum as mrsm


def _exit_target(code):
    raise SystemExit(code)


@pytest.mark.parametrize('code,success', [(None, True), (0, True), (3, False)])
def test_daemon_system_exit_result(code, success):
    from meerschaum.utils.daemon import Daemon
    daemon = Daemon(target=_exit_target, target_args=[code],
                    properties={'logs': {'stdin': False}})
    try:
        ok, msg = daemon.run()
        assert ok, msg
        deadline = time.monotonic() + 15
        while time.monotonic() < deadline:
            properties = daemon.read_properties() or {}
            if properties.get('result') is not None:
                break
            time.sleep(0.05)
        result = properties.get('result')
        assert result is not None, properties
        assert result[0] is success, result
        if success:
            assert 'stopped' in result[1].lower()
            assert 'Traceback' not in result[1]
        else:
            assert 'SystemExit: 3' in result[1]
    finally:
        if daemon.status != 'stopped':
            daemon.kill(timeout=3)
        daemon.cleanup()


def test_stop_warning_preserves_quit_failure(capsys):
    job = SimpleNamespace(executor=None, daemon=SimpleNamespace(
        status='running', quit=lambda **kw: (False, 'Timed out waiting for SIGINT.'),
        kill=lambda **kw: (True, 'Killed.')))
    assert mrsm.Job.stop(job)[0]
    assert 'Timed out waiting for SIGINT.' in capsys.readouterr().err


def test_create_job():
    """
    Test creating and running a new job.
    """
    sysargs = ['show', 'version', ':', '--loop', '--min-seconds', '0.1']
    job = mrsm.Job('test', sysargs, executor_keys='local')
    job.delete()
    success, msg = job.start()
    assert success, msg

    time.sleep(2.0)

    success, msg = job.stop()
    assert success, msg
    time.sleep(1.0)

    success, msg = job.result
    assert success, msg

    output_text = job.get_logs()
    assert output_text.count("Meerschaum v") > 1
