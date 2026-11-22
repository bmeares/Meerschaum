"""A Rust CI cancellation or panic must not leave a detached test server running."""

import os
from pathlib import Path
import signal
import subprocess
import sys
import time

import pytest


REAPER = Path(__file__).resolve().parents[1] / 'scripts/ci/reap_test_processes.py'
pytestmark = pytest.mark.skipif(sys.platform != 'linux', reason='Linux CI process cleanup')


@pytest.mark.parametrize('cancel_signal', [None, signal.SIGINT, signal.SIGTERM])
def test_reaps_a_detached_descendant_and_preserves_an_unrelated_process(tmp_path, cancel_signal):
    pid_file = tmp_path / 'child.pid'
    signal_file = tmp_path / 'cancel.signal'
    server = '''
import os, pathlib, signal, sys, time
signal.signal(signal.SIGTERM, signal.SIG_IGN)
pathlib.Path(sys.argv[1]).write_text(str(os.getpid()))
time.sleep(20)
'''
    test = '''
import pathlib, signal, subprocess, sys, time

def cancel(signum, _frame):
    pathlib.Path(sys.argv[4]).write_text(str(signum))
    sys.exit(128 + signum)

signal.signal(signal.SIGINT, cancel)
signal.signal(signal.SIGTERM, cancel)
subprocess.Popen([sys.executable, '-c', sys.argv[2], sys.argv[1]], start_new_session=True, env={})
pid_file = pathlib.Path(sys.argv[1])
while not (pid_file.exists() and pid_file.read_text()):
    time.sleep(.01)
if sys.argv[3] == 'True':
    time.sleep(20)
'''
    with subprocess.Popen(['sleep', '20']) as unrelated:
        with subprocess.Popen(
            [
                sys.executable, REAPER, sys.executable, '-c', test,
                pid_file, server, str(cancel_signal is not None), signal_file,
            ],
            stderr=subprocess.PIPE, text=True,
        ) as reaper:
            try:
                if cancel_signal is not None:
                    deadline = time.monotonic() + 5
                    while not (pid_file.exists() and pid_file.read_text()):
                        assert reaper.poll() is None and time.monotonic() < deadline
                        time.sleep(.01)
                    reaper.send_signal(cancel_signal)
                _, stderr = reaper.communicate(timeout=8)
                assert reaper.returncode == (128 + cancel_signal if cancel_signal is not None else 1), stderr
                assert ('left running child processes' in stderr) == (cancel_signal is None)
                if cancel_signal is not None:
                    assert int(signal_file.read_text()) == cancel_signal
                pid = int(pid_file.read_text())
                with pytest.raises(ProcessLookupError):
                    os.kill(pid, 0)
                assert unrelated.poll() is None
            finally:
                if reaper.poll() is None:
                    reaper.terminate()
                unrelated.terminate()


@pytest.mark.parametrize('code', [0, 23])
def test_preserves_the_test_exit_status(code):
    result = subprocess.run([sys.executable, REAPER, 'sh', '-c', f'exit {code}'], timeout=5)
    assert result.returncode == code
