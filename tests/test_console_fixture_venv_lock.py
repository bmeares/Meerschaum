#! /usr/bin/env python3
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

"""
The console suite's server interpreter (`tests/console/conftest.py::server_python`) is built once
when several processes ask at the same time (pytest-xdist workers): the others wait for it and
use it. Two workers used to build the same directory at once (`venv` with `clear=True`), and the
loser's copy of `python` vanished under it (`FileNotFoundError: .../bin/python3.14`).
"""

import json
import os
import pathlib
import subprocess
import sys
import tempfile
import textwrap

REPO = pathlib.Path(__file__).resolve().parents[1]

WORKER = textwrap.dedent('''
    import json, os, sys, time, pathlib
    from tests.console import conftest
    log = pathlib.Path(sys.argv[1])

    def fake_build(env_dir, python, ready, files):
        # A slow build that a concurrent build would trample: it records its span.
        start = time.monotonic()
        env_dir.mkdir(parents=True, exist_ok=True)
        python.parent.mkdir(parents=True, exist_ok=True)
        python.write_text('')
        time.sleep(1.5)
        assert python.exists(), 'Another build removed this one.'
        ready.write_text('done')
        with open(log, 'a') as stream:
            stream.write(json.dumps([os.getpid(), start, time.monotonic()]) + '\\n')

    conftest._provides_server_packages = lambda python: False
    conftest._build_server_venv = fake_build
    print(json.dumps(conftest.server_python()))
''')


def test_concurrent_workers_build_the_server_interpreter_once():
    scratch = pathlib.Path.home() / '.cache' / 'mrsm-api-host-tests'
    scratch.mkdir(parents=True, exist_ok=True)
    with tempfile.TemporaryDirectory(dir=scratch) as tmp:
        tmp = pathlib.Path(tmp)
        log = tmp / 'builds.jsonl'
        env = {
            key: val for key, val in os.environ.items()
            if not key.startswith('MRSM_') and key not in ('TMUX', 'TMUX_PANE')
        }
        env.update({
            'PYTHONPATH': str(REPO),
            'MRSM_ROOT_DIR': str(tmp / 'root'),
            'MRSM_PLUGINS_DIR': str(tmp / 'plugins'),
            'MRSM_TEST_CONSOLE_VENV_DIR': str(tmp / 'venvs'),
            'PYTHONDONTWRITEBYTECODE': '1',
        })
        (tmp / 'root').mkdir()
        (tmp / 'plugins').mkdir()
        workers = [
            subprocess.Popen(
                [sys.executable, '-c', WORKER, str(log)],
                env=env, cwd=str(REPO), stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True,
            )
            for _ in range(3)
        ]
        results = [worker.communicate(timeout=120) for worker in workers]
        for worker, (stdout, stderr) in zip(workers, results):
            assert worker.returncode == 0, stderr[-3000:]
        pythons = {json.loads(stdout.strip().splitlines()[-1]) for stdout, _ in results}
        builds = [json.loads(line) for line in log.read_text().splitlines()]

    ### One build; every worker uses the same, complete interpreter.
    assert len(builds) == 1, builds
    assert len(pythons) == 1, pythons
