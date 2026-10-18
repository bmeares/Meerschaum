"""Shared helpers of the v4 -> v5 transition harness (stdlib only)."""
from __future__ import annotations

import json
import os
import pathlib
import subprocess
import sys
import time

HERE = pathlib.Path(__file__).resolve().parent
REPO = pathlib.Path(os.environ.get('MRSM_TRANSITION_REPO', str(HERE.parent.parent)))
SCRATCH = pathlib.Path(os.environ.get('MRSM_TRANSITION_DIR', os.path.expanduser('~/.cache/mrsm-transition')))
# The "user" venv: `pip install meerschaum==4.1.4` from PyPI (no dependencies: v4 installs the rest into
# <root>/venvs/mrsm on first use). v5 runs from the same venv with PYTHONPATH=<repo> standing in for
# `pip install -U <v5 wheel>`. MRSM_TRANSITION_V4_PY=.venv-ref/bin/python selects the parity oracle venv instead.
V4_PY = pathlib.Path(os.environ.get('MRSM_TRANSITION_V4_PY', str(SCRATCH / 'venv-v4' / 'bin' / 'python')))
V5_PY = pathlib.Path(os.environ.get('MRSM_TRANSITION_V5_PY', str(V4_PY)))
V5_NO_AUTO_INSTALL = os.environ.get('MRSM_TRANSITION_V5_NO_AUTO_INSTALL', '0') == '1'
V5_BIN = REPO / 'target' / 'debug' / 'mrsm'


class Run:
    """One transition run: a work directory, a root, the plugins dir and scratch databases."""

    def __init__(self, work: pathlib.Path):
        self.work = pathlib.Path(work)
        self.root = self.work / 'root'
        self.plugins = self.work / 'plugins'
        self.db = self.work / 'db'
        self.out = self.work / 'out'
        self.cwd = self.work / 'cwd'  # outside the repo, an empty directory
        for p in (self.root, self.plugins, self.db, self.out, self.cwd):
            p.mkdir(parents=True, exist_ok=True)
        self.state_file = self.work / 'state.json'
        self.state = json.loads(self.state_file.read_text()) if self.state_file.exists() else {}

    def save(self):
        self.state_file.write_text(json.dumps(self.state, indent=2, default=str))

    # -- environment ------------------------------------------------------------------------

    def env(self, v5: bool = False, extra: dict | None = None, root=None, plugins=None) -> dict:
        """A clean environment: ONLY the variables we mean (v4 reads every MRSM_<T>_<L> as a connector)."""
        e = {
            'HOME': os.environ['HOME'],
            'PATH': f"{os.environ['HOME']}/.local/bin:/usr/local/bin:/usr/bin:/bin",
            'LANG': 'C.UTF-8',
            'TMPDIR': str(SCRATCH / 'tmp'),  # /tmp is a tmpfs near its inode limit on the CI host
            'MRSM_ROOT_DIR': str(root or self.root),
            'MRSM_PLUGINS_DIR': str(plugins or self.plugins),
            # default-instance guard: the default instance is a scratch sqlite connector, never sql:main
            'MRSM_SQL_TRN': f"sqlite:///{SCRATCH}/scratch.db",
            'MRSM_SQL_TRNPG': 'postgresql://trnuser:trnpass@127.0.0.1:59999/trndb',  # env-URI connector, never dialed
            'MRSM_CONFIG': '{"meerschaum": {"instance": "sql:trn"}}',
            'MRSM_SQL_TRNENV': f"sqlite:///{self.db}/env.db",  # the env-var connector under test
            'TRN_FLAVOR': (self.state.get('flavors') or ['sqlite'])[0],
            'TRN_LOG': str(self.work / 'hooks.log'),
            'TRN_FETCH_N': str(self.state.get('fetch_n', 5)),
            'UV_PYTHON': str(V4_PY),
        }
        if v5:
            if os.environ.get('MRSM_TRANSITION_V5_WHEEL') != '1':
                e['PYTHONPATH'] = str(REPO)  # else: the installed wheel is what runs
            if V5_NO_AUTO_INSTALL:
                e['MRSM_NO_AUTO_INSTALL'] = '1'
            e['UV_PYTHON'] = str(V5_PY)
            e['MRSM_PYTHON'] = str(V5_PY)
        e.update({k: str(v) for k, v in (extra or {}).items()})
        return e

    def argv(self, which: str) -> list[str]:
        if which == 'v4':
            return [str(V4_PY), '-s', '-E', '-m', 'meerschaum']
        if which == 'v5':
            return [str(V5_PY), '-s', '-m', 'meerschaum']
        if which == 'bin':
            return [str(V5_BIN)]
        raise ValueError(which)

    def mrsm(self, which: str, *args, timeout=300, extra_env=None, check=False, root=None, plugins=None, cwd=None):
        env = self.env(v5=(which != 'v4'), extra=extra_env, root=root, plugins=plugins)
        cmd = self.argv(which) + [str(a) for a in args]
        t0 = time.time()
        p = subprocess.run(
            cmd, env=env, cwd=str(cwd or self.cwd), capture_output=True, text=True, timeout=timeout,
            stdin=subprocess.DEVNULL,
        )
        p.elapsed = time.time() - t0
        if check and p.returncode != 0:
            raise RuntimeError(f"{which} {args} -> {p.returncode}\n{p.stdout}\n{p.stderr}")
        return p

    def py(self, which: str, script: pathlib.Path, *args, timeout=600, extra_env=None, check=False):
        env = self.env(v5=(which != 'v4'), extra=extra_env)
        interp = V4_PY if which == 'v4' else V5_PY
        flags = ['-s', '-E'] if which == 'v4' else ['-s']
        cmd = [str(interp), *flags, str(script), *[str(a) for a in args]]
        p = subprocess.run(cmd, env=env, cwd=str(self.cwd), capture_output=True, text=True,
                           timeout=timeout, stdin=subprocess.DEVNULL)
        if check and p.returncode != 0:
            raise RuntimeError(f"{which} {script} -> {p.returncode}\n{p.stdout}\n{p.stderr}")
        return p


def strip_ansi(s: str) -> str:
    import re
    return re.sub(r'\x1b\[[0-9;?]*[A-Za-z]', '', s)


def scrub_env(extra: dict | None = None) -> dict:
    """A from-scratch environment (no inherited MRSM_*, TMUX, session variables): v4 reads every MRSM_<TYPE>_<LABEL>
    as a connector, and a developer's variables must not leak into scratch venvs or servers."""
    e = {
        'HOME': os.environ['HOME'],
        'PATH': f"{os.environ['HOME']}/.local/bin:/usr/local/bin:/usr/bin:/bin",
        'LANG': 'C.UTF-8',
    }
    e.update({k: str(v) for k, v in (extra or {}).items()})
    return e


def find_root_pids(*roots) -> list[tuple[int, str]]:
    """(pid, command line) of the Meerschaum processes whose MRSM_ROOT_DIR is one of `roots` or below one of them."""
    out = subprocess.run(['pgrep', '-af', 'meerschaum|mrsm'], capture_output=True, text=True).stdout
    pats = [str(r) for r in roots]
    res = []
    for line in out.splitlines():
        if 'pgrep' in line:
            continue
        pid = int(line.split()[0])
        try:
            env = pathlib.Path(f'/proc/{pid}/environ').read_bytes().split(b'\0')
        except OSError:
            continue
        for item in env:
            if item.startswith(b'MRSM_ROOT_DIR=') and any(item.decode().endswith(p) or p in item.decode() for p in pats):
                res.append((pid, line[:200]))
                break
    return res


def pgrep_roots(*roots) -> list[str]:
    return [line for _, line in find_root_pids(*roots)]


def stop_roots(*roots, grace: float = 5.0) -> list[str]:
    """SIGTERM, then SIGKILL, the process groups of the processes of these roots (never this harness's own group).
    Returns the command lines that had to be stopped."""
    import signal
    found = find_root_pids(*roots)
    groups = set()
    for pid, _ in found:
        try:
            pg = os.getpgid(pid)
        except OSError:
            continue
        if pg != os.getpgrp():
            groups.add((pg, pid))
    for sig, wait in ((signal.SIGTERM, grace), (signal.SIGKILL, 1.0)):
        for pg, pid in groups:
            try:
                os.killpg(pg, sig)
            except OSError:
                try:
                    os.kill(pid, sig)
                except OSError:
                    pass
        t0 = time.time()
        while time.time() - t0 < wait and find_root_pids(*roots):
            time.sleep(0.3)
        if not find_root_pids(*roots):
            break
    return [line for _, line in found]


def run_pty(cmd, env, cwd, timeout=120):
    """Run under a pseudo-terminal (v4/v5 `-d` need a tty on stdin). Returns (rc, output)."""
    import pty, select, os, subprocess, time
    master, slave = pty.openpty()
    p = subprocess.Popen(cmd, env=env, cwd=str(cwd), stdin=slave, stdout=slave, stderr=slave, close_fds=True,
                         start_new_session=True)
    os.close(slave)
    out = b''
    t0 = time.time()
    while True:
        r, _, _ = select.select([master], [], [], 0.5)
        if r:
            try:
                d = os.read(master, 65536)
            except OSError:
                d = b''
            if not d:
                break
            out += d
        if p.poll() is not None and not r:
            break
        if time.time() - t0 > timeout:
            p.kill()
            break
    try:
        os.close(master)
    except OSError:
        pass
    try:
        p.wait(timeout=5)
    except Exception:
        p.kill()
    return p.returncode, out.decode('utf-8', 'replace')


def _mrsm_pty(self, which, *args, timeout=120, extra_env=None, root=None, plugins=None, cwd=None):
    env = self.env(v5=(which != 'v4'), extra=extra_env, root=root, plugins=plugins)
    env['TERM'] = 'xterm'
    return run_pty(self.argv(which) + [str(a) for a in args], env, cwd or self.cwd, timeout)


Run.mrsm_pty = _mrsm_pty


JOB_TAGS = {'sqlite': 'sq', 'postgis': 'pg', 'timescaledb': 'ts', 'postgresql': 'pq', 'mssql': 'ms', 'mysql': 'my', 'mariadb': 'md', 'oracle': 'or'}


def job_tag(flavor: str) -> str:
    """Short flavor tag in job names (the jobs table wraps long names, and v4 finds running daemons by scanning
    the process table, so concurrent runs must not share names)."""
    return JOB_TAGS.get(flavor, flavor[:3])
