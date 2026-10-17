#! /usr/bin/env python3
# vim:fenc=utf-8

"""
The job listings, the executors and the `meerschaum.utils.daemon` façades over `meerschaum-jobs`,
including the `api:<label>` executor against a loopback `mrsm start api`.

Needs the `mrsm` binary (see `tests/test_jobs.py`). The API server gets its own temporary root
under `~/.cache/mrsm-w16-s/`, a random port in 18400-18999 (never 8989), and no authentication.
"""

import os
import pathlib
import random
import shutil
import signal
import socket
import subprocess
import sys
import tempfile
import time
import urllib.error
import urllib.request
import uuid

import pytest

import meerschaum as mrsm
from meerschaum.jobs import (
    Job,
    StopMonitoringLogs,
    check_restart_jobs,
    get_executor_keys_from_context,
    get_filtered_jobs,
    get_jobs,
    get_paused_jobs,
    get_restart_jobs,
    get_running_jobs,
    get_stopped_jobs,
)


def _find_binary():
    from meerschaum._internal.binary import find_binary
    return find_binary()


pytestmark = pytest.mark.skipif(
    _find_binary() is None,
    reason="The `mrsm` binary is required to run jobs.",
)

LOOP = ['show', 'version', ':', '--loop', '--min-seconds', '0.2']
SCRATCH = pathlib.Path.home() / '.cache' / 'mrsm-w16-s'


def _name(prefix: str = 'w16sf') -> str:
    return f"{prefix}-{uuid.uuid4().hex[:8]}"


def _wait_for(predicate, timeout: float = 30.0, interval: float = 0.05):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        value = predicate()
        if value:
            return value
        time.sleep(interval)
    return predicate()


def _cleanup(job) -> None:
    try:
        if job.status != 'stopped':
            job._core.stop(2)
            if job.status != 'stopped' and job.pid:
                os.kill(job.pid, signal.SIGKILL)
        job.delete()
    except Exception:
        pass


@pytest.fixture
def local_jobs():
    jobs = []

    def _make(sysargs, name=None, **kwargs):
        kwargs.setdefault('executor_keys', 'local')
        job = Job(name or _name(), sysargs, **kwargs)
        jobs.append(job)
        return job

    yield _make
    for job in jobs:
        _cleanup(job)


# --- Listings -------------------------------------------------------------------------------


def test_listings_and_filters(local_jobs, capsys):
    done = local_jobs(['show', 'version'])
    loop = local_jobs(LOOP)
    hidden = local_jobs(['show', 'version'], name=_name('_w16sf'))
    assert done.start()[0] and loop.start()[0] and hidden.start()[0]
    assert _wait_for(lambda: loop.status == 'running')
    assert _wait_for(lambda: done.status == 'stopped' and done.ended is not None)
    assert _wait_for(lambda: hidden.status == 'stopped' and hidden.ended is not None)

    jobs = get_jobs(executor_keys='local')
    assert done.name in jobs and loop.name in jobs
    assert hidden.name not in jobs
    assert all(isinstance(job, Job) for job in jobs.values())
    assert hidden.name in get_jobs(executor_keys='local', include_hidden=True)

    ### The context's executor is `local` under a temporary root.
    assert get_executor_keys_from_context() == 'local'
    assert loop.name in get_jobs()

    assert loop.name in get_running_jobs(executor_keys='local')
    assert done.name not in get_running_jobs(executor_keys='local')
    assert done.name in get_stopped_jobs(executor_keys='local')
    assert loop.name not in get_stopped_jobs(executor_keys='local')
    assert loop.name not in get_paused_jobs(executor_keys='local')
    assert loop.name in get_restart_jobs(executor_keys='local')
    assert done.name not in get_restart_jobs(executor_keys='local')

    filtered = get_filtered_jobs('local', [done.name])
    assert list(filtered) == [done.name]
    ### Only underscore-prefixed names exclude the named jobs.
    excluded = get_filtered_jobs('local', ['_' + done.name])
    assert done.name not in excluded and loop.name in excluded

    capsys.readouterr()
    assert get_filtered_jobs('local', ['w16sf-missing'], warn=True) == {}
    assert "Job 'w16sf-missing' does not exist." in capsys.readouterr().err

    success, msg = check_restart_jobs(
        'local', jobs={loop.name: loop, done.name: done}, silent=True,
    )
    assert success, msg
    assert msg.startswith('Successfully restarted jobs ')


def test_executor_types_include_the_builtins():
    import meerschaum.jobs.systemd  # noqa: F401
    from meerschaum.jobs import executor_types
    assert {'api', 'local', 'systemd'} <= set(executor_types)


# --- The systemd executor (text and paths; every `systemctl` call is covered in Rust) -------


def test_systemd_executor_paths_and_unit_text():
    from meerschaum.jobs.systemd import SystemdExecutor
    from meerschaum._internal.static import STATIC_CONFIG
    executor = SystemdExecutor('systemd')
    assert executor.get_service_name('a b') == 'mrsm-a-b.service'
    service_file = executor.get_service_file_path('x')
    assert service_file.name == 'mrsm-x.service'
    assert service_file.parent == executor.get_service_job_path('x')
    assert executor.get_socket_path('x').name == 'mrsm-x.service.stdin'
    assert executor.get_result_path('x').name == 'mrsm-x.service.result.json'
    assert executor.get_service_logs_path('x').name == 'mrsm-x.service.log'

    text = executor.get_service_file_text('x', ['show', 'version'])
    assert '[Service]' in text
    assert ' -m meerschaum show version\n' in text
    assert f"Environment={STATIC_CONFIG['environment']['daemon_id']}=x\n" in text
    assert 'Restart=always' in text

    socket_text = executor.get_socket_file_text('x')
    assert 'BindsTo=mrsm-x.service' in socket_text
    assert f"ListenFIFO={executor.get_socket_path('x').as_posix()}" in socket_text


# --- The daemon façade ------------------------------------------------------------------------


def test_daemon_facade_reads_and_controls_a_job(local_jobs):
    from meerschaum.utils.daemon import (
        Daemon,
        get_daemon_ids,
        get_daemons,
        get_filtered_daemons,
        get_stopped_daemons,
    )
    job = local_jobs(['show', 'version'])
    assert job.start()[0]
    assert _wait_for(lambda: job.status == 'stopped' and job.ended is not None)

    daemon = Daemon(daemon_id=job.name)
    assert daemon.status == 'stopped'
    assert daemon.daemon_id == job.name and str(daemon) == job.name
    assert daemon.path.is_dir() and daemon.properties_path.is_file()
    assert daemon.pid is None
    assert daemon.target_args == [['show', 'version']]
    assert daemon.properties['process']['ended']
    assert 'Meerschaum v' in daemon.log_text
    assert 'Meerschaum v' in daemon.rotating_log.read()
    assert daemon.hidden is False
    assert daemon == Daemon(daemon_id=job.name) and hash(daemon) == hash(job.name)
    assert job.daemon.daemon_id == job.name

    assert job.name in get_daemon_ids()
    assert daemon in get_daemons()
    assert daemon in get_stopped_daemons()
    assert [d.daemon_id for d in get_filtered_daemons([job.name])] == [job.name]

    success, msg = daemon.cleanup()
    assert success, msg
    assert not daemon.path.exists()


def test_daemon_facade_refuses_what_v5_removed():
    from meerschaum.utils.daemon import Daemon
    with pytest.raises(Exception, match='does not exist'):
        Daemon(daemon_id='w16sf-does-not-exist')
    with pytest.raises(NotImplementedError, match='not supported in Meerschaum v5'):
        Daemon(target=lambda: None)
    import meerschaum.utils.daemon as daemon_module
    for removed in ('run_daemon', 'daemon_action', 'daemon_entry', 'FileDescriptorInterceptor'):
        assert not hasattr(daemon_module, removed)


def test_daemon_names_and_context(monkeypatch):
    from meerschaum._internal.static import STATIC_CONFIG
    from meerschaum.utils.daemon import (
        get_current_daemon,
        get_daemon_ids,
        get_new_daemon_name,
        running_in_daemon,
    )
    name = get_new_daemon_name()
    assert isinstance(name, str) and '-' in name and name not in get_daemon_ids()
    var = STATIC_CONFIG['environment']['daemon_id']
    monkeypatch.delenv(var, raising=False)
    assert running_in_daemon() is False
    assert get_current_daemon() is None
    monkeypatch.setenv(var, 'w16sf-not-a-job')
    assert running_in_daemon() is True
    assert get_current_daemon() is None


def test_job_from_pid(local_jobs):
    job = local_jobs(LOOP)
    assert job.start()[0]
    assert _wait_for(lambda: job.status == 'running')
    found = Job.from_pid(job.pid)
    assert found.name == job.name
    with pytest.raises(ProcessLookupError):
        Job.from_pid(2 ** 22 + 12345)


def test_rotating_file_and_stdin_file(tmp_path):
    from meerschaum.utils.daemon import RotatingFile, StdinFile
    log = RotatingFile(tmp_path / 'w16sf.log', num_files_to_keep=2, max_file_size=100)
    for i in range(30):
        log.write(f"line {i:02d} " + 'x' * 20 + '\n')
    text = log.read()
    assert 'line 29' in text and 'line 00' not in text
    assert 1 <= len(log.get_existing_subfile_paths()) <= 2
    assert log.get_latest_subfile_path().exists()
    log.delete()
    assert not log.get_existing_subfile_paths()
    with pytest.raises(ValueError):
        RotatingFile(tmp_path / 'bad.log', num_files_to_keep=0)
    with pytest.raises(NotImplementedError):
        RotatingFile(tmp_path / 'bad.log', redirect_streams=True)

    stdin = StdinFile(tmp_path / 'input.stdin')
    assert stdin.blocking_file_path.name == 'input.stdin.block'
    stdin.close()


# --- The `api:<label>` executor ---------------------------------------------------------------

FORBIDDEN_PORTS = {8989, 8000, 8998, 3000, 5432, 1883}


def _free_port() -> int:
    ports = list(range(18400, 19000))
    random.shuffle(ports)
    for port in ports:
        if port in FORBIDDEN_PORTS:
            continue
        with socket.socket() as probe:
            try:
                probe.bind(('127.0.0.1', port))
            except OSError:
                continue
        return port
    raise RuntimeError("No free port.")


def _http_status(url: str) -> int:
    try:
        with urllib.request.urlopen(url, timeout=3) as response:
            return response.status
    except urllib.error.HTTPError as e:
        return e.code
    except Exception:
        return 0


def _reap_by_root(root: pathlib.Path) -> None:
    needle = f"MRSM_ROOT_DIR={root}".encode()
    for proc in pathlib.Path('/proc').iterdir():
        if not proc.name.isdigit() or int(proc.name) == os.getpid():
            continue
        try:
            if needle in (proc / 'environ').read_bytes().split(b'\0'):
                os.kill(int(proc.name), signal.SIGKILL)
        except (OSError, PermissionError):
            continue


@pytest.fixture(scope='module')
def api_server():
    """A loopback `mrsm start api --no-auth` with its own root; the connector is `api:w16sf`."""
    SCRATCH.mkdir(parents=True, exist_ok=True)
    root = pathlib.Path(tempfile.mkdtemp(prefix='api-', dir=SCRATCH))
    plugins = root / 'plugins'
    plugins.mkdir()
    port = _free_port()
    env = {
        key: val for key, val in os.environ.items()
        if not key.startswith('MRSM_') and key not in ('TMUX', 'TMUX_PANE', 'HOME')
    }
    env.update({
        'HOME': str(root),
        'MRSM_ROOT_DIR': str(root),
        'MRSM_PLUGINS_DIR': str(plugins),
        'MRSM_NO_AUTO_INSTALL': '1',
        'MRSM_NO_UPDATE_CHECK': '1',
        'MRSM_PYTHON': sys.executable,
        'MRSM_SQL_W16SF': f"sqlite:///{root / 'w16sf.db'}",
        'MRSM_CONFIG': '{"meerschaum": {"instance": "sql:w16sf", "api_instance": "sql:w16sf"}}',
    })
    binary = _find_binary()
    log = open(root / 'server.log', 'wb')
    proc = subprocess.Popen(
        [binary, 'start', 'api', '--host', '127.0.0.1', '--port', str(port),
         '--no-auth', '--no-dash', '--no-webterm'],
        env=env, cwd=str(root), stdout=log, stderr=subprocess.STDOUT, start_new_session=True,
    )
    try:
        deadline = time.time() + 120
        while time.time() < deadline:
            if proc.poll() is not None:
                raise RuntimeError((root / 'server.log').read_text(errors='replace')[-3000:])
            if _http_status(f"http://127.0.0.1:{port}/version") == 200:
                break
            time.sleep(0.3)
        else:
            raise RuntimeError("The API server did not start.")
        connector = mrsm.get_connector(
            'api', 'w16sf', host='127.0.0.1', port=port, protocol='http',
        )
        yield connector
    finally:
        try:
            os.killpg(proc.pid, signal.SIGTERM)
            proc.wait(timeout=20)
        except Exception:
            try:
                os.killpg(proc.pid, signal.SIGKILL)
            except Exception:
                pass
        log.close()
        _reap_by_root(root)
        shutil.rmtree(root, ignore_errors=True)


def test_api_executor_job_lifecycle(api_server):
    name = _name('w16sf-remote')
    job = Job(name, LOOP, executor_keys='api:w16sf')
    try:
        assert job.executor_keys == 'api:w16sf'
        assert job.executor is api_server
        assert not job.exists()
        assert job.status == 'stopped'

        success, msg = job.start()
        assert success, msg
        assert job.exists()
        assert _wait_for(lambda: job.status == 'running', timeout=60)
        assert isinstance(job.pid, int)
        assert job.sysargs == LOOP
        assert job.restart is True
        assert _wait_for(lambda: 'Meerschaum v' in (job.get_logs() or ''), timeout=60)

        listed = get_jobs('api:w16sf')
        assert name in listed and listed[name].executor_keys == 'api:w16sf'
        assert name in get_running_jobs('api:w16sf')
        assert name in get_filtered_jobs('api:w16sf', [name])

        seen = []

        def callback(line):
            seen.append(line)
            raise StopMonitoringLogs

        job.monitor_logs(callback_function=callback, accept_input=False)
        assert seen

        success, msg = job.pause()
        assert success, msg
        assert _wait_for(lambda: job.status == 'paused', timeout=30)
        assert job.paused is not None

        success, msg = job.stop()
        assert success, msg
        assert _wait_for(lambda: job.status == 'stopped', timeout=30)
        assert job.stop_time is not None

        success, msg = job.delete()
        assert success, msg
        assert _wait_for(lambda: not job.exists())
    finally:
        _cleanup(job)


def test_api_executor_reports_an_unreachable_endpoint(capsys):
    with socket.socket() as probe:
        probe.bind(('127.0.0.1', 0))
        dead_port = probe.getsockname()[1]
    mrsm.get_connector('api', 'w16sdead', host='127.0.0.1', port=dead_port, protocol='http')
    job = Job(_name('w16sf-dead'), 'show version', executor_keys='api:w16sdead')
    success, msg = job.start()
    assert not success and msg
    capsys.readouterr()
    ### A failed read is not "no jobs": the listing says why before it answers.
    assert get_jobs('api:w16sdead') == {}
    assert 'Could not get the jobs of executor' in capsys.readouterr().err
