#! /usr/bin/env python3
# vim:fenc=utf-8

"""
Wave 16 (task S): `meerschaum.connectors.api.APIConnector` is a façade over the Rust client
(`meerschaum._core.ApiConnector`).

The server tests run against a loopback `mrsm start api` (random port, temporary root, a SQLite
instance and an admin user; never 8989). The v4 oracle is the released 4.1.4 in `.venv-ref` (the
tests which compare with it skip when it is not built).
"""

from __future__ import annotations

import os
import sys
import json
import time
import uuid
import pathlib
import subprocess
import tarfile
import tempfile
from datetime import datetime, timezone

import pytest

from tests._binary import skip_or_fail
import meerschaum as mrsm
from meerschaum.connectors.api import APIConnector

REPO = pathlib.Path(__file__).resolve().parents[1]
REF_PYTHON = REPO / '.venv-ref' / 'bin' / 'python'

pytestmark = pytest.mark.skipif(
    not __import__('meerschaum._core', fromlist=['x']).AVAILABLE
    or not hasattr(__import__('meerschaum._core', fromlist=['x']), 'ApiConnector'),
    reason="the extension with `ApiConnector` is not built (scripts/rust/build_py.sh)",
)


def _v4(code: str) -> str:
    """Run `code` with the released 4.1.4 outside the repo; returns its stdout."""
    if not REF_PYTHON.exists():
        skip_or_fail("the 4.1.4 reference is not built (scripts/parity/setup_reference.sh)")
    root = tempfile.mkdtemp(prefix='mrsm-w16s-ref-', dir=_scratch())
    env = {
        key: val for key, val in os.environ.items()
        if not key.startswith('MRSM_') and key not in ('PYTHONPATH', 'TMUX', 'TMUX_PANE')
    }
    env.update({
        'MRSM_ROOT_DIR': root,
        'MRSM_PLUGINS_DIR': str(pathlib.Path(root) / 'plugins'),
        'MRSM_NO_AUTO_INSTALL': '1',
        'MRSM_NO_UPDATE_CHECK': '1',
    })
    ### The packages the v4 client needs (`more_itertools`, pandas, ...) are in `.venv-ref`.
    result = subprocess.run(
        [str(REF_PYTHON), '-s', '-E', '-c', code],
        env=env, cwd=root, capture_output=True, text=True, timeout=180,
    )
    assert result.returncode == 0, result.stderr[-3000:]
    return result.stdout


def _scratch() -> str:
    base = pathlib.Path.home() / '.cache' / 'mrsm-w16-s'
    base.mkdir(parents=True, exist_ok=True)
    return str(base)


def _connector(label, **kw):
    """An `api:<label>` connector the registry knows (`Job` resolves its executor by keys)."""
    return mrsm.get_connector('api', label, **kw)


# ---------------------------------------------------------------------------------------------
# Offline


def test_base_path_imports_no_http_client():
    """`import meerschaum`, building the connectors and the key helpers import no HTTP library (D20)."""
    code = (
        "import sys\n"
        "import meerschaum as mrsm\n"
        "from meerschaum.connectors.api import APIConnector\n"
        "from meerschaum.connectors.valkey import ValkeyConnector\n"
        "c = APIConnector('w16s', host='localhost', port=1, username='u', password='p')\n"
        "assert c.URI and c.url and c.login_scheme == 'password'\n"
        "v = ValkeyConnector('w16s', host='localhost', port=1)\n"
        "assert v.URI and v.quote_table('a b')\n"
        "banned = ('requests', 'httpx', 'urllib3', 'redis', 'valkey', 'psutil', 'websockets')\n"
        "print(sorted(m for m in banned if m in sys.modules))\n"
    )
    root = tempfile.mkdtemp(prefix='mrsm-w16s-imp-', dir=_scratch())
    env = {
        key: val for key, val in os.environ.items()
        if not key.startswith('MRSM_') and key not in ('TMUX', 'TMUX_PANE')
    }
    env.update({
        'PYTHONPATH': str(REPO),
        'MRSM_ROOT_DIR': root,
        'MRSM_PLUGINS_DIR': str(pathlib.Path(root) / 'plugins'),
        'MRSM_NO_AUTO_INSTALL': '1',
    })
    result = subprocess.run(
        [sys.executable, '-c', code], env=env, cwd=root,
        capture_output=True, text=True, timeout=120,
    )
    assert result.returncode == 0, result.stderr[-3000:]
    assert result.stdout.strip().splitlines()[-1] == '[]', result.stdout


@pytest.mark.parametrize('uri', [
    'http://localhost:8989',
    'https://User:p%40ss@Example.com:8443',
    'http://alice@10.0.0.5',
])
def test_from_uri_and_properties_match_v4(uri):
    ref = json.loads(_v4(
        "import json\n"
        "from meerschaum.connectors.api import APIConnector\n"
        f"params = APIConnector.from_uri({uri!r}, as_dict=True)\n"
        f"c = APIConnector.from_uri({uri!r})\n"
        "print(json.dumps({'params': params, 'url': c.url, 'URI': c.URI, "
        "'scheme': c.login_scheme, 'label': c.label}))\n"
    ).strip().splitlines()[-1])
    params = APIConnector.from_uri(uri, as_dict=True)
    conn = APIConnector.from_uri(uri)
    assert params == ref['params']
    assert (conn.url, conn.URI, conn.login_scheme, conn.label) == (
        ref['url'], ref['URI'], ref['scheme'], ref['label'],
    )


def test_unreachable_server_raises_like_requests_and_warns(capsys):
    conn = _connector('w16s_down', host='127.0.0.1', port=1, protocol='http')
    pipe = mrsm.Pipe('w16s', 'down', instance=conn)
    with pytest.raises(ConnectionError):
        conn.pipe_exists(pipe)
    assert conn.test_connection() is False
    assert conn.get_chaining_status() is None
    assert conn.get_mrsm_version() is None
    # `get_pipe_data` warns and returns `None` (v4).
    assert conn.get_pipe_data(pipe) is None
    assert 'Failed to get data' in capsys.readouterr().err


def test_the_client_is_rebuilt_after_a_fork():
    """The fork happens in a child interpreter (this one runs threads: forking it is unsafe)."""
    code = (
        "import os, warnings\n"
        "warnings.simplefilter('ignore')\n"
        "import meerschaum as mrsm\n"
        "conn = mrsm.get_connector('api', 'w16s_fork', host='127.0.0.1', port=1, protocol='http')\n"
        "parent = conn._rust_client()\n"
        "assert conn._rust_client() is parent\n"
        "pid = os.fork()\n"
        "if pid == 0:\n"
        "    status = 1\n"
        "    try:\n"
        "        child = conn._rust_client()\n"
        "        try:\n"
        "            conn.get('/version')\n"
        "        except ConnectionError:\n"
        "            status = 0 if child is not parent else 2\n"
        "    finally:\n"
        "        os._exit(status)\n"
        "_, wait_status = os.waitpid(pid, 0)\n"
        "assert os.waitstatus_to_exitcode(wait_status) == 0, wait_status\n"
        "assert conn._rust_client() is parent\n"
        "print('FORK-OK')\n"
    )
    result = _run_child(code)
    assert 'FORK-OK' in result.stdout, result.stderr[-3000:]


def _run_child(code: str) -> subprocess.CompletedProcess:
    root = tempfile.mkdtemp(prefix='mrsm-w16s-child-', dir=_scratch())
    env = {
        key: val for key, val in os.environ.items()
        if not key.startswith('MRSM_') and key not in ('TMUX', 'TMUX_PANE')
    }
    env.update({
        'PYTHONPATH': str(REPO),
        'MRSM_ROOT_DIR': root,
        'MRSM_PLUGINS_DIR': str(pathlib.Path(root) / 'plugins'),
        'MRSM_NO_AUTO_INSTALL': '1',
    })
    return subprocess.run(
        [sys.executable, '-c', code], env=env, cwd=root,
        capture_output=True, text=True, timeout=180,
    )


def test_a_connector_can_be_copied_and_pickled():
    import copy
    import pickle
    conn = _connector('w16s_copy', host='127.0.0.1', port=1, protocol='http')
    conn._rust_client()
    assert copy.deepcopy(conn).url == conn.url
    assert pickle.loads(pickle.dumps(conn)).URI == conn.URI


# ---------------------------------------------------------------------------------------------
# A loopback server


@pytest.fixture(scope='module')
def server():
    from tests._binary import binary_or_skip
    binary_or_skip()
    from tests.console.conftest import Server
    instance = Server('w16s-api', native=False)
    try:
        instance.start()
    except Exception as e:
        instance.stop()
        pytest.skip(f"cannot start a loopback API server: {e}")
    yield instance
    instance.stop()


@pytest.fixture(scope='module')
def conn(server):
    from tests.console.conftest import USERNAME, PASSWORD
    return _connector(
        'w16s_main', host='127.0.0.1', port=server.port, protocol='http',
        username=USERNAME, password=PASSWORD,
    )


def _pipe(conn, name=None, **kw):
    kw.setdefault('columns', {'datetime': 'dt', 'id': 'id'})
    kw.setdefault('dtypes', {'dt': 'datetime', 'id': 'int', 'val': 'float', 'name': 'str'})
    return mrsm.Pipe('w16s', name or 'p' + uuid.uuid4().hex[:8], instance=conn, **kw)


def _docs(n=5, offset=0):
    return [
        {
            'dt': datetime(2024, 1, 1 + i + offset, tzinfo=timezone.utc).replace(tzinfo=None),
            'id': i + offset,
            'val': float(i + offset) / 2,
            'name': f'n{i + offset}',
        }
        for i in range(n)
    ]


def test_login_token_and_misc(server, conn):
    success, msg = conn.login()
    assert success, msg
    assert 'Successfully logged into' in msg
    assert conn.token and conn.token == conn.token
    assert conn.get_chaining_status() in (True, False)
    assert conn.get_mrsm_version()
    assert conn.test_connection() is True
    assert conn.URI.startswith('http://') and str(server.port) in conn.URI

    bad = _connector(
        'w16s_bad', host='127.0.0.1', port=server.port, protocol='http',
        username='nobody', password='wrong',
    )
    success, msg = bad.login(warn=False)
    assert not success and "Failed to log into" in msg


def test_a_failed_login_warns_once(server, capsys):
    bad = _connector(
        'w16s_bad2', host='127.0.0.1', port=server.port, protocol='http',
        username='nobody', password='wrong',
    )
    bad.login()
    bad.login()
    assert capsys.readouterr().err.count('Failed to log into') == 1


def test_login_warn_false_is_silent(server, capsys):
    bad = _connector(
        'w16s_bad3', host='127.0.0.1', port=server.port, protocol='http',
        username='nobody', password='wrong',
    )
    assert bad.login(warn=False)[0] is False
    assert capsys.readouterr().err == ''
    # A silent login leaves the connector's one warning for the next `login()` (as v4).
    assert bad.login()[0] is False
    assert capsys.readouterr().err.count('Failed to log into') == 1
    assert bad.login()[0] is False
    assert capsys.readouterr().err == ''
    other = _connector(
        'w16s_bad4', host='127.0.0.1', port=server.port, protocol='http',
        username='nobody', password='wrong',
    )
    assert other.login()[0] is False
    assert capsys.readouterr().err.count('Failed to log into') == 1
    assert other.login(warn=False)[0] is False
    assert capsys.readouterr().err == ''


def test_make_request_and_the_response_shape(conn):
    response = conn.get('/version')
    assert response.ok and bool(response) and response.status_code == 200
    assert isinstance(response.json(), str)
    assert response.text.strip('"') == response.json()
    assert response.content == response.text.encode()
    assert 'json' in response.headers['Content-Type']
    response.raise_for_status()

    missing = conn.get('/no/such/route')
    assert not missing and missing.status_code == 404
    with pytest.raises(OSError):
        missing.raise_for_status()
    with pytest.raises(ValueError):
        conn.make_request('TRACE', '/version')

    no_token = conn.get('/pipes/keys', use_token=False)
    assert no_token.status_code in (401, 403)


def test_pipe_lifecycle_through_the_connector(conn):
    pipe = _pipe(conn)
    try:
        assert conn.pipe_exists(pipe) is False
        assert conn.get_pipe_id(pipe) is None
        success, msg = conn.register_pipe(pipe)
        assert success, msg
        attributes = conn.get_pipe_attributes(pipe)
        assert attributes['parameters']['columns'] == {'datetime': 'dt', 'id': 'id'}
        pipe_id = conn.get_pipe_id(pipe)
        assert isinstance(pipe_id, (int, str)) and pipe_id is not None

        success, msg = conn.sync_pipe(pipe, _docs(5))
        assert success, msg
        assert 'to sync 5 rows across 1 chunk' in msg
        assert conn.pipe_exists(pipe) is True
        assert conn.get_pipe_rowcount(pipe) == 5
        assert conn.get_pipe_rowcount(pipe, begin=datetime(2024, 1, 3)) == 3
        assert conn.get_pipe_rowcount(pipe, params={'id': [0, 1]}) == 2

        df = conn.get_pipe_data(pipe)
        assert list(df['id']) == [0, 1, 2, 3, 4]
        assert set(df.columns) == {'dt', 'id', 'val', 'name'}
        assert str(df['dt'].dtype).startswith('datetime64')
        subset = conn.get_pipe_data(pipe, select_columns=['id', 'val'], params={'id': 3})
        assert list(subset.columns) == ['id', 'val'] and list(subset['id']) == [3]
        omitted = conn.get_pipe_data(pipe, omit_columns=['name'], end=datetime(2024, 1, 3))
        assert 'name' not in omitted.columns and len(omitted) == 2

        docs = conn.get_pipe_docs(pipe, order='desc', limit=2)
        assert [d['id'] for d in docs] == [4, 3]
        assert conn.get_pipe_docs(pipe, params={'id': 99}) == []

        sync_time = conn.get_sync_time(pipe)
        utc = timezone.utc
        assert sync_time == datetime(2024, 1, 5, tzinfo=utc)
        assert conn.get_sync_time(pipe, newest=False) == datetime(2024, 1, 1, tzinfo=utc)
        assert conn.get_sync_time(pipe, params={'id': 1}) == datetime(2024, 1, 2, tzinfo=utc)

        types = conn.get_pipe_columns_types(pipe)
        assert set(types) == {'dt', 'id', 'val', 'name'}
        indices = conn.get_pipe_columns_indices(pipe)
        assert isinstance(indices, dict)
        assert isinstance(conn.get_pipe_index_names(pipe), dict)
        assert conn.get_pipe_size(pipe) is None or conn.get_pipe_size(pipe) >= 0

        # `sync_pipe` of the same rows again leaves five.
        assert conn.sync_pipe(pipe, _docs(5))[0]
        assert conn.get_pipe_rowcount(pipe) == 5
        # A DataFrame and a dictionary of lists sync too.
        pd = pytest.importorskip('pandas')
        assert conn.sync_pipe(pipe, pd.DataFrame(_docs(2, offset=5)))[0]
        assert conn.sync_pipe(pipe, {'dt': [datetime(2024, 1, 20)], 'id': [20],
                                     'val': [1.5], 'name': ['x']})[0]
        assert conn.sync_pipe(pipe, json.dumps(_docs(1, offset=30), default=str))[0]
        assert conn.get_pipe_rowcount(pipe) == 9

        # `None` and an unsupported type.
        success, msg = conn.sync_pipe(pipe, None)
        assert not success and 'DataFrame is `None`' in msg
        with pytest.raises(TypeError):
            conn.sync_pipe(pipe, 12)

        success, msg = conn.clear_pipe(pipe, params={'id': 20})
        assert success, msg
        assert conn.get_pipe_rowcount(pipe) == 8
        success, msg = conn.clear_pipe(pipe, begin=datetime(2024, 1, 3), end=datetime(2024, 1, 5))
        assert success, msg
        assert conn.get_pipe_rowcount(pipe) == 6

        success, msg = conn.delete_pipe_cache(pipe)
        assert success, msg
        success, msg = conn.drop_pipe(pipe)
        assert success, msg
        assert conn.pipe_exists(pipe) is False
        assert conn.get_pipe_attributes(pipe)['parameters']
    finally:
        conn.delete_pipe(pipe)
    assert conn.get_pipe_id(pipe) is None


def test_sync_pipe_chunks_and_forwards_keyword_arguments(conn):
    pipe = _pipe(conn)
    try:
        assert conn.register_pipe(pipe)[0]
        success, msg = conn.sync_pipe(pipe, _docs(5), chunksize=2)
        assert success, msg
        assert 'to sync 5 rows across 3 chunks' in msg
        assert conn.get_pipe_rowcount(pipe) == 5
        success, msg = conn.sync_pipe(pipe, _docs(5), check_existing=False)
        assert success, msg
    finally:
        conn.delete_pipe(pipe)


def test_fetch_pipes_keys_filters_and_shape(conn):
    pipes = [_pipe(conn, f'k{uuid.uuid4().hex[:6]}', tags=['w16s-tag']) for _ in range(2)]
    other = _pipe(conn, f'k{uuid.uuid4().hex[:6]}', location=None)
    try:
        for pipe in pipes + [other]:
            assert conn.register_pipe(pipe)[0]
        keys = conn.fetch_pipes_keys(connector_keys=['w16s'], tags=['w16s-tag'])
        assert isinstance(keys, dict)
        assert sorted(v[1] for v in keys.values()) == sorted(p.metric_key for p in pipes)
        assert all(isinstance(k, (int, str)) for k in keys)
        by_metric = conn.fetch_pipes_keys(metric_keys=[other.metric_key], location_keys=[None])
        assert [v[:3] for v in by_metric.values()] == [('w16s', other.metric_key, None)]
        negated = conn.fetch_pipes_keys(connector_keys=['w16s'], metric_keys=[f'_{other.metric_key}'])
        assert other.metric_key not in {v[1] for v in negated.values()}
    finally:
        for pipe in pipes + [other]:
            conn.delete_pipe(pipe)


def test_edit_pipe_clears_the_clients_attribute_cache(conn):
    pipe = _pipe(conn)
    try:
        assert conn.register_pipe(pipe)[0]
        assert conn.sync_pipe(pipe, _docs(3))[0]
        pipe.parameters = {**pipe.parameters, 'tags': ['edited']}
        success, msg = conn.edit_pipe(pipe)
        assert success, msg
        assert conn.get_pipe_attributes(pipe)['parameters']['tags'] == ['edited']
        success, msg = conn.edit_pipe(pipe, patch=True)
        assert success, msg
    finally:
        conn.delete_pipe(pipe)


def test_maintenance_methods_answer_with_success_tuples(conn):
    pipe = _pipe(conn)
    try:
        assert conn.register_pipe(pipe)[0]
        assert conn.sync_pipe(pipe, _docs(3))[0]
        for name, kwargs in (
            ('vacuum_pipe', {}),
            ('vacuum_pipe', {'full': True}),
            ('analyze_pipe', {}),
            ('compress_pipe', {}),
            ('decompress_pipe', {}),
            ('decompress_pipe', {'no_policy': True}),
            ('partition_pipe', {}),
            ('partition_pipe', {'chunk_minutes': 1440}),
        ):
            result = getattr(conn, name)(pipe, **kwargs)
            assert isinstance(result, tuple) and len(result) == 2, (name, result)
            assert isinstance(result[0], bool) and isinstance(result[1], str), (name, result)
    finally:
        conn.delete_pipe(pipe)


def test_users_through_the_connector(conn):
    from meerschaum.core import User
    name = 'w16s_' + uuid.uuid4().hex[:6]
    user = User(name, 'pw-' + uuid.uuid4().hex[:8], instance=conn, email=f'{name}@example.com')
    try:
        assert name not in conn.get_users()
        success, msg = conn.register_user(user)
        assert success, msg
        assert name in conn.get_users()
        user_id = conn.get_user_id(user)
        assert user_id is not None
        attributes = conn.get_user_attributes(user)
        assert attributes is None or isinstance(attributes, dict)
        assert conn.get_user_type(user) in (None, '', 'user')
        success, msg = conn.edit_user(user)
        assert isinstance(success, bool) and isinstance(msg, str)
    finally:
        success, msg = conn.delete_user(user)
    assert success, msg
    assert name not in conn.get_users()


def test_plugins_through_the_connector(conn, tmp_path):
    from types import SimpleNamespace
    name = 'w16s' + uuid.uuid4().hex[:6]
    assert name not in conn.get_plugins()
    assert conn.get_plugin_version(SimpleNamespace(name=name)) is None

    source = tmp_path / f'{name}.py'
    source.write_text("__version__ = '0.0.1'\ndef fetch(pipe, **kw):\n    return None\n")
    archive = tmp_path / f'{name}.tar.gz'
    with tarfile.open(archive, 'w:gz') as tar:
        tar.add(source, arcname=f'{name}/__init__.py')
    plugin = SimpleNamespace(
        name=name,
        version='0.0.1',
        attributes={'description': 'w16s'},
        user_id=None,
        archive_path=archive,
        make_tar=lambda debug=False: archive,
        __str__=lambda self: name,
    )
    try:
        success, msg = conn.register_plugin(plugin)
        assert success, msg
        assert name in conn.get_plugins()
        assert conn.get_plugins(search_term=name[:6]).count(name) == 1
        assert conn.get_plugin_version(plugin) == '0.0.1'
        assert conn.get_plugin_attributes(plugin).get('description') == 'w16s'
    finally:
        success, msg = conn.delete_plugin(plugin)
    assert success, msg
    assert name not in conn.get_plugins()


def test_tokens_through_the_connector(conn):
    from meerschaum.core import Token, User
    from tests.console.conftest import USERNAME, PASSWORD
    label = 'w16s-' + uuid.uuid4().hex[:6]
    user = User(USERNAME, PASSWORD, instance=conn)
    token = Token(label, instance=conn, user=user)
    try:
        success, msg = conn.register_token(token)
        assert success, msg
        assert token.id is not None and token.secret
        assert conn.token_exists(token) is True
        model = conn.get_token_model(token.id)
        assert model.label == label
        assert label in [t.label for t in conn.get_tokens()]
        assert [t.label for t in conn.get_tokens(labels=[label])] == [label]
        # PARITY W17T-004: v4 returned `[]` here (`getattr(<list>, 'scopes', [])`); v5 returns the
        # token's real scopes (a token registered without scopes holds every scope).
        from meerschaum._internal.static import STATIC_CONFIG
        assert conn.get_token_scopes(token) == list(STATIC_CONFIG['tokens']['scopes'])
        # A token read back has its creation time (`edit_token` sends it).
        loaded = conn.get_tokens(labels=[label])[0]
        loaded.scopes = ['pipes:read']
        success, msg = conn.edit_token(loaded)
        assert success, msg
        success, msg = conn.invalidate_token(loaded)
        assert success, msg
    finally:
        success, msg = conn.delete_token(token)
    assert success, msg
    assert conn.token_exists(token) is False


def test_actions_and_remote_jobs(conn):
    actions = conn.get_actions().json()
    assert 'show' in actions and 'sync' in actions

    lines = []
    success, msg = conn._rust_client().do_action(['show', 'version'], lines.append)
    assert success, msg
    assert any('Meerschaum' in line for line in lines), lines

    name = 'w16s-job-' + uuid.uuid4().hex[:6]
    assert conn.get_job_exists(name) is False
    success, msg = conn.create_job(name, ['show', 'version'])
    assert success, msg
    try:
        assert conn.get_job_exists(name) is True
        deadline = time.time() + 60
        while time.time() < deadline and conn.get_job_status(name) == 'running':
            time.sleep(0.2)
            conn._rust_client().get_job_metadata(name)
        deadline = time.time() + 30
        logs = ''
        while time.time() < deadline and 'Meerschaum' not in logs:
            time.sleep(0.3)
            logs = conn.get_logs(name)
        assert 'Meerschaum' in logs
        assert isinstance(conn.get_job_metadata(name), dict)
        assert conn.get_job_properties(name) is not None
        assert conn.get_job_is_blocking_on_stdin(name) is False
        assert conn.get_job_prompt_kwargs(name) == {}
        job = conn.get_job(name)
        assert job.name == name and job.executor_keys == str(conn)
        assert name in conn.get_jobs()

        received = []
        conn.monitor_logs(
            name, received.append, None, None, stop_on_exit=True, strip_timestamps=True,
        )
        assert any('Meerschaum' in line for line in received), received
    finally:
        success, msg = conn.delete_job(name)
    assert success, msg
    assert conn.get_job_exists(name) is False


def test_monitor_logs_stops_on_stop_monitoring_logs(conn):
    from meerschaum.jobs import StopMonitoringLogs
    name = 'w16s-mon-' + uuid.uuid4().hex[:6]
    success, msg = conn.create_job(name, ['show', 'version', ':', '--loop', '--min-seconds', '0.2'])
    assert success, msg
    try:
        conn.start_job(name)
        seen = []

        def callback(line):
            seen.append(line)
            raise StopMonitoringLogs

        started = time.time()
        conn.monitor_logs(name, callback, None, None)
        assert seen and time.time() - started < 60
        assert conn.pause_job(name)[0] in (True, False)
        assert conn.stop_job(name)[0] in (True, False)
        assert conn.get_job_stop_time(name) is None or isinstance(
            conn.get_job_stop_time(name), datetime,
        )
    finally:
        conn.stop_job(name)
        conn.delete_job(name)


def test_monitor_logs_async_runs_coroutine_callbacks(conn):
    import asyncio
    name = 'w16s-async-' + uuid.uuid4().hex[:6]
    assert conn.create_job(name, ['show', 'version'])[0]
    try:
        assert conn.start_job(name)[0] in (True, False)
        received, stopped = [], []

        async def callback(line):
            await asyncio.sleep(0)
            received.append(line)

        async def on_stop(result):
            stopped.append(result)

        async def main():
            await conn.monitor_logs_async(
                name, callback, None, on_stop, stop_on_exit=True, strip_timestamps=True,
            )

        asyncio.run(main())
        assert any('Meerschaum' in line for line in received), received
        assert stopped and isinstance(stopped[0], tuple)
    finally:
        conn.delete_job(name)


def test_a_cancelled_monitor_stops_streaming(conn):
    import asyncio
    name = 'w16s-cancel-' + uuid.uuid4().hex[:6]
    assert conn.create_job(name, ['show', 'version', ':', '--loop', '--min-seconds', '0.2'])[0]
    try:
        assert conn.start_job(name)[0] in (True, False)
        seen = []

        async def main():
            task = asyncio.ensure_future(conn.monitor_logs_async(
                name, seen.append, None, None, strip_timestamps=True,
            ))
            deadline = time.time() + 30
            while time.time() < deadline and len(seen) < 2:
                await asyncio.sleep(0.2)
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                pass

        asyncio.run(main())
        count = len(seen)
        assert count >= 1
        time.sleep(1.5)
        # The stream was closed: lines which keep coming no longer reach the callback.
        assert len(seen) <= count + 1
    finally:
        conn.stop_job(name)
        conn.delete_job(name)


def test_do_action_async_streams_output(conn):
    import asyncio
    lines = []

    async def main():
        return await conn.do_action_async(['show', 'version'], lines.append)

    success, msg = asyncio.run(main())
    assert success, msg
    assert any('Meerschaum' in line for line in lines), lines
    assert conn.do_action(['show', 'version'])[0] is True


def test_do_action_legacy_posts_the_action(conn):
    success, msg = conn.do_action_legacy(['show', 'version'])
    assert isinstance(success, bool) and isinstance(msg, str)


def test_a_pipe_whose_instance_is_the_connector_reads_and_syncs(conn):
    pipe = _pipe(conn)
    try:
        success, msg = pipe.sync(_docs(4))
        assert success, msg
        assert pipe.get_rowcount() == 4
        assert list(pipe.get_data()['id']) == [0, 1, 2, 3]
        assert pipe.get_sync_time() == datetime(2024, 1, 4, tzinfo=timezone.utc)
        assert pipe.exists()
    finally:
        pipe.delete()


# ---------------------------------------------------------------------------------------------
# The released 4.1.4 client against the same server is the oracle

BATTERY = r'''
import json, re, uuid, time, sys
from datetime import datetime
import meerschaum as mrsm
from meerschaum.connectors.api import APIConnector


def family(dtype):
    text = str(dtype).lower()
    for key in ('datetime', 'int', 'float', 'bool'):
        if key in text:
            return key
    return 'object'


def norm(x):
    if isinstance(x, datetime):
        return 'dt:' + x.isoformat()
    if isinstance(x, (list, tuple)):
        return [norm(i) for i in x]
    if isinstance(x, dict):
        return {str(k): norm(v) for k, v in x.items()}
    if isinstance(x, str):
        if x.startswith('Failed to post a chunk'):
            ### The transport's error text (`requests` / `reqwest`) is its own.
            x = x.split('\n')[0]
        return re.sub(r'It took [^ ]+( [^ ]+)? to sync', 'It took X to sync', x)
    if hasattr(x, 'columns') and hasattr(x, 'dtypes'):
        return {
            'df': {c: family(x[c].dtype) for c in x.columns},
            'rows': [
                [norm(None if v != v else v.isoformat() if hasattr(v, 'isoformat') else v)
                 for v in row]
                for row in x.astype(object).values.tolist()
            ],
        }
    if x is None or isinstance(x, (bool, int, float)):
        return x
    return repr(x)


def run(host, port, username, password, tag):
    out = {}
    conn = APIConnector(
        'oracle' + tag, host=host, port=port, protocol='http',
        username=username, password=password,
    )

    def rec(name, fn):
        try:
            out[name] = norm(fn())
        except Exception as e:
            kind = type(e).__name__
            ### `error()` raises `Exception`; the Rust client raises its subclass `RuntimeError`.
            out[name] = {'raised': (
                'ConnectionError' if 'Connection' in kind
                else 'Exception' if kind == 'RuntimeError' else kind
            )}

    pipe = mrsm.Pipe(
        'w16s', 'oracle' + tag, instance=conn,
        columns={'datetime': 'dt', 'id': 'id'},
        dtypes={'dt': 'datetime', 'id': 'int', 'val': 'float', 'name': 'str'},
    )
    docs = [
        {'dt': datetime(2024, 1, 1 + i), 'id': i, 'val': i / 2, 'name': f'n{i}'}
        for i in range(5)
    ]
    rec('exists_0', lambda: conn.pipe_exists(pipe))
    rec('id_0', lambda: conn.get_pipe_id(pipe))
    rec('attributes_0', lambda: conn.get_pipe_attributes(pipe))
    rec('data_0', lambda: conn.get_pipe_data(pipe))
    rec('docs_0', lambda: conn.get_pipe_docs(pipe))
    rec('rowcount_0', lambda: conn.get_pipe_rowcount(pipe))
    rec('sync_time_0', lambda: conn.get_sync_time(pipe))
    rec('types_0', lambda: conn.get_pipe_columns_types(pipe))
    rec('register', lambda: conn.register_pipe(pipe))
    rec('register_again', lambda: conn.register_pipe(pipe))
    rec('id_type', lambda: type(conn.get_pipe_id(pipe)).__name__)
    rec('attributes', lambda: conn.get_pipe_attributes(pipe)['parameters'])
    ### v4 only takes a DataFrame (`df.columns` is read before anything else).
    import pandas as pd
    df = pd.DataFrame(docs)
    rec('sync', lambda: conn.sync_pipe(pipe, df))
    rec('sync_again', lambda: conn.sync_pipe(pipe, df))
    rec('sync_chunks', lambda: conn.sync_pipe(pipe, df, chunksize=2))
    rec('sync_none', lambda: conn.sync_pipe(pipe, None))
    rec('exists', lambda: conn.pipe_exists(pipe))
    rec('rowcount', lambda: conn.get_pipe_rowcount(pipe))
    rec('rowcount_begin', lambda: conn.get_pipe_rowcount(pipe, begin=datetime(2024, 1, 3)))
    rec('rowcount_end', lambda: conn.get_pipe_rowcount(pipe, end='2024-01-03'))
    rec('rowcount_params', lambda: conn.get_pipe_rowcount(pipe, params={'id': [0, 1]}))
    rec('data', lambda: conn.get_pipe_data(pipe))
    rec('data_select', lambda: conn.get_pipe_data(pipe, select_columns=['id', 'val'], params={'id': 3}))
    rec('data_omit', lambda: conn.get_pipe_data(pipe, omit_columns=['name'], end=datetime(2024, 1, 3)))
    rec('data_missing_params', lambda: conn.get_pipe_data(pipe, params={'id': 99}))
    rec('docs', lambda: conn.get_pipe_docs(pipe))
    rec('docs_desc', lambda: conn.get_pipe_docs(pipe, order='desc', limit=2))
    rec('docs_params', lambda: conn.get_pipe_docs(pipe, params={'id': 99}))
    rec('sync_time', lambda: conn.get_sync_time(pipe))
    rec('sync_time_oldest', lambda: conn.get_sync_time(pipe, newest=False))
    rec('sync_time_params', lambda: conn.get_sync_time(pipe, params={'id': 1}))
    rec('types', lambda: conn.get_pipe_columns_types(pipe))
    rec('indices_keys', lambda: sorted(conn.get_pipe_columns_indices(pipe) or {}))
    ### The v4 class does not expose `get_pipe_index_names` (its module defines it).
    rec('index_names_type', lambda: type(conn.get_pipe_index_names(pipe)).__name__
        if hasattr(conn, 'get_pipe_index_names') else 'dict')
    rec('size_type', lambda: type(conn.get_pipe_size(pipe)).__name__)
    rec('keys', lambda: sorted(
        (v[0], v[1], v[2])
        for v in conn.fetch_pipes_keys(connector_keys=['w16s'], metric_keys=['oracle' + tag]).values()
    ))
    rec('keys_none', lambda: conn.fetch_pipes_keys(metric_keys=['no_such_' + tag]))
    rec('clear_params', lambda: conn.clear_pipe(pipe, params={'id': 4}))
    rec('rowcount_after_clear', lambda: conn.get_pipe_rowcount(pipe))
    rec('clear_range', lambda: conn.clear_pipe(pipe, begin=datetime(2024, 1, 2), end=datetime(2024, 1, 3)))
    rec('rowcount_after_range', lambda: conn.get_pipe_rowcount(pipe))
    rec('delete_cache', lambda: conn.delete_pipe_cache(pipe))
    rec('drop', lambda: conn.drop_pipe(pipe))
    rec('exists_after_drop', lambda: conn.pipe_exists(pipe))
    rec('edit', lambda: conn.edit_pipe(pipe))
    rec('delete', lambda: conn.delete_pipe(pipe))
    rec('id_after_delete', lambda: conn.get_pipe_id(pipe))
    rec('attributes_after_delete', lambda: conn.get_pipe_attributes(pipe))

    for method, args in (
        ('vacuum_pipe', ()), ('analyze_pipe', ()), ('compress_pipe', ()),
        ('decompress_pipe', ()), ('partition_pipe', ()),
    ):
        rec(method, lambda: getattr(conn, method)(pipe))

    rec('actions', lambda: sorted(a for a in conn.get_actions().json() if a in ('show', 'sync', 'start')))
    rec('version', lambda: type(conn.get_mrsm_version()).__name__)
    rec('chaining', lambda: conn.get_chaining_status())
    rec('login', lambda: conn.login()[0])
    rec('test_connection', lambda: conn.test_connection())
    rec('users_has_admin', lambda: username in conn.get_users())
    rec('plugins_type', lambda: type(conn.get_plugins()).__name__)
    rec('job_exists_0', lambda: conn.get_job_exists('w16s-or-' + tag))
    rec('job_metadata_0', lambda: conn.get_job_metadata('w16s-or-' + tag))
    rec('job_status_0', lambda: conn.get_job_status('w16s-or-' + tag))
    rec('job_logs_0', lambda: conn.get_logs('w16s-or-' + tag))
    rec('job_create', lambda: conn.create_job('w16s-or-' + tag, ['show', 'version']))
    rec('job_exists', lambda: conn.get_job_exists('w16s-or-' + tag))
    rec('job_delete', lambda: conn.delete_job('w16s-or-' + tag))
    rec('job_delete_again', lambda: conn.delete_job('w16s-or-' + tag)[0])
    rec('job_stop_missing', lambda: conn.stop_job('w16s-or-' + tag))

    bad = APIConnector(
        'oraclebad' + tag, host=host, port=port, protocol='http',
        username='nobody', password='wrong',
    )
    rec('bad_login', lambda: bad.login(warn=False))
    rec('bad_exists', lambda: bad.pipe_exists(pipe))
    rec('bad_keys', lambda: bad.fetch_pipes_keys())

    dead = APIConnector('oracledead' + tag, host='127.0.0.1', port=1, protocol='http')
    rec('dead_exists', lambda: dead.pipe_exists(pipe))
    rec('dead_attributes', lambda: dead.get_pipe_attributes(pipe))
    rec('dead_rowcount', lambda: dead.get_pipe_rowcount(pipe))
    rec('dead_sync', lambda: dead.sync_pipe(pipe, df))
    rec('dead_version', lambda: dead.get_mrsm_version())
    rec('dead_chaining', lambda: dead.get_chaining_status())
    rec('dead_test', lambda: dead.test_connection())
    return out
'''


def _battery(server, runner):
    from tests.console.conftest import USERNAME, PASSWORD
    return runner(
        '127.0.0.1', server.port, USERNAME, PASSWORD, uuid.uuid4().hex[:6],
    )


def test_every_call_matches_the_v4_client_against_the_same_server(server):
    from tests.console.conftest import USERNAME, PASSWORD
    tag = uuid.uuid4().hex[:6]
    if not REF_PYTHON.exists():
        skip_or_fail("the 4.1.4 reference is not built (scripts/parity/setup_reference.sh)")
    ref = json.loads(_v4(
        BATTERY
        + f"\nprint('RESULT' + json.dumps(run('127.0.0.1', {server.port}, {USERNAME!r}, "
        + f"{PASSWORD!r}, {tag!r}), default=str))\n"
    ).split('RESULT')[-1])
    namespace = {}
    exec(BATTERY, namespace)
    ours = json.loads(json.dumps(
        namespace['run']('127.0.0.1', server.port, USERNAME, PASSWORD, tag), default=str,
    ))
    differences = {
        key: (ref.get(key), ours.get(key))
        for key in sorted(set(ref) | set(ours))
        if ref.get(key) != ours.get(key)
    }
    assert not differences, json.dumps(differences, indent=1, default=str)


BATTERY_ACCOUNTS = r"""
import json, re, uuid, tarfile, tempfile, pathlib
from datetime import datetime
from types import SimpleNamespace
import meerschaum as mrsm
from meerschaum.connectors.api import APIConnector
from meerschaum.core import User, Token


def norm(x):
    if isinstance(x, datetime):
        return 'dt'
    if isinstance(x, (list, tuple)):
        return [norm(i) for i in x]
    if isinstance(x, dict):
        return {str(k): norm(v) for k, v in x.items()}
    if hasattr(x, 'label') and hasattr(x, 'scopes'):
        return {'label': x.label, 'scopes': list(x.scopes), 'is_valid': x.is_valid}
    if x is None or isinstance(x, (bool, int, float, str)):
        return x
    return repr(x)


def run(host, port, username, password, tag):
    out = {}
    conn = APIConnector(
        'acc' + tag, host=host, port=port, protocol='http', username=username, password=password,
    )

    def rec(name, fn):
        try:
            out[name] = norm(fn())
        except Exception as e:
            kind = type(e).__name__
            out[name] = {'raised': 'Exception' if kind == 'RuntimeError' else kind}

    name = 'w16sacc' + tag
    user = User(name, 'pw-' + tag, instance=conn, email=name + '@example.com')
    rec('users_0', lambda: name in conn.get_users())
    rec('user_id_0', lambda: conn.get_user_id(user))
    rec('register_user', lambda: conn.register_user(user))
    rec('register_user_again', lambda: conn.register_user(user)[0])
    rec('users_1', lambda: name in conn.get_users())
    rec('user_id_type', lambda: type(conn.get_user_id(user)).__name__)
    rec('user_attributes', lambda: conn.get_user_attributes(user))
    rec('user_type', lambda: conn.get_user_type(user))
    rec('user_hash_type', lambda: type(conn.get_user_password_hash(user)).__name__)
    rec('edit_user', lambda: conn.edit_user(user))
    rec('delete_user', lambda: conn.delete_user(user))
    rec('users_2', lambda: name in conn.get_users())
    rec('delete_user_again', lambda: conn.delete_user(user)[0])

    tmp = pathlib.Path(tempfile.mkdtemp())
    pname = 'w16sp' + tag
    (tmp / (pname + '.py')).write_text("__version__ = '0.0.1'\n")
    archive = tmp / (pname + '.tar.gz')
    with tarfile.open(archive, 'w:gz') as tar:
        tar.add(tmp / (pname + '.py'), arcname=pname + '/__init__.py')
    class P:
        def __init__(self):
            self.name = pname
            self.version = '0.0.1'
            self.attributes = {'description': 'w16s'}
            self.user_id = None
            self.archive_path = archive
        def make_tar(self, debug=False):
            return archive
        def __str__(self):
            return pname
    plugin = P()
    rec('plugins_0', lambda: pname in conn.get_plugins())
    rec('plugin_version_0', lambda: conn.get_plugin_version(plugin))
    rec('plugin_attributes_0', lambda: conn.get_plugin_attributes(plugin))
    rec('register_plugin', lambda: conn.register_plugin(plugin))
    rec('register_plugin_again', lambda: conn.register_plugin(plugin)[0])
    rec('plugins_1', lambda: pname in conn.get_plugins())
    rec('plugins_search', lambda: pname in conn.get_plugins(search_term=pname[:6]))
    rec('plugins_search_no', lambda: pname in conn.get_plugins(search_term='zzz'))
    rec('plugin_version', lambda: conn.get_plugin_version(plugin))
    rec('plugin_attributes', lambda: conn.get_plugin_attributes(plugin))
    rec('delete_plugin', lambda: conn.delete_plugin(plugin))
    rec('plugins_2', lambda: pname in conn.get_plugins())
    rec('delete_plugin_again', lambda: conn.delete_plugin(plugin)[0])

    label = 'w16s-' + tag
    admin = User(username, password, instance=conn)
    token = Token(label, instance=conn, user=admin)
    rec('token_register', lambda: conn.register_token(token))
    rec('token_has_secret', lambda: bool(token.secret and token.id))
    rec('token_exists', lambda: conn.token_exists(token))
    rec('token_model', lambda: conn.get_token_model(token.id).label)
    rec('tokens', lambda: label in [t.label for t in conn.get_tokens()])
    rec('tokens_filtered', lambda: [t.label for t in conn.get_tokens(labels=[label])])
    rec('token_scopes', lambda: conn.get_token_scopes(token))
    def edited():
        loaded = conn.get_tokens(labels=[label])[0]
        loaded.scopes = ['pipes:read']
        return conn.edit_token(loaded)
    rec('token_edit', edited)
    rec('token_scopes_model', lambda: conn.get_token_model(token.id).scopes)
    rec('token_invalidate', lambda: conn.invalidate_token(token))
    rec('token_delete', lambda: conn.delete_token(token))
    rec('token_exists_after', lambda: conn.token_exists(token))
    return out
"""


def test_users_plugins_and_tokens_match_the_v4_client(server):
    from tests.console.conftest import USERNAME, PASSWORD
    tag = uuid.uuid4().hex[:6]
    if not REF_PYTHON.exists():
        skip_or_fail("the 4.1.4 reference is not built (scripts/parity/setup_reference.sh)")
    ref = json.loads(_v4(
        BATTERY_ACCOUNTS
        + f"\nprint('RESULT' + json.dumps(run('127.0.0.1', {server.port}, {USERNAME!r}, "
        + f"{PASSWORD!r}, {tag!r}), default=str))\n"
    ).split('RESULT')[-1])
    namespace = {}
    exec(BATTERY_ACCOUNTS, namespace)
    ours = json.loads(json.dumps(
        namespace['run']('127.0.0.1', server.port, USERNAME, PASSWORD, tag), default=str,
    ))
    ### PARITY W17T-004, the one intentional deviation: v4's `get_token_scopes()` returned `[]` for an
    ### existing token; v5 returns its real scopes. Every other key stays strict.
    from meerschaum._internal.static import STATIC_CONFIG
    assert ref.get('token_scopes') == []
    assert ours.get('token_scopes') == list(STATIC_CONFIG['tokens']['scopes'])
    differences = {
        key: (ref.get(key), ours.get(key))
        for key in sorted(set(ref) | set(ours))
        if key != 'token_scopes' and ref.get(key) != ours.get(key)
    }
    assert not differences, json.dumps(differences, indent=1, default=str)


# ---------------------------------------------------------------------------------------------
# Native calls share the connector's client (one login, one warning per process)


@pytest.fixture
def fake_api():
    """
    A loopback server which counts `POST /login` requests and answers the few pipe routes a
    read needs. `state['reject']` makes every login a 401.
    """
    import threading
    from http.server import BaseHTTPRequestHandler, HTTPServer
    from urllib.parse import urlparse

    state = {'logins': 0, 'reject': False, 'requests': [], 'last_login': {}}

    class Handler(BaseHTTPRequestHandler):
        def log_message(self, *args):
            pass

        def _send(self, status, body):
            data = json.dumps(body).encode()
            self.send_response(status)
            self.send_header('Content-Type', 'application/json')
            self.send_header('Content-Length', str(len(data)))
            self.end_headers()
            self.wfile.write(data)

        def do_POST(self):
            length = int(self.headers.get('Content-Length') or 0)
            body = self.rfile.read(length)
            if urlparse(self.path).path.endswith('/login'):
                state['logins'] += 1
                from urllib.parse import parse_qs
                form = parse_qs(body.decode())
                state['last_login'] = {k: v[0] for k, v in form.items()}
                if state['reject']:
                    return self._send(401, {'detail': 'Invalid credentials'})
                return self._send(200, {
                    'access_token': 'token', 'token_type': 'bearer',
                    'expires': '2099-01-01T00:00:00.000000',
                })
            self._send(404, {'detail': 'no'})

        def do_GET(self):
            path = urlparse(self.path).path
            state['requests'].append(path)
            if path.endswith('/exists'):
                return self._send(200, True)
            if path.endswith('/rowcount'):
                return self._send(200, 3)
            if path.endswith('/sync_time'):
                return self._send(200, None)
            if path.endswith('/attributes'):
                return self._send(200, {'parameters': {'columns': {'datetime': 'dt'}}})
            if path.endswith('/columns/types'):
                return self._send(200, {'dt': 'TIMESTAMP', 'v': 'BIGINT'})
            self._send(404, {'detail': 'no'})

    server = HTTPServer(('127.0.0.1', 0), Handler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    state['port'] = server.server_address[1]
    yield state
    server.shutdown()
    server.server_close()


def _native_reads(pipe):
    return [pipe.get_rowcount(), pipe.get_sync_time(), pipe.exists(), pipe.get_rowcount()]


def test_native_calls_log_in_once_per_process(fake_api):
    conn = _connector(
        'w16s_login_once', host='127.0.0.1', port=fake_api['port'], protocol='http',
        username='u', password='p',
    )
    pipe = mrsm.Pipe('w16s', 'login_once', instance=conn, cache=False)
    for _ in range(3):
        assert pipe.get_rowcount() == 3
        pipe.get_sync_time()
        pipe.exists()
    assert fake_api['logins'] == 1, fake_api
    # The connector's own methods use the same client, hence the same token.
    assert conn.pipe_exists(pipe) is True
    assert fake_api['logins'] == 1


def test_native_calls_warn_about_a_failed_login_once(fake_api, capsys):
    fake_api['reject'] = True
    conn = _connector(
        'w16s_warn_once', host='127.0.0.1', port=fake_api['port'], protocol='http',
        username='u', password='p',
    )
    pipe = mrsm.Pipe('w16s', 'warn_once', instance=conn, cache=False)
    for _ in range(4):
        _native_reads(pipe)
    assert capsys.readouterr().err.count('Failed to log into') == 1


def test_a_pipe_on_a_connector_with_other_attributes_gets_its_own_client(fake_api):
    """The shared client is the Python connector's with the same attributes, never another's."""
    one = _connector(
        'w16s_same_label', host='127.0.0.1', port=fake_api['port'], protocol='http',
        username='u', password='p',
    )
    pipe = mrsm.Pipe('w16s', 'own_client', instance=one, cache=False)
    assert pipe.get_rowcount() == 3
    assert fake_api['logins'] == 1
    two = _connector(
        'w16s_same_label_2', host='127.0.0.1', port=fake_api['port'], protocol='http',
        username='other', password='p',
    )
    assert mrsm.Pipe('w16s', 'own_client', instance=two, cache=False).get_rowcount() == 3
    assert fake_api['logins'] == 2


# ---------------------------------------------------------------------------------------------
# `Pipe.get_data()` dtypes against the v4 oracle

PIPE_BATTERY = r"""
import json
from datetime import datetime
from decimal import Decimal
import meerschaum as mrsm


def run(host, port, username, password, tag):
    conn = mrsm.get_connector(
        'api', 'w16sdt' + tag, host=host, port=port, protocol='http',
        username=username, password=password,
    )
    docs = [
        {
            'dt': datetime(2024, 1, 1 + i), 'id': i, 'val': i / 2, 'n': None if i == 2 else i,
            'name': None if i == 1 else 'x', 'b': i % 2 == 0, 'js': {'a': i},
            'num': Decimal('1.%d' % i),
        }
        for i in range(4)
    ]
    declared = {
        'dt': 'datetime', 'id': 'int', 'val': 'float', 'n': 'int', 'name': 'str', 'b': 'bool',
        'js': 'json', 'num': 'numeric',
    }
    out = {}
    for label, dtypes in (('declared', declared), ('inferred', None)):
        keys = dict(columns={'datetime': 'dt', 'id': 'id'})
        writer = mrsm.Pipe('w16s', 'dt' + label + tag, instance=conn, **keys, **(
            {'dtypes': dtypes} if dtypes else {}
        ))
        writer.sync(docs)
        pipe = mrsm.Pipe('w16s', 'dt' + label + tag, instance=conn, **keys)
        out[label + '_pipe_dtypes'] = {c: str(t) for c, t in pipe.dtypes.items()}
        for name, kwargs in (
            ('df', {}), ('select', {'select_columns': ['id', 'val']}),
            ('params', {'params': {'id': 3}}), ('chunks', {'as_iterator': True}),
        ):
            result = pipe.get_data(**kwargs)
            if kwargs.get('as_iterator'):
                result = next(iter(result))
            out[label + '_' + name] = {c: str(t) for c, t in result.dtypes.items()}
        writer.delete()
    return out
"""


def test_get_data_dtypes_match_the_v4_client(server):
    """
    The dtype names depend on the pandas major: the reference runs the pandas of `.venv-ref` (3.x)
    and the façade runs in a child of the test interpreter, whose pandas must be 3.x too (the
    `pandas` extra). Neither needs a prepared `test_root/venvs`: both find their packages in
    their own interpreter.
    """
    from tests.console.conftest import USERNAME, PASSWORD
    if not REF_PYTHON.exists():
        skip_or_fail("the 4.1.4 reference is not built (scripts/parity/setup_reference.sh)")
    pandas = pytest.importorskip('pandas')
    if int(pandas.__version__.split('.')[0]) < 3:
        pytest.skip(f"the reference runs pandas 3 (pandas {pandas.__version__} is installed)")
    tag = uuid.uuid4().hex[:6]
    code = (
        PIPE_BATTERY
        + f"\nprint('RESULT' + json.dumps(run('127.0.0.1', {server.port}, {USERNAME!r}, "
        + f"{PASSWORD!r}, {tag!r})))\n"
    )
    ref = json.loads(_v4(code).split('RESULT')[-1])
    result = _run_child(code.replace(f"{tag!r}", repr(tag + 'x')))
    assert 'RESULT' in result.stdout, result.stderr[-3000:]
    ours = json.loads(result.stdout.split('RESULT')[-1])
    differences = {
        key: (ref.get(key), ours.get(key))
        for key in sorted(set(ref) | set(ours))
        if ref.get(key) != ours.get(key)
    }
    assert not differences, json.dumps(differences, indent=1)


def test_changing_the_credentials_of_a_live_connector_logs_in_again(fake_api):
    conn = _connector(
        'w16s_new_password', host='127.0.0.1', port=fake_api['port'], protocol='http',
        username='u', password='first',
    )
    pipe = mrsm.Pipe('w16s', 'new_password', instance=conn, cache=False)
    assert pipe.get_rowcount() == 3
    assert (fake_api['logins'], fake_api['last_login']['password']) == (1, 'first')
    # The connector's own methods and the native flow both use the changed attributes.
    conn.password = 'second'
    assert conn.pipe_exists(pipe) is True
    assert (fake_api['logins'], fake_api['last_login']['password']) == (2, 'second')
    assert pipe.get_rowcount() == 3
    assert fake_api['logins'] == 2
    conn.username = 'other'
    assert pipe.get_rowcount() == 3
    assert (fake_api['logins'], fake_api['last_login']['username']) == (3, 'other')
    assert conn.URI.startswith('http://other:second@')
