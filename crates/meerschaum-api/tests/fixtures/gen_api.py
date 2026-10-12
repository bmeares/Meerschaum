#! /usr/bin/env python3
# vim:fenc=utf-8

"""
Generate the HTTP contract fixtures for `meerschaum-api`.

Starts the Python API server (v4.1.4, this repository) on a free port against a fresh sqlite
instance in a temporary root directory, drives the Python `APIConnector` through a synthetic
sequence, and records every request it makes (method, URL, headers minus auth and transport
defaults, body) with the server's responses, plus each method's return value and the logs
websocket traffic. The Rust contract tests replay the same sequence against a mock server that
serves these responses, and assert identical requests and parsed results.

Usage (from the repository root):

    PYTHONPATH=$(pwd) python crates/meerschaum-api/tests/fixtures/gen_api.py
    PYTHONPATH=$(pwd) python crates/meerschaum-api/tests/fixtures/gen_api.py --serve

`--serve` only sets up the root and the server and prints `MRSM_API_CONTRACT_URL=...`, for
running the Rust live contract test against a real server; it stops on Ctrl-C or EOF.

The server (and its process group) is killed on exit, and the temporary root is removed.
All data is synthetic.
"""

import os
import sys
import json
import time
import shutil
import signal
import socket
import atexit
import base64
import pathlib
import tempfile
import subprocess

HERE = pathlib.Path(__file__).resolve().parent
REPO = HERE.parents[3]
OUT = HERE / 'api_sequence.json'
USERNAME = 'fixture_admin'
PASSWORD = 'fixture_pass'


def free_port() -> int:
    with socket.socket() as s:
        s.bind(('127.0.0.1', 0))
        return s.getsockname()[1]


### Phase 1: create the root and re-exec with the environment set before `meerschaum` imports.
if 'MRSM_API_FIXTURE_ROOT' not in os.environ:
    root = tempfile.mkdtemp(prefix='mrsm-api-fixture-')
    env = dict(os.environ)
    env.update({
        'MRSM_API_FIXTURE_ROOT': root,
        'MRSM_ROOT_DIR': root,
        'MRSM_PLUGINS_DIR': str(pathlib.Path(root) / 'plugins'),
        ### Reuse the repository's test venvs so nothing is installed.
        'MRSM_VENVS_DIR': str(REPO / 'test_root' / 'venvs'),
        'MRSM_SQL_FIXTURE': f"sqlite:///{root}/fixture.db",
        'PYTHONPATH': str(REPO),
        'MRSM_NONINTERACTIVE': 'true',
    })
    plugin_dir = pathlib.Path(root) / 'plugins' / 'fixture_plugin'
    plugin_dir.mkdir(parents=True, exist_ok=True)
    (plugin_dir / '__init__.py').write_text("__version__ = '0.1.0'\n")
    try:
        rc = subprocess.call([sys.executable, __file__] + sys.argv[1:], env=env)
    finally:
        if not os.environ.get('MRSM_API_FIXTURE_KEEP'):
            shutil.rmtree(root, ignore_errors=True)
        else:
            print(f"Kept {root}.")
    sys.exit(rc)

ROOT = pathlib.Path(os.environ['MRSM_API_FIXTURE_ROOT'])
SERVE_ONLY = '--serve' in sys.argv

import meerschaum as mrsm  # noqa: E402
from meerschaum.core import User  # noqa: E402

_server = {'proc': None}


def kill_server():
    proc = _server['proc']
    if proc is None or proc.poll() is not None:
        return
    try:
        os.killpg(proc.pid, signal.SIGTERM)
        proc.wait(timeout=15)
    except Exception:
        try:
            os.killpg(proc.pid, signal.SIGKILL)
        except Exception:
            pass


atexit.register(kill_server)
signal.signal(signal.SIGTERM, lambda *a: sys.exit(1))


def start_server() -> int:
    instance = mrsm.get_connector('sql:fixture')
    user = User(USERNAME, PASSWORD, type='admin', instance=instance)
    success, msg = instance.register_user(user)
    assert success, msg
    port = free_port()
    log = open(ROOT / 'api.log', 'w')
    _server['proc'] = subprocess.Popen(
        [
            sys.executable, '-m', 'meerschaum', 'start', 'api',
            '-p', str(port), '-i', 'sql:fixture', '-w', '1',
            '--no-dash', '--no-webterm', '-y',
        ],
        env=os.environ,
        stdout=log,
        stderr=subprocess.STDOUT,
        start_new_session=True,
    )
    import requests
    deadline = time.time() + 180
    while time.time() < deadline:
        if _server['proc'].poll() is not None:
            raise RuntimeError("API server exited:\n" + (ROOT / 'api.log').read_text())
        try:
            if requests.get(f"http://localhost:{port}/healthcheck", timeout=2).ok:
                return port
        except Exception:
            pass
        time.sleep(0.5)
    raise RuntimeError("API server did not start:\n" + (ROOT / 'api.log').read_text())


SKIP_HEADERS = {
    'authorization', 'user-agent', 'accept-encoding', 'accept', 'connection', 'content-length',
}
REQUESTS = []
WEBSOCKETS = []


def body_repr(body):
    if body is None:
        return None
    if isinstance(body, str):
        body = body.encode('utf-8')
    try:
        text = body.decode('utf-8')
        if '\x00' not in text:
            return {'text': text}
    except UnicodeDecodeError:
        pass
    return {'base64': base64.b64encode(body).decode()}


def install_recorder():
    import requests
    original_send = requests.Session.send

    def send(self, request, **kwargs):
        response = original_send(self, request, **kwargs)
        content_type = response.headers.get('content-type', '')
        is_text = 'json' in content_type or content_type.startswith('text/')
        REQUESTS.append({
            'method': request.method,
            'url': request.url,
            'headers': {
                k: v for k, v in request.headers.items() if k.lower() not in SKIP_HEADERS
            },
            'has_auth': 'Authorization' in request.headers,
            'body': body_repr(request.body),
            'status': response.status_code,
            'content_type': content_type,
            'response': (
                {'text': response.text}
                if is_text
                else {'base64': base64.b64encode(response.content).decode()}
            ),
        })
        return response

    requests.Session.send = send

    _ = mrsm.attempt_import('websockets').connect
    websockets = sys.modules['websockets']
    original_connect = websockets.connect

    class Proxy:
        def __init__(self, uri, *args, **kwargs):
            self.uri = uri
            self.cm = original_connect(uri, *args, **kwargs)
            self.events = []
            WEBSOCKETS.append({'uri': uri, 'events': self.events})

        async def __aenter__(self):
            self.ws = await self.cm.__aenter__()
            return self

        async def __aexit__(self, *args):
            return await self.cm.__aexit__(*args)

        async def send(self, data):
            self.events.append(['send', data if len(WEBSOCKETS[-1]['events']) else '<token>'])
            return await self.ws.send(data)

        async def recv(self):
            data = await self.ws.recv()
            self.events.append(['recv', data])
            return data

        async def close(self):
            self.events.append(['close', None])
            return await self.ws.close()

    websockets.connect = Proxy


def tag(x):
    """Encode Python values as JSON with tags for rich scalars (see `gen_valkey.py`)."""
    import math
    import uuid
    import decimal
    from datetime import date
    import pandas as pd
    if isinstance(x, pd.DataFrame):
        return {'__df__': {
            'columns': [str(c) for c in x.columns],
            'dtypes': {str(c): str(t) for c, t in x.dtypes.items()},
            'data': {str(c): [tag(v) for v in x[c].tolist()] for c in x.columns},
        }}
    if isinstance(x, mrsm.core.Token):
        return {'__token__': {
            'id': str(x.id) if x.id else None,
            'label': x.label,
            'secret': x.secret,
            'scopes': list(x.scopes or []),
            'is_valid': x.is_valid,
            'expiration': x.expiration.isoformat() if x.expiration else None,
        }}
    if hasattr(x, 'model_dump'):
        return tag(x.model_dump())
    if x is None or isinstance(x, (bool, int, str)):
        return x
    if x is pd.NaT:
        return {'t': 'nat'}
    if x is pd.NA:
        return {'t': 'na'}
    if isinstance(x, float):
        return {'t': 'nan'} if math.isnan(x) else x
    if hasattr(x, 'tzinfo') and hasattr(x, 'isoformat'):
        return {'t': 'dt', 'v': x.isoformat()}
    if isinstance(x, date):
        return {'t': 'date', 'v': x.isoformat()}
    if isinstance(x, decimal.Decimal):
        return {'t': 'dec', 'v': str(x)}
    if isinstance(x, uuid.UUID):
        return {'t': 'uuid', 'v': str(x)}
    if isinstance(x, (list, tuple)):
        return [tag(v) for v in x]
    if isinstance(x, dict):
        return {str(k): tag(v) for k, v in x.items()}
    if hasattr(x, 'item'):
        return tag(x.item())
    return {'t': 'repr', 'v': repr(x)}


def main_record(port: int):
    import pandas as pd
    from datetime import datetime, timezone
    from meerschaum.core import Token
    install_recorder()

    conn = mrsm.get_connector(
        'api:fixture', host='localhost', port=port, username=USERNAME, password=PASSWORD,
    )
    steps = []

    def step(name, fn, **extra):
        REQUESTS.clear()
        WEBSOCKETS.clear()
        try:
            result = fn()
            error = None
        except Exception as e:
            result = None
            error = f"{type(e).__name__}: {e}"
        steps.append({
            'step': name,
            'result': tag(result),
            'error': error,
            'requests': list(REQUESTS),
            'websockets': list(WEBSOCKETS),
            **extra,
        })
        return result

    step('login', lambda: conn.login())
    step('get_mrsm_version', lambda: conn.get_mrsm_version())
    step('get_chaining_status', lambda: conn.get_chaining_status())
    step('get_actions', lambda: conn.get_actions().json())
    for path in ('/version', '/info', '/healthcheck', '/connectors', '/users/me', '/id'):
        step(f'raw_get{path.replace("/", "_")}', lambda path=path: conn.get(path).json())
    step('create_metadata', lambda: conn.create_metadata())

    pipe = mrsm.Pipe(
        'demo', 'temperature', 'home',
        instance=conn,
        columns={'datetime': 'ts', 'id': 'station'},
        tags=['outdoor'],
    )
    nloc = mrsm.Pipe('demo', 'humidity', instance=conn, columns={'id': 'room'})
    step('register_pipe', lambda: conn.register_pipe(pipe))
    step('register_pipe_again', lambda: conn.register_pipe(pipe))
    step('register_pipe_null_location', lambda: conn.register_pipe(nloc))
    step('mrsm_connector_403', lambda: conn.register_pipe(mrsm.Pipe('mrsm', 'secrets', instance=conn)))
    step('fetch_pipes_keys', lambda: conn.fetch_pipes_keys())
    step('fetch_pipes_keys_filtered', lambda: conn.fetch_pipes_keys(
        connector_keys=['demo'], metric_keys=['temperature'], location_keys=['home'],
        tags=['outdoor'], params={'connector_keys': 'demo'},
    ))
    step('get_pipe_id', lambda: conn.get_pipe_id(pipe))
    step('get_pipe_attributes', lambda: conn.get_pipe_attributes(pipe))
    step('pipe_exists_before', lambda: conn.pipe_exists(pipe))

    df = pd.DataFrame({
        'ts': pd.to_datetime(
            ['2024-01-01 00:00:00', '2024-01-01 00:00:00', '2024-01-02 12:30:00.250'],
            utc=True,
            format='ISO8601',
        ),
        'station': ['a', 'b/c', 'é'],
        'val': [1.5, 0.1, 1e20],
        'n': pd.array([1, None, 3], dtype='Int64'),
    })
    sync_kwargs = {
        'begin': '', 'end': None, 'force': False, 'retries': 10, 'min_seconds': 1,
        'check_existing': True, 'blocking': True, 'workers': None, 'callback': None,
        'error_callback': None, 'sync_chunks': True, 'safe_copy': True,
    }
    step(
        'sync_pipe_chunked',
        lambda: conn.sync_pipe(pipe, df, chunksize=2, **sync_kwargs),
        sync_df=tag(df),
        sync_kwargs=sync_kwargs,
        chunksize=2,
    )
    df2 = pd.DataFrame({
        'ts': pd.to_datetime(['2024-01-03'], utc=True),
        'station': ['a'],
        'val': [-4.0],
        'n': pd.array([4], dtype='Int64'),
    })
    step(
        'sync_pipe_default_chunks',
        lambda: conn.sync_pipe(pipe, df2, check_existing=False),
        sync_df=tag(df2),
        sync_kwargs={'check_existing': False},
        chunksize=-1,
    )
    step('pipe_exists_after', lambda: conn.pipe_exists(pipe))
    step('get_sync_time_newest', lambda: conn.get_sync_time(pipe))
    step('get_sync_time_oldest', lambda: conn.get_sync_time(pipe, newest=False))
    step('get_sync_time_params', lambda: conn.get_sync_time(pipe, params={'station': 'a'}))
    step('get_pipe_rowcount', lambda: conn.get_pipe_rowcount(pipe))
    step('get_pipe_rowcount_filtered', lambda: conn.get_pipe_rowcount(
        pipe,
        begin=datetime(2024, 1, 2, tzinfo=timezone.utc),
        params={'station': ['a', 'é']},
    ))
    step('get_pipe_data', lambda: conn.get_pipe_data(pipe))
    step('get_pipe_data_filtered', lambda: conn.get_pipe_data(
        pipe,
        select_columns=['ts', 'station', 'val'],
        begin=datetime(2024, 1, 1, tzinfo=timezone.utc),
        end=datetime(2024, 1, 3, tzinfo=timezone.utc),
        params={'station': '_b/c'},
    ))
    step('get_pipe_docs', lambda: conn.get_pipe_docs(pipe, params={'station': 'a'}, limit=5))
    step('get_pipe_columns_types', lambda: conn.get_pipe_columns_types(pipe))
    step('get_pipe_columns_indices', lambda: conn.get_pipe_columns_indices(pipe))
    step('get_pipe_index_names', lambda: conn.get_pipe_index_names(pipe))
    step('raw_get_indices_names', lambda: conn.get('/pipes/demo/temperature/home/indices/names').json())
    step('get_pipe_size', lambda: conn.get_pipe_size(pipe))
    step('vacuum_pipe', lambda: conn.vacuum_pipe(pipe, full=True))
    step('analyze_pipe', lambda: conn.analyze_pipe(pipe))
    step('compress_pipe', lambda: conn.compress_pipe(pipe))
    step('decompress_pipe', lambda: conn.decompress_pipe(pipe, no_policy=True))
    step('partition_pipe', lambda: conn.partition_pipe(pipe, chunk_minutes=1440))
    step('raw_get_chunk_bounds', lambda: conn.get(
        '/pipes/demo/temperature/home/chunk_bounds',
        params={'begin': '2024-01-01', 'end': '2024-01-03', 'chunk_interval_minutes': 1440},
    ).json())
    step('raw_get_csv', lambda: conn.get('/pipes/demo/temperature/home/csv').text)

    pipe.parameters['tags'] = ['outdoor', 'edited']
    step('edit_pipe', lambda: conn.edit_pipe(pipe))
    step('edit_pipe_patch', lambda: conn.edit_pipe(pipe, patch=True))
    step('clear_pipe', lambda: conn.clear_pipe(
        pipe, begin=datetime(2024, 1, 2, tzinfo=timezone.utc), params={'station': 'a'},
    ))
    step('delete_pipe_cache', lambda: conn.delete_pipe_cache(pipe))
    step('drop_pipe', lambda: conn.drop_pipe(pipe))
    step('delete_pipe', lambda: conn.delete_pipe(pipe))
    step('delete_pipe_null_location', lambda: conn.delete_pipe(nloc))

    new_user = User('fixture_user', 'fixture_user_pass', email='u@example.com', attributes={'a': 1})
    step('register_user', lambda: conn.register_user(new_user))
    step('get_users', lambda: conn.get_users())
    step('get_user_id', lambda: conn.get_user_id(new_user))
    step('get_user_attributes', lambda: conn.get_user_attributes(new_user))
    step('get_user_type', lambda: conn.get_user_type(new_user))
    step('get_user_password_hash', lambda: conn.get_user_password_hash(new_user))
    new_user.email = 'new@example.com'
    new_user.type = 'basic'
    step('edit_user', lambda: conn.edit_user(new_user))
    step('delete_user', lambda: conn.delete_user(new_user))

    expiration = datetime(2030, 1, 1, tzinfo=timezone.utc)
    token = Token(
        label='fixture-token',
        scopes=['pipes:read', 'pipes:write'],
        expiration=expiration,
        instance=conn,
    )
    step('register_token', lambda: conn.register_token(token))

    ### PARITY: `register_token` posts JSON without a Content-Type, which this FastAPI rejects
    ### (500). Register the token on the instance directly to exercise the other token routes.
    sql_conn = mrsm.get_connector('sql:fixture')
    direct = Token(
        label='fixture-token',
        user=User(USERNAME, instance=sql_conn),
        scopes=['pipes:read', 'pipes:write'],
        expiration=expiration,
        instance=sql_conn,
    )
    assert direct.register()[0]
    token = Token(
        id=direct.id,
        label='fixture-token',
        scopes=['pipes:read', 'pipes:write'],
        expiration=expiration,
        instance=conn,
    )
    step('get_tokens', lambda: [t.to_model() for t in conn.get_tokens()])
    step('get_tokens_labels', lambda: [t.to_model() for t in conn.get_tokens(labels=['fixture-token'])])
    step('get_token_model', lambda: conn.get_token_model(token.id))
    _model = conn.get_token_model(token.id)
    token.creation = _model.creation if _model is not None else None
    token.label = 'fixture-token-edited'
    step('edit_token', lambda: conn.edit_token(token))
    step('token_exists', lambda: conn.token_exists(token.id))
    step('get_token_scopes', lambda: conn.get_token_scopes(token.id))

    cc = mrsm.get_connector(
        'api:fixture_cc', host='localhost', port=port,
        client_id=str(direct.id), client_secret=direct.secret,
    )
    step(
        'login_client_credentials',
        lambda: cc.login(),
        client_id=str(direct.id),
        client_secret=direct.secret,
    )
    step('client_credentials_request', lambda: cc.fetch_pipes_keys())
    ak = mrsm.get_connector('api:fixture_ak', host='localhost', port=port, api_key=direct.get_api_key())
    step('login_api_key', lambda: ak.login(), api_key=direct.get_api_key())
    step('api_key_request', lambda: ak.get_users())

    step('invalidate_token', lambda: conn.invalidate_token(token))
    step('delete_token', lambda: conn.delete_token(token))
    step('api_key_validate', lambda: conn.post(
        '/tokens/validate',
        headers={'Authorization': 'Bearer mrsm-key:aW52YWxpZA=='},
        use_token=False,
    ).json())

    plugin = mrsm.Plugin('fixture_plugin')
    archive_path = plugin.make_tar()
    archive_b64 = base64.b64encode(pathlib.Path(archive_path).read_bytes()).decode()
    step(
        'register_plugin',
        lambda: conn.register_plugin(plugin, make_archive=False),
        archive=archive_b64,
        plugin={'name': 'fixture_plugin', 'version': plugin.version, 'attributes': plugin.attributes},
    )
    step('get_plugins', lambda: conn.get_plugins())
    step('get_plugins_search', lambda: conn.get_plugins(search_term='fix'))
    step('get_plugin_attributes', lambda: conn.get_plugin_attributes(plugin))
    step('get_plugin_version', lambda: conn.get_plugin_version(plugin))
    step('raw_download_plugin', lambda: len(conn.get('/plugins/fixture_plugin').content))
    step('delete_plugin', lambda: conn.delete_plugin(plugin))

    step('create_job', lambda: conn.create_job('fixture-job', ['show', 'version'], properties={'tags': ['x']}))
    step('get_job_exists', lambda: conn.get_job_exists('fixture-job'))
    step('start_job', lambda: conn.start_job('fixture-job'))
    lines = []
    results = []

    def monitor():
        conn.monitor_logs(
            'fixture-job',
            lines.append,
            input_callback_function=lambda: '',
            stop_callback_function=results.append,
            stop_on_exit=True,
        )
        return {'lines': len(lines), 'result': results[0] if results else None}

    step('monitor_logs', monitor)
    step('get_jobs', lambda: sorted(conn.get_jobs().keys()))
    step('get_job_metadata', lambda: sorted(conn.get_job_metadata('fixture-job').keys()))
    step('get_logs', lambda: isinstance(conn.get_logs('fixture-job'), str))
    step('get_job_stop_time', lambda: conn.get_job_stop_time('fixture-job') is not None)
    step('get_job_is_blocking_on_stdin', lambda: conn.get_job_is_blocking_on_stdin('fixture-job'))
    from meerschaum.connectors.api._jobs import get_job_prompt_kwargs
    step('get_job_prompt_kwargs', lambda: get_job_prompt_kwargs(conn, 'fixture-job'))
    step('pause_job', lambda: conn.pause_job('fixture-job'))
    step('stop_job', lambda: conn.stop_job('fixture-job'))
    step('delete_job', lambda: conn.delete_job('fixture-job'))
    step('get_job_missing', lambda: conn.get_job_metadata('missing-job'))
    step('do_action_legacy', lambda: conn.do_action_legacy(['show', 'version']))

    OUT.write_text(json.dumps({
        'username': USERNAME,
        'password': PASSWORD,
        'steps': steps,
    }, indent=2) + '\n')
    print(f"Wrote {len(steps)} steps to {OUT}.")


def main():
    port = start_server()
    if SERVE_ONLY:
        print(
            f"MRSM_API_CONTRACT_URL=http://{USERNAME}:{PASSWORD}@localhost:{port}",
            flush=True,
        )
        try:
            sys.stdin.read()
        except KeyboardInterrupt:
            pass
        return
    main_record(port)


if __name__ == '__main__':
    try:
        main()
    finally:
        kill_server()
