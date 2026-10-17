#! /usr/bin/env python3
# vim:fenc=utf-8

"""
Record the HTTP contract of the Python v4 API server for `meerschaum-server` (§14, "Server
contract tests").

Starts the Python API server on a free port against a fresh sqlite instance in a temporary root
(see `_pyserver.py`), drives it with the Python `APIConnector` (and raw `requests` for the error
cases) through a synthetic scenario, and records every request/response pair: method, path,
query, headers (minus auth and transport defaults), body, status, content type and body.

Volatile values are normalized the same way the Rust replay normalizes its own responses
(`tests/contract.rs`):

- credentials minted during the run (JWTs, token IDs, secrets, API keys) become `{slot}`
  placeholders in requests and responses; the replay captures its own values into the slots;
- the values of `access_token`, `expires`, `creation`, `secret` and `api_key` become `<volatile>`;
- ISO 8601 timestamps are rewritten to one canonical form (the instant, with or without an
  offset), so `...+00:00` and `...Z` renderings compare equal;
- the server version becomes `<version>`.

Each exchange carries a comparison mode: `exact` (status and normalized body), `shape` (status
and the JSON keys/types), `status`, or `skip`, with the reason when it is not `exact` (the
sqlite instance's own behavior, which the in-memory test instance does not reproduce).

Usage (from the repository root):

    PYTHONPATH=$(pwd) python crates/meerschaum-server/tests/fixtures/gen_contract.py
    scripts/parity/run.sh crates/meerschaum-server/tests/fixtures/gen_contract.py

All data is synthetic.
"""

import io
import re
import sys
import json
import gzip
import base64
import pathlib
import tarfile

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parent))
import _pyserver  # noqa: E402

_pyserver.reexec_in_temp_root(__file__)

OUT = _pyserver.HERE / 'contract.json'
SKIP_HEADERS = {
    'authorization', 'user-agent', 'accept-encoding', 'accept', 'connection', 'content-length',
}
VOLATILE_KEYS = {'access_token', 'expires', 'creation', 'secret', 'api_key'}
ISO_RE = re.compile(
    r'^(\d{4}-\d{2}-\d{2})[T ](\d{2}:\d{2}:\d{2})(\.\d+)?(Z|[+-]\d{2}:?\d{2})?$'
)


def canonical_datetime(s: str):
    """Rewrite an ISO timestamp as `YYYY-MM-DDTHH:MM:SS.ffffff[+00:00]` (UTC when aware)."""
    from datetime import datetime, timezone
    m = ISO_RE.match(s)
    if not m:
        return None
    date, time_, frac, tz = m.groups()
    frac = (frac or '.0')[1:]
    micros = int((frac + '000000')[:6])
    dt = datetime.fromisoformat(f"{date}T{time_}").replace(microsecond=micros)
    if tz:
        tz = '+00:00' if tz == 'Z' else tz
        if len(tz) == 5:
            tz = tz[:3] + ':' + tz[3:]
        dt = datetime.fromisoformat(dt.isoformat() + tz).astimezone(timezone.utc)
        return dt.strftime('%Y-%m-%dT%H:%M:%S.%f') + '+00:00'
    return dt.strftime('%Y-%m-%dT%H:%M:%S.%f')


class Slots:
    """Credentials minted during the run, replaced by `{name}` placeholders."""

    def __init__(self):
        self.values = {}

    def set(self, name, value):
        if value:
            self.values[name] = str(value)

    def sub_text(self, text: str) -> str:
        for name, value in sorted(self.values.items(), key=lambda kv: -len(kv[1])):
            text = text.replace(value, '{' + name + '}')
        return text


SLOTS = Slots()
VERSION = None


def normalize(x, key=None):
    if isinstance(x, dict):
        return {k: normalize(v, k) for k, v in x.items()}
    if isinstance(x, list):
        return [normalize(v) for v in x]
    if isinstance(x, str):
        if key in VOLATILE_KEYS:
            return '<volatile>'
        if VERSION and x == VERSION:
            return '<version>'
        x = SLOTS.sub_text(x)
        c = canonical_datetime(x)
        return c if c is not None else x
    if key in VOLATILE_KEYS and x is not None:
        return '<volatile>'
    return x


EXCHANGES = []
CURRENT = {'step': None, 'compare': 'exact', 'reason': None, 'capture': {}, 'auth_slot': None}
AUTH_SLOTS = {}


def auth_of(headers) -> object:
    value = headers.get('Authorization')
    if value is None:
        return None
    if value in AUTH_SLOTS:
        return {'slot': AUTH_SLOTS[value]}
    return {'raw': SLOTS.sub_text(value)}


def body_of(body):
    if body is None:
        return None
    if isinstance(body, str):
        body = body.encode('utf-8')
    try:
        text = body.decode('utf-8')
        if '\x00' not in text:
            return {'text': SLOTS.sub_text(text)}
    except UnicodeDecodeError:
        pass
    return {'base64': base64.b64encode(body).decode()}


def install_recorder():
    import requests
    from urllib.parse import urlsplit
    original_send = requests.Session.send

    def send(self, request, **kwargs):
        response = original_send(self, request, **kwargs)
        parts = urlsplit(request.url)
        content_type = response.headers.get('content-type', '')
        exchange = {
            'step': CURRENT['step'],
            'method': request.method,
            'path': SLOTS.sub_text(parts.path),
            'query': SLOTS.sub_text(parts.query),
            'headers': {
                k.lower(): v for k, v in request.headers.items()
                if k.lower() not in SKIP_HEADERS
            },
            'auth': auth_of(request.headers),
            'body': body_of(request.body),
            'status': response.status_code,
            'content_type': content_type.split(';')[0],
            'compare': CURRENT['compare'],
        }
        if CURRENT['reason']:
            exchange['reason'] = CURRENT['reason']
        if 'json' in content_type:
            try:
                data = response.json()
            except Exception:
                data = None
            for slot, key in CURRENT['capture'].items():
                if isinstance(data, dict) and data.get(key):
                    if slot.startswith('jwt.'):
                        AUTH_SLOTS['Bearer ' + data[key]] = slot[len('jwt.'):]
                    else:
                        SLOTS.set(slot, data[key])
            if CURRENT['capture']:
                exchange['capture'] = dict(CURRENT['capture'])
            exchange['response'] = {'json': normalize(data)}
        elif content_type.startswith('text/'):
            exchange['response'] = {'text': SLOTS.sub_text(response.text)}
        else:
            exchange['response'] = {'base64': base64.b64encode(response.content).decode()}
        disposition = response.headers.get('content-disposition')
        if disposition:
            exchange['content_disposition'] = disposition
        EXCHANGES.append(exchange)
        return response

    requests.Session.send = send


def step(name, fn, compare='exact', reason=None, capture=None):
    CURRENT.update({
        'step': name,
        'compare': compare,
        'reason': reason,
        'capture': capture or {},
    })
    try:
        fn()
    except Exception as e:
        print(f"[{name}] {type(e).__name__}: {e}", file=sys.stderr)
    CURRENT.update({'step': None, 'compare': 'exact', 'reason': None, 'capture': {}})


SQLITE_TOKENS = (
    "The sqlite instance returns token scopes as a JSON string (`instance/_tokens.py:304`), "
    "so `TokenModel(**doc)` fails (a 500) where the in-memory instance returns a list."
)
SQLITE_TYPES = "Column types are the instance's own (sqlite affinities vs. pandas dtypes)."
SQLITE_MAINT = "Maintenance results are the instance's own (sqlite page sizes)."


def plugin_archive() -> bytes:
    """A deterministic `.tar.gz` holding `fixture_plugin/__init__.py`."""
    raw = io.BytesIO()
    with tarfile.open(fileobj=raw, mode='w') as tar:
        content = b"__version__ = '0.1.0'\n"
        info = tarfile.TarInfo('fixture_plugin/__init__.py')
        info.size = len(content)
        info.mtime = 0
        info.mode = 0o644
        tar.addfile(info, io.BytesIO(content))
    out = io.BytesIO()
    with gzip.GzipFile(fileobj=out, mode='wb', mtime=0) as gz:
        gz.write(raw.getvalue())
    return out.getvalue()


def multipart(field: str, filename: str, data: bytes, boundary: str) -> bytes:
    return (
        f'--{boundary}\r\n'
        f'Content-Disposition: form-data; name="{field}"; filename="{filename}"\r\n'
        'Content-Type: application/gzip\r\n\r\n'
    ).encode() + data + f'\r\n--{boundary}--\r\n'.encode()


def main_record(port: int):
    global VERSION
    import requests
    import pandas as pd
    import meerschaum as mrsm
    from meerschaum.core import User
    VERSION = mrsm.__version__
    install_recorder()
    base = f"http://127.0.0.1:{port}"
    def raw(method, path, **kw):
        # A fresh connection per request: a 500 may leave a pooled connection half-closed.
        with requests.Session() as session:
            return session.request(method, base + path, **kw)

    conn = mrsm.get_connector(
        'api:contract', host='127.0.0.1', port=port,
        username=_pyserver.USERNAME, password=_pyserver.PASSWORD,
    )

    # Unauthenticated routes and auth failures.
    step('version', lambda: raw('GET', '/version'))
    step('version_mrsm', lambda: raw('GET', '/version/mrsm'))
    step('healthcheck', lambda: raw('GET', '/healthcheck'))
    step('chaining', lambda: raw('GET', '/chaining'))
    step('info', lambda: raw('GET', '/info'))
    step('not_found', lambda: raw('GET', '/no/such/route'))
    step('keys_no_auth', lambda: raw('GET', '/pipes/keys'))
    step('keys_bad_jwt', lambda: raw('GET', '/pipes/keys', headers={'Authorization': 'Bearer garbage'}))
    step('keys_basic_scheme', lambda: raw('GET', '/pipes/keys', headers={'Authorization': 'Basic abc'}))
    step('users_me_no_auth', lambda: raw('GET', '/users/me'))
    step('login_wrong_password', lambda: raw(
        'POST', '/login', data={'username': _pyserver.USERNAME, 'password': 'wrong'},
    ))
    step('login_unknown_user', lambda: raw(
        'POST', '/login', data={'username': 'nobody', 'password': 'x'},
    ))
    step('login_bad_grant_type', lambda: raw(
        'POST', '/login', data={'grant_type': 'bogus', 'username': 'a', 'password': 'b'},
    ))
    step('login', lambda: conn.login(), capture={'jwt.admin': 'access_token'})
    step('login_scoped', lambda: raw(
        'POST', '/login',
        data={'username': _pyserver.USERNAME, 'password': _pyserver.PASSWORD, 'scope': 'pipes:read'},
    ), capture={'jwt.reader': 'access_token'})

    def reader(method, path, **kw):
        token = next(k for k, v in AUTH_SLOTS.items() if v == 'reader')
        return raw(method, path, headers={'Authorization': token}, **kw)

    def admin(method, path, headers=None, **kw):
        token = next(k for k, v in AUTH_SLOTS.items() if v == 'admin')
        return raw(method, path, headers={'Authorization': token, **(headers or {})}, **kw)

    step('users_me', lambda: conn.get('/users/me'))
    step('connectors', lambda: conn.get('/connectors'))
    step('connectors_sql', lambda: conn.get('/connectors/sql'))
    step('connectors_missing', lambda: conn.get('/connectors', params={'type': 'nope'}))
    step('metadata', lambda: conn.create_metadata())

    # Pipes: registration and attributes.
    pipe = mrsm.Pipe(
        'demo', 'temperature', 'garden', instance=conn,
        columns={'datetime': 'ts', 'id': 'station'}, tags=['outdoor'],
    )
    nloc = mrsm.Pipe('demo', 'humidity', instance=conn, columns={'id': 'room'})
    step('register_pipe', lambda: conn.register_pipe(pipe))
    step('register_pipe_again', lambda: conn.register_pipe(pipe))
    step('register_pipe_null_location', lambda: conn.register_pipe(nloc))
    step('register_pipe_reader_403', lambda: reader(
        'POST', '/pipes/demo/other/[None]/register', json={'columns': {'id': 'x'}},
    ))
    step('register_pipe_bad_body', lambda: admin(
        'POST', '/pipes/demo/other/[None]/register', data='[1, 2]',
        headers={'Content-Type': 'application/json'},
    ))
    step('mrsm_connector_403', lambda: conn.register_pipe(mrsm.Pipe('mrsm', 'secrets', instance=conn)))
    step('mrsm_connector_403_data', lambda: admin('GET', '/pipes/mrsm/users/[None]/data'))
    step('fetch_pipes_keys', lambda: conn.fetch_pipes_keys())
    step('fetch_pipes_keys_filtered', lambda: conn.fetch_pipes_keys(
        connector_keys=['demo'], metric_keys=['temperature'], location_keys=['garden'],
        tags=['outdoor'], params={'connector_keys': 'demo'},
    ))
    step('fetch_pipes_keys_list', lambda: admin('GET', '/pipes/keys'))
    step('get_pipes', lambda: admin('GET', '/pipes'))
    step('get_pipes_by_connector', lambda: admin('GET', '/pipes/demo'))
    step('get_pipes_by_metric', lambda: admin('GET', '/pipes/demo/temperature'))
    step('get_pipe_by_keys', lambda: admin('GET', '/pipes/demo/temperature/garden'))
    step('get_pipe_null_location', lambda: admin('GET', '/pipes/demo/humidity/None'))
    step('get_pipes_missing_connector', lambda: admin('GET', '/pipes/nope'))
    step('get_pipes_missing_metric', lambda: admin('GET', '/pipes/demo/nope'))
    step('get_pipes_missing_location', lambda: admin('GET', '/pipes/demo/temperature/nope'))
    step('get_pipe_id', lambda: conn.get_pipe_id(pipe))
    step('get_pipe_id_missing', lambda: admin('GET', '/pipes/demo/nope/[None]/id'))
    step('get_pipe_attributes', lambda: conn.get_pipe_attributes(pipe))
    step('pipe_exists_before', lambda: conn.pipe_exists(pipe))

    # Pipes: data.
    df = pd.DataFrame({
        'ts': pd.to_datetime(
            ['2024-01-01 00:00:00', '2024-01-01 00:00:00', '2024-01-02 12:30:00'],
            utc=True,
        ),
        'station': ['a', 'b/c', 'é'],
        'val': [10, 20, 30],
    })
    step('sync_pipe_chunked', lambda: conn.sync_pipe(pipe, df, chunksize=2), reason=(
        "The client's final `DELETE .../cache` is compared; the client-side timing message is not."
    ))
    df2 = pd.DataFrame({
        'ts': pd.to_datetime(['2024-01-02 12:30:00', '2024-01-03 00:00:00'], utc=True),
        'station': ['é', 'a'],
        'val': [31, 40],
    })
    step('sync_pipe_update', lambda: conn.sync_pipe(pipe, df2))
    step('sync_empty', lambda: admin('POST', '/pipes/demo/temperature/garden/data', data='[]'))
    evil = mrsm.Pipe('demo', 'evil', instance=conn, target='mrsm_users')
    step('register_protected_target', lambda: conn.register_pipe(evil))
    step('sync_protected', lambda: admin(
        'POST', '/pipes/demo/evil/[None]/data', data='{"a": [1]}',
    ))
    step('get_data_protected', lambda: admin('GET', '/pipes/demo/evil/[None]/data'))
    step('vacuum_protected', lambda: admin('POST', '/pipes/demo/evil/[None]/vacuum'))
    step('delete_protected_target', lambda: conn.delete_pipe(evil))
    step('pipe_exists_after', lambda: conn.pipe_exists(pipe))
    step('get_sync_time_newest', lambda: conn.get_sync_time(pipe))
    step('get_sync_time_oldest', lambda: conn.get_sync_time(pipe, newest=False))
    step('get_sync_time_params', lambda: conn.get_sync_time(pipe, params={'station': 'b/c'}))
    step('get_sync_time_bad_bool', lambda: admin(
        'GET', '/pipes/demo/temperature/garden/sync_time', params={'newest': 'maybe'},
    ))
    step('get_sync_time_no_dt', lambda: admin('GET', '/pipes/demo/humidity/[None]/sync_time'))
    step('get_pipe_rowcount', lambda: conn.get_pipe_rowcount(pipe))
    step('get_pipe_rowcount_filtered', lambda: admin(
        'GET', '/pipes/demo/temperature/garden/rowcount',
        params={'begin': '2024-01-02'}, json={'station': ['a', 'é']},
    ))
    step('get_pipe_data', lambda: admin('GET', '/pipes/demo/temperature/garden/data'))
    step('get_pipe_data_filtered', lambda: admin(
        'GET', '/pipes/demo/temperature/garden/data',
        params={
            'select_columns': json.dumps(['ts', 'station', 'val']),
            'begin': '2024-01-01', 'end': '2024-01-03',
            'params': json.dumps({'station': '_b/c'}),
            'order': 'desc',
        },
    ))
    step('get_pipe_data_limit', lambda: admin(
        'GET', '/pipes/demo/temperature/garden/data', params={'limit': 2, 'order': 'desc'},
    ))
    step('get_pipe_data_too_many', lambda: admin(
        'GET', '/pipes/demo/temperature/garden/data', params={'limit': 100_001},
    ))
    step('get_pipe_data_bad_params', lambda: admin(
        'GET', '/pipes/demo/temperature/garden/data', params={'params': '[1]'},
    ))
    step('get_pipe_data_bad_select', lambda: admin(
        'GET', '/pipes/demo/temperature/garden/data', params={'select_columns': '{}'},
    ))
    step('get_pipe_data_unregistered', lambda: admin('GET', '/pipes/demo/nope/[None]/data'))
    step('get_pipe_docs', lambda: admin(
        'GET', '/pipes/demo/temperature/garden/docs',
        params={'select_columns': json.dumps(['station', 'val']), 'params': json.dumps({'station': 'a'})},
    ))
    step('get_chunk_bounds', lambda: admin(
        'GET', '/pipes/demo/temperature/garden/chunk_bounds',
        params={'begin': '2024-01-01', 'end': '2024-01-03', 'chunk_interval_minutes': 1440},
    ))
    step('get_chunk_bounds_unbounded', lambda: admin(
        'GET', '/pipes/demo/temperature/garden/chunk_bounds',
        params={'chunk_interval_minutes': 2880, 'bounded': 'false'},
    ))
    step('get_csv', lambda: admin('GET', '/pipes/demo/temperature/garden/csv'))
    step('get_columns_types', lambda: conn.get_pipe_columns_types(pipe), compare='shape', reason=SQLITE_TYPES)
    step('get_columns_indices', lambda: conn.get_pipe_columns_indices(pipe), compare='shape',
         reason="Index metadata is the instance's own.")
    step('get_index_names', lambda: admin('GET', '/pipes/demo/temperature/garden/indices/names'),
         compare='status', reason="Index names are the instance's own (SQL naming).")
    step('get_size', lambda: conn.get_pipe_size(pipe), compare='status', reason=SQLITE_MAINT)
    step('vacuum', lambda: conn.vacuum_pipe(pipe), compare='status', reason=SQLITE_MAINT)
    step('analyze', lambda: conn.analyze_pipe(pipe), compare='status', reason=SQLITE_MAINT)
    step('compress', lambda: conn.compress_pipe(pipe), compare='status', reason=SQLITE_MAINT)
    step('decompress', lambda: conn.decompress_pipe(pipe, no_policy=True), compare='status',
         reason=SQLITE_MAINT)
    step('partition', lambda: conn.partition_pipe(pipe, chunk_minutes=1440), compare='status',
         reason=SQLITE_MAINT)
    step('maintenance_protected', lambda: admin('POST', '/pipes/mrsm/users/[None]/vacuum'))

    pipe.parameters['tags'] = ['outdoor', 'edited']
    step('edit_pipe', lambda: conn.edit_pipe(pipe))
    step('edit_pipe_patch', lambda: conn.edit_pipe(pipe, patch=True))
    step('edit_pipe_missing_body', lambda: admin('PATCH', '/pipes/demo/temperature/garden/edit'))
    step('edit_pipe_unregistered', lambda: admin(
        'PATCH', '/pipes/demo/nope/[None]/edit', json={'columns': {}},
    ))
    step('get_pipe_attributes_after_edit', lambda: conn.get_pipe_attributes(pipe))
    step('delete_pipe_cache', lambda: conn.delete_pipe_cache(pipe))
    step('clear_pipe', lambda: admin(
        'DELETE', '/pipes/demo/temperature/garden/clear',
        params={'begin': '2024-01-02', 'params': json.dumps({'station': 'a'})},
    ))
    step('rowcount_after_clear', lambda: conn.get_pipe_rowcount(pipe))
    step('clear_bad_params', lambda: admin(
        'DELETE', '/pipes/demo/temperature/garden/clear', params={'params': 'nope'},
    ))
    step('drop_pipe', lambda: conn.drop_pipe(pipe))
    step('exists_after_drop', lambda: conn.pipe_exists(pipe))
    step('delete_pipe', lambda: conn.delete_pipe(pipe))
    step('delete_pipe_again', lambda: admin('DELETE', '/pipes/demo/temperature/garden/delete'))
    step('delete_pipe_null_location', lambda: conn.delete_pipe(nloc))

    # Users.
    new_user = User('fixture_user', 'fixture_user_pass', email='u@example.com', attributes={'a': 1})
    step('register_user', lambda: conn.register_user(new_user))
    step('register_user_again', lambda: conn.register_user(new_user))
    step('register_user_admin', lambda: raw(
        'POST', '/users/register', data={'username': 'x_admin', 'password': 'p', 'type': 'admin'},
    ))
    step('register_user_missing', lambda: raw('POST', '/users/register', data={'username': 'x'}))
    step('register_user_bad_attributes', lambda: raw(
        'POST', '/users/register', data={'username': 'y', 'password': 'p', 'attributes': '{bad'},
    ))
    step('get_users', lambda: conn.get_users())
    step('get_user_id', lambda: conn.get_user_id(new_user))
    step('get_user_attributes', lambda: conn.get_user_attributes(new_user))
    step('get_user_attributes_missing', lambda: raw('GET', '/users/nobody/attributes'))
    step('get_user_type_chaining', lambda: conn.get_user_type(new_user))
    step('get_user_password_hash_chaining', lambda: conn.get_user_password_hash(new_user))
    new_user.email = 'new@example.com'
    new_user.type = 'basic'
    step('edit_user', lambda: conn.edit_user(new_user))
    step('user_login', lambda: raw(
        'POST', '/login', data={'username': 'fixture_user', 'password': 'fixture_user_pass'},
    ), capture={'jwt.user': 'access_token'})

    def as_user(method, path, **kw):
        token = next(k for k, v in AUTH_SLOTS.items() if v == 'user')
        return raw(method, path, headers={'Authorization': token}, **kw)

    step('user_me', lambda: as_user('GET', '/users/me'))
    step('user_create_job_denied', lambda: as_user('POST', '/jobs/x', json=['show', 'version']))
    step('user_delete_other', lambda: as_user('DELETE', f'/users/{_pyserver.USERNAME}'))
    step('delete_user', lambda: conn.delete_user(new_user))
    step('delete_user_missing', lambda: admin('DELETE', '/users/nobody'))

    # Tokens.
    step('register_token', lambda: admin(
        'POST', '/tokens/register',
        json={'label': 'fixture-token', 'scopes': ['pipes:read'], 'expiration': '2100-01-01T00:00:00+00:00'},
    ), capture={'token_id': 'id', 'token_secret': 'secret', 'api_key': 'api_key'})
    step('register_token_no_body', lambda: admin('POST', '/tokens/register'))
    step('get_tokens', lambda: admin('GET', '/tokens'))
    step('get_tokens_labels', lambda: admin('GET', '/tokens', params={'labels': 'fixture-token'}))
    step('get_tokens_no_auth', lambda: raw('GET', '/tokens'))
    step('get_token_invalid_id', lambda: admin('GET', '/tokens/not-a-uuid'))
    step('get_token_missing', lambda: admin('GET', '/tokens/00000000-0000-4000-8000-000000000000'))
    step('get_token', lambda: admin('GET', '/tokens/' + SLOTS.values.get('token_id', 'x')),
         compare='skip', reason=SQLITE_TOKENS)
    step('edit_token_null_creation', lambda: admin(
        'POST', '/tokens/' + SLOTS.values.get('token_id', 'x') + '/edit',
        json={'creation': None, 'expiration': None, 'label': 'x'},
    ))
    step('edit_token_missing_fields', lambda: admin(
        'POST', '/tokens/' + SLOTS.values.get('token_id', 'x') + '/edit', json={'label': 'x'},
    ))
    step('invalidate_token_bad_id', lambda: admin('POST', '/tokens/nope/invalidate'))
    step('delete_token_bad_id', lambda: admin('DELETE', '/tokens/nope'))
    step('validate_bad_key', lambda: raw(
        'POST', '/tokens/validate', headers={'Authorization': 'Bearer mrsm-key:aW52YWxpZA=='},
    ))
    step('validate_no_key', lambda: raw('POST', '/tokens/validate'))
    step('validate_key', lambda: raw(
        'POST', '/tokens/validate', headers={'Authorization': 'Bearer ' + SLOTS.values.get('api_key', '')},
    ), compare='skip', reason=SQLITE_TOKENS)
    step('login_client_credentials', lambda: raw(
        'POST', '/login',
        data={
            'client_id': SLOTS.values.get('token_id', ''),
            'client_secret': SLOTS.values.get('token_secret', ''),
        },
    ), capture={'jwt.client': 'access_token'})
    step('login_client_credentials_bad_secret', lambda: raw(
        'POST', '/login',
        data={'client_id': SLOTS.values.get('token_id', ''), 'client_secret': 'wrong'},
    ))
    step('login_client_credentials_not_uuid', lambda: raw(
        'POST', '/login', data={'client_id': 'nope', 'client_secret': 'x'},
    ))
    step('client_credentials_request', lambda: raw(
        'GET', '/pipes/keys',
        headers={'Authorization': next(k for k, v in AUTH_SLOTS.items() if v == 'client')},
    ), compare='skip', reason=SQLITE_TOKENS)
    step('delete_token', lambda: admin('DELETE', '/tokens/' + SLOTS.values.get('token_id', 'x')),
         compare='skip', reason=SQLITE_TOKENS)

    # Plugins.
    archive = plugin_archive()
    boundary = 'mrsmcontractboundary'
    step('register_plugin', lambda: admin(
        'POST', '/plugins/fixture_plugin',
        params={'version': '0.1.0', 'attributes': '{}'},
        data=multipart('archive', 'fixture_plugin.tar.gz', archive, boundary),
        headers={'Content-Type': f'multipart/form-data; boundary={boundary}'},
    ))
    step('register_plugin_no_archive', lambda: admin('POST', '/plugins/other_plugin'))
    step('get_plugins', lambda: conn.get_plugins())
    step('get_plugins_search', lambda: conn.get_plugins(search_term='fix'))
    step('get_plugins_search_none', lambda: conn.get_plugins(search_term='zzz'))
    step('get_plugin_attributes', lambda: raw('GET', '/plugins/fixture_plugin/attributes'))
    step('get_plugin_version', lambda: raw('GET', '/plugins/fixture_plugin/version'))
    step('download_plugin', lambda: raw('GET', '/plugins/fixture_plugin'))
    step('download_plugin_missing', lambda: raw('GET', '/plugins/nope'))
    step('delete_plugin_missing', lambda: admin('DELETE', '/plugins/nope'))
    step('delete_plugin', lambda: admin('DELETE', '/plugins/fixture_plugin'))
    step('delete_plugin_no_auth', lambda: raw('DELETE', '/plugins/fixture_plugin'))

    # Jobs (the Rust test uses a backend with no jobs, like a fresh v4 root).
    step('get_jobs', lambda: admin('GET', '/jobs'))
    step('get_job_missing', lambda: admin('GET', '/jobs/missing-job'))
    step('job_exists_missing', lambda: admin('GET', '/jobs/missing-job/exists'))
    step('start_job_missing', lambda: admin('POST', '/jobs/missing-job/start'))
    step('logs_missing', lambda: admin('GET', '/logs/missing-job'))
    step('jobs_reader_403', lambda: reader('GET', '/jobs'))
    step('create_job_no_body', lambda: admin('POST', '/jobs/x'))

    # Allow-listed odds and ends.
    step('index', lambda: raw('GET', '/', allow_redirects=False))
    step('openapi_title', lambda: None)

    _pyserver.write_fixture(OUT, {
        'username': _pyserver.USERNAME,
        'password': _pyserver.PASSWORD,
        'instance': _pyserver.INSTANCE,
        'connectors': {
            typ: [k for k in labels if k != 'default']
            for typ, labels in mrsm.get_config('meerschaum', 'connectors').items()
        },
        'jwt_vectors': jwt_vectors(),
        'exchanges': EXCHANGES,
    })
    print(f"Wrote {len(EXCHANGES)} exchanges to {OUT}.")


def jwt_vectors() -> list:
    """
    JWTs minted by v4's libraries (PyJWT through `fastapi_login`, and python-jose) with a fixed
    secret and expiry, so the Rust encoder can be checked byte-for-byte and the decoder against
    tokens issued by 4.1.
    """
    import meerschaum as mrsm
    from datetime import datetime, timezone
    pyjwt = mrsm.attempt_import('jwt', lazy=False)
    jose_jwt = mrsm.attempt_import('jose.jwt', lazy=False)
    secret = b'fixture-secret-0123456789abcdef'
    vectors = []
    for claims in (
        {'sub': 'fixture_admin', 'scopes': ['*'], 'type': 'admin'},
        {'sub': '00000000-0000-4000-8000-000000000001', 'scopes': ['pipes:read'], 'type': None},
        {'sub': 'utilisateur_é', 'scopes': [], 'type': 'basic'},
    ):
        payload = dict(claims)
        payload['exp'] = datetime(2100, 1, 1, tzinfo=timezone.utc)
        vectors.append({
            'secret': secret.decode(),
            'claims': {**claims, 'exp': 4102444800},
            'pyjwt': pyjwt.encode(payload, secret, algorithm='HS256'),
            'jose': jose_jwt.encode({**claims, 'exp': 4102444800}, secret, algorithm='HS256'),
        })
    return vectors


def main():
    _pyserver.seed_admin()
    port = _pyserver.start_server()
    main_record(port)


if __name__ == '__main__':
    try:
        main()
    finally:
        _pyserver.kill_server()
