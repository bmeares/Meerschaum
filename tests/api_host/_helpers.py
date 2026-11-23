#! /usr/bin/env python3
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

"""
Helpers for the API host tests: run Python in host mode with a temporary root, a temporary
plugins directory and a temporary SQLite instance (never the real config).
"""

import os
import sys
import json
import pathlib
import tempfile
import textwrap
import subprocess

from typing import Optional

import pytest

REPO = pathlib.Path(__file__).resolve().parents[2]


def sandbox_main_db(root: pathlib.Path) -> pathlib.Path:
    """The SQLite file `sql:main` is inside a sandbox."""
    return pathlib.Path(root) / 'sql_main.db'


def sandbox_sql_main(root: pathlib.Path) -> str:
    """`MRSM_SQL_MAIN` for a sandbox: a SQLite file under its root, never a network database."""
    return json.dumps({'flavor': 'sqlite', 'database': str(sandbox_main_db(root))})


def make_env(root: pathlib.Path, plugins: pathlib.Path, host_mode: bool = True) -> dict:
    env = {
        key: val
        for key, val in os.environ.items()
        if not key.startswith('MRSM_') and key not in ('PYTHONPATH', 'TMUX', 'TMUX_PANE')
    }
    env.update({
        'PYTHONPATH': str(REPO),
        'MRSM_ROOT_DIR': str(root),
        'MRSM_PLUGINS_DIR': str(plugins),
        'MRSM_NO_AUTO_INSTALL': '1',
        'PYTHONDONTWRITEBYTECODE': '1',
        'MRSM_SQL_APIHOSTTEST': f"sqlite:///{root / 'instance.db'}",
        ### Whatever resolves `sql:main` (its default is a database on localhost:5432) gets a
        ### throwaway SQLite file inside the sandbox root: a test never reaches a network database
        ### it did not create, and code that needs a working `sql:main` still works.
        'MRSM_SQL_MAIN': sandbox_sql_main(root),
        'MRSM_API_HOST_CONFIG': json.dumps({'mrsm_instance': 'sql:apihosttest'}),
    })
    ### Starlette 1.9 warns when `starlette.testclient` falls back to `httpx` (it wants `httpx2`,
    ### which CI and `scripts/test.sh` install): the tests behave the same either way, so a
    ### developer venv without `httpx2` prints no deprecation noise into the stderr they check.
    env['PYTHONWARNINGS'] = ','.join(filter(None, [
        os.environ.get('PYTHONWARNINGS', ''),
        STARLETTE_HTTPX_WARNING_FILTER,
    ]))
    if host_mode:
        env['MRSM_API_HOST'] = '1'
    return env


### `-W action:message`: the message is matched literally from its start (no category, so the
### filter does not import Starlette at interpreter startup).
STARLETTE_HTTPX_WARNING_FILTER = 'ignore:Using `httpx` with `starlette.testclient` is deprecated'


#: Python for a test's code: `fake_viewers(...)` makes every console access check (the
#: `meerschaum.api.dash.viewer.resolve_viewer` of a session, which `access` and `sessions` ask)
#: see the viewer named for that session's ID:
#: `fake_viewers({'admin': {'admin': True}, 'bob': {'role': 'viewer'}}, default={'admin': True})`
#: (each value: `access.make_viewer()` keywords, plus an optional `username`, else the session ID).
#: Sessions named nowhere (and no default) are anonymous.
FAKE_VIEWERS = '''
def fake_viewers(viewers=None, default=None):
    from meerschaum.api.dash import access as _access, viewer as _viewer_model
    def _resolve_viewer(session_data=None, session_id=None, **kwargs):
        if session_id is None and isinstance(session_data, dict):
            session_id = session_data.get('session-id')
        spec = (viewers or {}).get(session_id, default)
        if spec is None:
            return _access.ANONYMOUS
        spec = dict(spec)
        username = spec.pop('username', session_id)
        return _access.make_viewer(username, **spec)
    _viewer_model.resolve_viewer = _access.resolve_viewer = _resolve_viewer
    return _resolve_viewer
'''


def extract_json(result, prefix: str = 'JSON:') -> dict:
    """Assert subprocess `result` exited 0 and parse the last `JSON:` line from stdout."""
    assert result.returncode == 0, result.stdout[-3000:] + result.stderr[-6000:]
    lines = [line for line in result.stdout.splitlines() if line.startswith(prefix)]
    assert lines, f"No {prefix!r} line in stdout:\n{result.stdout[-2000:]}\nSTDERR:\n{result.stderr[-2000:]}"
    return json.loads(lines[-1][len(prefix):])


def check(result) -> str:
    """Assert subprocess `result` exited 0 and return its stdout."""
    assert result.returncode == 0, result.stdout[-3000:] + result.stderr[-6000:]
    return result.stdout


def walk(node):
    """Yield every component node in a Dash component tree."""
    if isinstance(node, (list, tuple)):
        for child in node:
            yield from walk(child)
        return
    if node is None or isinstance(node, (str, int, float, bool)):
        return
    yield node
    for attr in ('children', 'label'):
        child = getattr(node, attr, None)
        if child is not None:
            yield from walk(child)


def ids(component) -> set:
    """Return all string `id`s in a Dash component tree."""
    return {
        node_id
        for node in walk(component)
        if isinstance((node_id := getattr(node, 'id', None)), str)
    }


def run_python(root, plugins, code: str, host_mode: bool = True, extra_env: dict = None):
    env = make_env(root, plugins, host_mode)
    env.update(extra_env or {})
    return subprocess.run(
        [sys.executable, '-c', textwrap.dedent(code)],
        env=env,
        cwd=str(root),
        capture_output=True,
        text=True,
        timeout=180,
    )


#: Python to put before a test's code: `sign_in(username, user_type=None, role=None,
#: permissions=None, teams=None, session_id=None)` registers `username` on the API's instance when
#: it is missing (password `correct-horse-9`), sets its role or explicit permissions (`None`: the
#: default role) and its teams (`{team: 'member' | 'manager'}`, created when missing), and returns
#: the `session-store` data of a new console session for it. The console then sees exactly that
#: user (`meerschaum.api.dash.access`): its type, effective permissions and teams.
SIGN_IN = '''
def sign_in(username, user_type=None, role=None, permissions=None, teams=None, session_id=None):
    import uuid as _uuid
    import meerschaum as _mrsm
    from meerschaum.core import User as _User
    from meerschaum.api import get_api_connector as _api_connector
    from meerschaum.api.dash import sessions as _sessions, access as _access
    _conn = _api_connector()
    _user = _User(username, 'correct-horse-9', type=user_type, instance=_conn)
    if _conn.get_user_id(_user) is None:
        _ok, _msg = _conn.register_user(_user)
        assert _ok, _msg
    if user_type != 'admin':
        _attributes = dict(_conn.get_user_attributes(_User(username, instance=_conn)) or {})
        _attributes.pop('scopes', None)
        _attributes['role'] = role
        if permissions is not None:
            _attributes = _access.attributes_with_rights(_attributes, 'custom', permissions)
        _ok, _msg = _conn.set_user_attributes(_User(username, instance=_conn), _attributes)
        assert _ok, _msg
    for _name, _team_role in (teams or {}).items():
        _team = _mrsm.Team(_name, instance=_conn)
        if not _team.exists():
            _ok, _msg = _team.register()
            assert _ok, _msg
        _ok, _msg = _team.add_members(username, role=_team_role)
        assert _ok, _msg
    session_id = session_id or str(_uuid.uuid4())
    _sessions.set_session(session_id, {'username': username})
    _access.forget()
    return {'session-id': session_id}
'''


#: Python to put before a test's code: `as_viewer(username='tester', admin=True, role=None,
#: permissions=None, teams=None)` makes every console check see that viewer (the access model's
#: one seam, `meerschaum.api.dash.viewer.resolve_viewer`) without a user or a session, for tests of
#: something other than who may do what; `as_viewer(None)` is an anonymous visitor. `sign_in`
#: signs in a real user instead.
AS_VIEWER = '''
def as_viewer(username='tester', admin=True, role=None, permissions=None, teams=None):
    from meerschaum.api.dash import access as _access, viewer as _viewer_model
    if username is None:
        _viewer = _access.ANONYMOUS
    else:
        _viewer = _access.make_viewer(
            username, admin=admin, role=role, permissions=permissions, teams=teams,
        )
    _viewer_model.resolve_viewer = _access.resolve_viewer = lambda *args, **kwargs: _viewer
    return _viewer
'''


def write_plugin(plugins: pathlib.Path, name: str, source: str) -> None:
    path = plugins / name
    path.mkdir(parents=True, exist_ok=True)
    (path / '__init__.py').write_text(textwrap.dedent(source), encoding='utf-8')


_DEPS_CHECKED: bool = False
_DEPS_SKIP_REASON: Optional[str] = None


def ensure_api_host_deps() -> bool:
    """Verify the API host + Dash console dependencies once per pytest worker process."""
    global _DEPS_CHECKED, _DEPS_SKIP_REASON
    if not _DEPS_CHECKED:
        import shutil
        scratch = pathlib.Path.home() / '.cache' / 'mrsm-api-host-tests'
        scratch.mkdir(parents=True, exist_ok=True)
        root = pathlib.Path(tempfile.mkdtemp(prefix='mrsm-apihost-probe-', dir=scratch))
        plugins = pathlib.Path(tempfile.mkdtemp(prefix='mrsm-apihost-probe-plugins-', dir=scratch))
        try:
            probe = run_python(
                root,
                plugins,
                "import meerschaum.utils.venv as v; v.activate_venv('mrsm'); "
                "import fastapi, uvicorn, dash, dash_bootstrap_components, dash_ace, a2wsgi, jwt",
            )
            if probe.returncode != 0:
                _DEPS_SKIP_REASON = (
                    f"The API host's dependencies are missing:\n{probe.stderr[-1500:]}"
                )
        finally:
            shutil.rmtree(root, ignore_errors=True)
            shutil.rmtree(plugins, ignore_errors=True)
        _DEPS_CHECKED = True
    if _DEPS_SKIP_REASON is not None:
        pytest.skip(_DEPS_SKIP_REASON)
    return True


def check_reserved_instance_visibility(sandbox, seed):
    """Exercise the console table, detail, scope filter, and Access dialog on a reserved instance."""
    root, plugins = sandbox
    reserved_config = json.dumps({
        'flavor': 'sqlite',
        'database': str(root / 'reserved-pipes.db'),
        'teams': 'tidelands',
    })
    out = extract_json(run_python(root, plugins, seed + '''
import flask
from dash.exceptions import PreventUpdate
from meerschaum import api
from meerschaum.api.dash import pipes_table
from meerschaum.api.dash.callbacks.pipes import open_pipe_access

team('tidelands', isolated=True, instance='sql:tidelands')
member = sign_in('reserved-member', teams={'tidelands': 'member'})
manager = sign_in('reserved-manager', teams={'tidelands': 'manager'})
admin = sign_in('reserved-admin', user_type='admin')
reserved = mrsm.get_connector('sql:tidelands')
api.permissions_config.setdefault('instances', {}).update(
    allow_multiple_instances=True,
    allowed_instance_keys=[INSTANCE, str(reserved)],
)
legacy = mrsm.Pipe('ti:src', 'reserved_legacy', instance=reserved)
assert legacy.register()[0]

app = flask.Flask('reserved-pipes')

def snapshot(session, acting_team=None):
    cookie = f"mrsm-session-id={session['session-id']}"
    if acting_team:
        cookie += '; mrsm-team=' + acting_team
    with app.test_request_context('/dash/pipes', headers={'Cookie': cookie}):
        viewer = access.resolve_viewer(session)
        rows = pipes_table.list_rows(reserved, viewer=viewer)
        filtered = pipes_table.filter_scope(rows, 'teams', pipes_table.viewer_summary(viewer))
        detail = access.visible_pipe('ti:src', 'reserved_legacy', instance_keys=str(reserved),
                                     viewer=viewer)
        trigger('pipe-manage-access.n_clicks')
        owner_options, everyone_label = None, None
        try:
            modal = open_pipe_access(1, None, {
                'connector_keys': 'ti:src', 'metric_key': 'reserved_legacy',
                'instance_keys': str(reserved),
            }, session)
        except PreventUpdate:
            pass
        else:
            owner_options = [option['value'] for option in modal[1]]
            everyone_label = next(
                option['label'] for option in modal[3] if option['value'] == 'Everyone'
            )
        return {
            'isolated': viewer.is_isolated,
            'acting_team': viewer.acting_team,
            'rows': [[row['mk'], row['team']] for row in rows],
            'my_teams': [row['mk'] for row in filtered],
            'detail': detail is not None,
            'everyone_label': everyone_label,
            'owner_options': owner_options,
        }

print('JSON:' + json.dumps({
    'member': snapshot(member),
    'manager': snapshot(manager),
    'acting_admin': snapshot(admin, 'tidelands'),
    'stored_access': legacy.get_parameters(apply_symlinks=False, refresh=True).get('access'),
}))
''', extra_env={'MRSM_SQL_TIDELANDS': reserved_config}))
    assert out['acting_admin']['acting_team'] == 'tidelands'
    assert out['acting_admin']['everyone_label'] == 'Everyone (including tidelands)'
    assert out['member']['owner_options'] is None
    assert 'reserved-member' in out['manager']['owner_options']
    for viewer in (out['member'], out['manager'], out['acting_admin']):
        assert viewer['isolated']
        assert viewer['rows'] == [['reserved_legacy', None]]
        assert viewer['my_teams'] == ['reserved_legacy']
        assert viewer['detail']
    assert out['stored_access'] is None
