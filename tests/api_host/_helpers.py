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

import pytest

REPO = pathlib.Path(__file__).resolve().parents[2]


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
#: `meerschaum.api.dash.access` viewer of a session) see the viewer named for that session's ID:
#: `fake_viewers({'admin': {'admin': True}, 'bob': {'role': 'viewer'}}, default={'admin': True})`
#: (each value: `access.make_viewer()` keywords, plus an optional `username`, else the session ID).
#: Sessions named nowhere (and no default) are anonymous.
FAKE_VIEWERS = '''
def fake_viewers(viewers=None, default=None):
    from meerschaum.api.dash import access as _access
    def _get_viewer(session_data=None, session_id=None):
        if session_id is None and isinstance(session_data, dict):
            session_id = session_data.get('session-id')
        spec = (viewers or {}).get(session_id, default)
        if spec is None:
            return _access.ANONYMOUS
        spec = dict(spec)
        username = spec.pop('username', session_id)
        return _access.make_viewer(username, **spec)
    _access.get_viewer = _get_viewer
    return _get_viewer
'''


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
#: one seam, `meerschaum.api.dash.access.get_viewer`) without a user or a session, for tests of
#: something other than who may do what; `as_viewer(None)` is an anonymous visitor. `sign_in`
#: signs in a real user instead.
AS_VIEWER = '''
def as_viewer(username='tester', admin=True, role=None, permissions=None, teams=None):
    from meerschaum.api.dash import access as _access
    if username is None:
        _viewer = _access.ANONYMOUS
    else:
        _viewer = _access.make_viewer(
            username, admin=admin, role=role, permissions=permissions, teams=teams,
        )
    _access.get_viewer = lambda *args, **kwargs: _viewer
    return _viewer
'''


def write_plugin(plugins: pathlib.Path, name: str, source: str) -> None:
    path = plugins / name
    path.mkdir(parents=True, exist_ok=True)
    (path / '__init__.py').write_text(textwrap.dedent(source), encoding='utf-8')


@pytest.fixture(scope='module')
def sandbox():
    """A temporary root and plugins directory, skipped when the host dependencies are missing."""
    scratch = pathlib.Path.home() / '.cache' / 'mrsm-api-host-tests'
    scratch.mkdir(parents=True, exist_ok=True)
    root = pathlib.Path(tempfile.mkdtemp(prefix='mrsm-apihost-', dir=scratch))
    plugins = pathlib.Path(tempfile.mkdtemp(prefix='mrsm-apihost-plugins-', dir=scratch))
    import shutil
    try:
        check = run_python(
            root, plugins,
            "import meerschaum.utils.venv as v; v.activate_venv('mrsm'); "
            "import fastapi, uvicorn, dash, dash_bootstrap_components, dash_ace, a2wsgi, jwt",
        )
        if check.returncode != 0:
            pytest.skip(f"The API host's dependencies are missing:\n{check.stderr[-1500:]}")
        yield root, plugins
    finally:
        ### Also when the check skips or fails: the directories are never left behind.
        shutil.rmtree(root, ignore_errors=True)
        shutil.rmtree(plugins, ignore_errors=True)
