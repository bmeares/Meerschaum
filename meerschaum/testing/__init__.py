#! /usr/bin/env python3
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

"""
A pytest plugin for testing Meerschaum plugins and projects.

Enable it with `pytest -p meerschaum.testing`, or in your `conftest.py`:

```python
pytest_plugins = ['meerschaum.testing']
```

Fixtures:

- `mrsm_root` (session): a temporary Meerschaum root and plugins directory for the whole session.
  Every inherited `MRSM_*` variable is removed (and `TMUX`, `TMUX_PANE`), and restored afterwards,
  so a test never touches your real configuration, instances or jobs.
  `mrsm_root.write_plugin(name, source)` adds a plugin.
- `mrsm_instance`: a temporary SQLite instance (`sql:test`), the default instance during the test.
- `mrsm_pipe(connector_keys, metric_key, location_key=None, **kwargs)`: register a pipe on
  `mrsm_instance` (deleted after the test).
- `mrsm_api` (session): the API host app (your `@api_plugin` routes, MCP tools and the web console
  with your pages) in this process, with a `TestClient` (`mrsm_api.client`), test users
  (`mrsm_api.user('admin')`), console sessions and tokens (`mrsm_api.client_for('admin')`).
- `render_page(path, as_user=None)`: render a console or plugin page as a browser would (a Dash
  layout, or a page-contract fragment), then fire its callbacks (`page.callback(...)`) with the
  user's session.

The plugins under test come from `--mrsm-plugins-dir PATH` (repeatable) or the ini option
`mrsm_plugins_dirs` (paths relative to the ini file), next to the session's own plugins directory.

```python
def test_my_plugin(mrsm_pipe):
    pipe = mrsm_pipe('plugin:noaa', 'weather', columns={'datetime': 'timestamp'})
    success, message = pipe.sync()
    assert success, message
```

Nothing heavy is imported until a fixture is used (the API host's packages only for `mrsm_api`
and `render_page`).
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any, Callable, Iterator, List, Optional

import pytest

if TYPE_CHECKING:
    from meerschaum.testing._api import MrsmApi
    from meerschaum.testing._root import MrsmRoot

__all__ = ('mrsm_root', 'mrsm_instance', 'mrsm_pipe', 'mrsm_api', 'render_page')

#: The label of `mrsm_instance` (`sql:test`).
INSTANCE_LABEL: str = 'test'


def pytest_addoption(parser) -> None:
    group = parser.getgroup('meerschaum', 'Meerschaum (meerschaum.testing)')
    group.addoption(
        '--mrsm-plugins-dir',
        action='append',
        default=[],
        dest='mrsm_plugins_dirs',
        metavar='PATH',
        help="A plugins directory to test (repeatable), next to the session's own.",
    )
    group.addoption(
        '--mrsm-venvs-dir',
        default=None,
        dest='mrsm_venvs_dir',
        metavar='PATH',
        help="Keep the plugins' virtual environments here between sessions (default: in the root).",
    )
    group.addoption(
        '--mrsm-env',
        action='append',
        default=[],
        dest='mrsm_env',
        metavar='NAME=VALUE',
        help="A variable to set in the session's environment after it is cleaned (repeatable), "
        "e.g. MRSM_NO_AUTO_INSTALL=1.",
    )
    try:
        parser.addini('mrsm_plugins_dirs', "Plugins directories to test.", type='paths', default=[])
    except ValueError:
        parser.addini('mrsm_plugins_dirs', "Plugins directories to test.", type='pathlist', default=[])
    parser.addini('mrsm_venvs_dir', "Where the plugins' virtual environments are kept.", default='')
    parser.addini(
        'mrsm_env',
        "NAME=VALUE lines to set in the session's environment after it is cleaned.",
        type='linelist',
        default=[],
    )


def _plugins_dirs(config) -> List[Any]:
    import pathlib
    dirs = list(config.getini('mrsm_plugins_dirs') or [])
    dirs += [pathlib.Path(str(path)) for path in (config.getoption('mrsm_plugins_dirs') or [])]
    return [pathlib.Path(str(path)).resolve() for path in dirs]


def _extra_env(config) -> dict:
    out = {}
    for line in list(config.getini('mrsm_env') or []) + list(config.getoption('mrsm_env') or []):
        name, sep, value = str(line).strip().partition('=')
        if not sep or not name.strip():
            raise pytest.UsageError(f"--mrsm-env / mrsm_env expects NAME=VALUE, not '{line}'.")
        out[name.strip()] = value
    return out


@pytest.fixture(scope='session')
def mrsm_root(tmp_path_factory, pytestconfig) -> Iterator['MrsmRoot']:
    """
    A temporary Meerschaum root and plugins directory for the test session (see the module).
    """
    from meerschaum.testing._root import MrsmRoot
    venvs_dir = pytestconfig.getoption('mrsm_venvs_dir') or pytestconfig.getini('mrsm_venvs_dir') or None
    root = MrsmRoot(
        tmp_path_factory.mktemp('mrsm_root'),
        plugins_dirs=_plugins_dirs(pytestconfig),
        venvs_dir=venvs_dir,
        extra_env=_extra_env(pytestconfig),
    )
    root.activate()
    try:
        yield root
    finally:
        root.deactivate()


@pytest.fixture
def mrsm_instance(mrsm_root, tmp_path) -> Iterator[Any]:
    """
    A temporary SQLite instance connector (`sql:test`, a new database for each test), the
    default instance while the test runs (also for jobs and `mrsm` commands it starts).
    """
    conn = mrsm_root.sqlite_instance(INSTANCE_LABEL, tmp_path / 'mrsm_test.db')
    previous = mrsm_root.set_default_instance(f'sql:{INSTANCE_LABEL}')
    try:
        yield conn
    finally:
        if previous is not None:
            mrsm_root.set_default_instance(previous)
        mrsm_root.drop_instance(INSTANCE_LABEL)


@pytest.fixture
def mrsm_pipe(mrsm_instance) -> Iterator[Callable[..., Any]]:
    """
    A factory: `mrsm_pipe(connector_keys, metric_key, location_key=None, register=True, **kwargs)`
    returns a `Pipe` on `mrsm_instance` (registered unless `register=False`), deleted after the test.
    """
    import meerschaum as mrsm
    pipes: List[Any] = []

    def _make(
        connector_keys: str,
        metric_key: str,
        location_key: Optional[str] = None,
        register: bool = True,
        **kwargs: Any
    ):
        kwargs.pop('instance', None)
        kwargs.pop('mrsm_instance', None)
        pipe = mrsm.Pipe(connector_keys, metric_key, location_key, instance=mrsm_instance, **kwargs)
        if register:
            success, message = pipe.register()
            if not success:
                raise RuntimeError(f"Could not register {pipe}: {message}")
        pipes.append(pipe)
        return pipe

    try:
        yield _make
    finally:
        for pipe in reversed(pipes):
            try:
                pipe.delete()
            except Exception:
                pass


@pytest.fixture(scope='session')
def mrsm_api(mrsm_root) -> Iterator['MrsmApi']:
    """
    The API host app of the test session, built in this process (see `MrsmApi`). Skipped when the
    API's packages (`pip install "meerschaum[api]"`, plus `httpx` for the test client) are missing.
    """
    pytest.importorskip('fastapi', reason='mrsm_api needs `pip install "meerschaum[api]"`.')
    pytest.importorskip('httpx', reason='mrsm_api needs `httpx` for the test client.')
    from meerschaum.testing._api import MrsmApi
    api = MrsmApi(mrsm_root).start()
    yield api


@pytest.fixture
def render_page(mrsm_api) -> Callable[..., Any]:
    """
    A function: `render_page(path, as_user=None, admin=None)` renders a console or plugin page
    (`'/dash/my-page'`, or `'my-page'`) for `as_user` (a test user signed in to the console; an
    admin when `admin`, by default when the name is `admin`; anonymous when `None`) and returns a
    `RenderedPage`: its `layout`, `text`, `ids` or `html`, and `callback(...)` to fire one of its
    callbacks. Skipped without the web console (`pip install "meerschaum[dash]"`).
    """
    if not mrsm_api.console_mounted:
        pytest.skip('render_page needs the web console: `pip install "meerschaum[dash]"`.')
    from meerschaum.testing._api import render_page as _render_page

    def _render(path: str, as_user: Optional[str] = None, admin: Optional[bool] = None):
        return _render_page(mrsm_api, path, as_user=as_user, admin=admin)

    return _render
