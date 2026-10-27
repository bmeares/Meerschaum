#! /usr/bin/env python3
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

"""
The temporary Meerschaum root of a test session (`mrsm_root`) and its temporary SQLite instances
(`mrsm_instance`).
"""

from __future__ import annotations

import copy
import json
import os
import pathlib
import textwrap
from typing import Any, Dict, Iterable, List, Optional, Union

#: What a test session never inherits: every Meerschaum variable, and the user's tmux server
#: (a fixture that started tmux would otherwise reach the developer's own session).
SCRUBBED_PREFIX: str = 'MRSM_'
SCRUBBED_NAMES = ('TMUX', 'TMUX_PANE')


class MrsmRoot:
    """
    A temporary root directory and plugins directory, active for the whole test session:
    `os.environ` has no other `MRSM_*` variables (nor `TMUX`/`TMUX_PANE`), and Meerschaum's paths,
    config and connectors point here. Subprocesses (jobs, `mrsm`, workers) inherit it.

    Attributes
    ----------
    path: pathlib.Path
        The root directory (`MRSM_ROOT_DIR`).

    plugins_dir: pathlib.Path
        The temporary plugins directory, first in `MRSM_PLUGINS_DIR` (see `write_plugin()`).

    plugins_dirs: List[pathlib.Path]
        Every plugins directory: `plugins_dir`, then the ones given with `--mrsm-plugins-dir` or
        the `mrsm_plugins_dirs` ini option (the plugins under test).
    """

    def __init__(
        self,
        path: Union[str, pathlib.Path],
        plugins_dirs: Optional[Iterable[Union[str, pathlib.Path]]] = None,
        venvs_dir: Union[str, pathlib.Path, None] = None,
        extra_env: Optional[Dict[str, str]] = None,
    ):
        self.path = pathlib.Path(path).resolve()
        self.extra_env = dict(extra_env or {})
        self.plugins_dir = self.path / 'plugins'
        self.plugins_dirs: List[pathlib.Path] = [self.plugins_dir] + [
            pathlib.Path(plugins_dir).resolve() for plugins_dir in (plugins_dirs or [])
        ]
        self.venvs_dir = pathlib.Path(venvs_dir).resolve() if venvs_dir else None
        self.active = False
        self._saved: Dict[str, Any] = {}

    def __repr__(self) -> str:
        return f"MrsmRoot('{self.path}')"

    ### Environment

    def env(self) -> Dict[str, str]:
        """
        The Meerschaum variables of this root (`MRSM_ROOT_DIR`, `MRSM_PLUGINS_DIR`, ...), as set in
        `os.environ` while the root is active.
        """
        env = {
            'MRSM_ROOT_DIR': str(self.path),
            'MRSM_PLUGINS_DIR': (
                str(self.plugins_dirs[0])
                if len(self.plugins_dirs) == 1
                else json.dumps([str(path) for path in self.plugins_dirs])
            ),
            ### Jobs run locally (a temporary root never installs systemd units).
            'MRSM_CONFIG': json.dumps({'meerschaum': {'executor': 'local'}}),
        }
        if self.venvs_dir is not None:
            env['MRSM_VENVS_DIR'] = str(self.venvs_dir)
        env.update(self.extra_env)
        return env

    def activate(self) -> 'MrsmRoot':
        """
        Point the environment, Meerschaum's paths, config and connectors at this root.
        """
        if self.active:
            return self
        import meerschaum.config._paths as _paths
        import meerschaum.connectors as connectors_module
        from meerschaum.config import _config, set_config
        from meerschaum.config.environment import apply_environment_patches

        self.path.mkdir(parents=True, exist_ok=True)
        for plugins_dir in self.plugins_dirs:
            plugins_dir.mkdir(parents=True, exist_ok=True)
        if self.venvs_dir is not None:
            self.venvs_dir.mkdir(parents=True, exist_ok=True)

        self._saved = {
            'environ': dict(os.environ),
            'root': _paths.paths.get('ROOT_DIR_PATH'),
            'plugins_dirs': list(getattr(_paths, 'PLUGINS_DIR_PATHS', []) or []),
            'venvs': _paths.paths.get('VIRTENV_RESOURCES_PATH'),
            'config_dir': _paths.paths.get('CONFIG_DIR_PATH'),
            'config': copy.deepcopy(_config()),
            'connectors': {typ: dict(labels) for typ, labels in connectors_module.connectors.items()},
        }

        for key in [key for key in os.environ if key.startswith(SCRUBBED_PREFIX)]:
            del os.environ[key]
        for key in SCRUBBED_NAMES:
            os.environ.pop(key, None)
        env = self.env()
        os.environ.update(env)

        _unload_plugins()
        _paths.set_root(self.path)
        _paths.set_config_dir_path(self.path / 'config')
        _paths.set_plugins_dir_paths(list(self.plugins_dirs))
        if self.venvs_dir is not None:
            _paths.set_venvs_dir_path(self.venvs_dir)
        else:
            _paths.paths['VIRTENV_RESOURCES_PATH'] = ('{ROOT_DIR_PATH}', 'venvs')
            vars(_paths).pop('VIRTENV_RESOURCES_PATH', None)

        ### A fresh config (read from this root) and no connector built for the previous one.
        set_config({})
        apply_environment_patches(env)
        for labels in connectors_module.connectors.values():
            labels.clear()

        from meerschaum.plugins import invalidate_plugins_cache
        invalidate_plugins_cache()
        self.active = True
        return self

    def deactivate(self) -> None:
        """
        Restore what `activate()` replaced.
        """
        if not self.active:
            return
        import meerschaum.config._paths as _paths
        import meerschaum.connectors as connectors_module
        from meerschaum.config import _config, set_config
        from meerschaum.plugins import invalidate_plugins_cache

        saved = self._saved
        _unload_plugins()
        for key in [key for key in os.environ if key not in saved['environ']]:
            del os.environ[key]
        for key, value in saved['environ'].items():
            if os.environ.get(key) != value:
                os.environ[key] = value

        if saved['root'] is not None:
            _paths.set_root(saved['root'])
        if saved['plugins_dirs']:
            _paths.set_plugins_dir_paths(saved['plugins_dirs'])
        _paths.paths['VIRTENV_RESOURCES_PATH'] = saved['venvs']
        vars(_paths).pop('VIRTENV_RESOURCES_PATH', None)
        if saved['config_dir'] is not None:
            _paths.paths['CONFIG_DIR_PATH'] = saved['config_dir']

        _config().clear()
        set_config(saved['config'])
        for typ, labels in connectors_module.connectors.items():
            labels.clear()
            labels.update(saved['connectors'].get(typ, {}))
        invalidate_plugins_cache()
        self.active = False

    ### Plugins

    def write_plugin(
        self,
        name: str,
        source: str,
        files: Optional[Dict[str, Union[str, bytes]]] = None,
    ) -> pathlib.Path:
        """
        Write a plugin into the session's plugins directory (`<plugins_dir>/<name>/__init__.py`,
        plus `files` relative to it) and reload the plugins. Return the plugin's directory.
        """
        plugin_dir = self.plugins_dir / name
        plugin_dir.mkdir(parents=True, exist_ok=True)
        (plugin_dir / '__init__.py').write_text(textwrap.dedent(source), encoding='utf-8')
        for relative, content in (files or {}).items():
            target = plugin_dir / relative
            target.parent.mkdir(parents=True, exist_ok=True)
            if isinstance(content, bytes):
                target.write_bytes(content)
            else:
                target.write_text(content, encoding='utf-8')
        self.reload_plugins()
        return plugin_dir

    def reload_plugins(self) -> None:
        """
        Load the plugins of this root again (after a plugin's files changed).
        """
        _unload_plugins()
        from meerschaum.plugins import invalidate_plugins_cache, sync_plugins_symlinks, load_plugins
        invalidate_plugins_cache()
        sync_plugins_symlinks()
        load_plugins(skip_if_loaded=False)

    ### Instances

    def sqlite_instance(self, label: str, database: Union[str, pathlib.Path]):
        """
        Define `sql:<label>`, a SQLite instance in `database`, for this process (and subprocesses,
        through `MRSM_SQL_<LABEL>`). Return the connector.
        """
        import meerschaum as mrsm
        from meerschaum.config.environment import apply_connector_uri
        database = pathlib.Path(database).resolve()
        database.parent.mkdir(parents=True, exist_ok=True)
        env_var = f"MRSM_SQL_{label.upper()}"
        os.environ[env_var] = f"sqlite:///{database.as_posix()}"
        apply_connector_uri(env_var)
        return mrsm.get_connector('sql', label, refresh=True, flavor='sqlite', database=str(database))

    def drop_instance(self, label: str) -> None:
        """
        Forget `sql:<label>` (its variable, its config and its connector).
        """
        import meerschaum.connectors as connectors_module
        from meerschaum.config import get_config
        os.environ.pop(f"MRSM_SQL_{label.upper()}", None)
        conn = connectors_module.connectors.get('sql', {}).pop(label, None)
        try:
            sql_config = get_config('meerschaum', 'connectors', 'sql', warn=False) or {}
            sql_config.pop(label, None)
        except Exception:
            pass
        engine = getattr(conn, '__dict__', {}).get('_engine', None) if conn is not None else None
        if engine is not None:
            try:
                engine.dispose()
            except Exception:
                pass

    def set_default_instance(self, instance_keys: Optional[str]) -> Optional[str]:
        """
        Make `instance_keys` the default instance (`meerschaum:instance`, `api_instance`) here and in
        subprocesses (`MRSM_CONFIG`). Return the previous default.
        """
        from meerschaum.config import get_config
        meerschaum_config = get_config('meerschaum')
        previous = meerschaum_config.get('instance', None)
        if instance_keys is None:
            return previous
        meerschaum_config['instance'] = instance_keys
        meerschaum_config['api_instance'] = instance_keys
        patch = json.loads(os.environ.get('MRSM_CONFIG', '') or '{}')
        patch.setdefault('meerschaum', {}).update({
            'instance': instance_keys,
            'api_instance': instance_keys,
        })
        os.environ['MRSM_CONFIG'] = json.dumps(patch)
        return previous


def _unload_plugins() -> None:
    """
    Forget the plugins loaded in this process (their actions, connectors, hooks, routes and
    pages) without touching any plugins directory.
    """
    import sys
    import meerschaum.plugins as plugins_module
    from meerschaum.plugins import unload_custom_actions
    try:
        unload_custom_actions()
    except Exception:
        pass
    for registry in (
        plugins_module._api_plugins,
        plugins_module._dash_plugins,
        plugins_module._pre_sync_hooks,
        plugins_module._post_sync_hooks,
        plugins_module._plugin_endpoints_to_pages,
        plugins_module._plugins_web_pages,
        plugins_module._console_head,
        plugins_module._plugins_import_errors,
        plugins_module._data_plugins_cache,
    ):
        registry.clear()
    connectors_module = sys.modules.get('meerschaum.connectors', None)
    if connectors_module is not None:
        plugin_names = list(getattr(connectors_module, 'plugins_types', {}) or {})
        try:
            connectors_module.unload_plugin_connectors(plugin_names or [None])
        except Exception:
            pass
        getattr(connectors_module, 'plugins_types', {}).clear()
    mcp_registry = sys.modules.get('meerschaum.mcp._registry', None)
    if mcp_registry is not None and hasattr(mcp_registry, 'unload_plugin_registrations'):
        try:
            mcp_registry.unload_plugin_registrations()
        except Exception:
            pass
