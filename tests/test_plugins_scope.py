#! /usr/bin/env python3
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

"""
Regression tests for plugin loading across in-process scope switches
(`meerschaum.config.environment.replace_env`).

Before the fix, the `plugins` package lingered in `sys.modules` with a
`__path__` pointing at the previous scope's `.internal/plugins`, so a plugin
doing a module-level `from_plugin_import` of a sibling failed with
"Unable to import plugin '<name>'" after a scope switch.
"""

import sys
import pathlib

import pytest

import meerschaum as mrsm
from meerschaum._internal.static import STATIC_CONFIG
from meerschaum.config.environment import replace_env

DEP_PLUGIN_SOURCE = """
def get_value():
    return 'scoped-dep-value'
"""

USER_PLUGIN_SOURCE = """
from meerschaum.plugins import from_plugin_import

get_value = from_plugin_import('plugins.scope_dep', 'get_value')

def get_dep_value():
    return get_value()
"""


@pytest.fixture
def project_scope(tmp_path):
    """Create a temporary root + plugins dir containing two sibling plugins."""
    root_dir = tmp_path / 'root'
    plugins_dir = tmp_path / 'project_plugins'
    plugins_dir.mkdir(parents=True)
    (plugins_dir / 'scope_dep.py').write_text(DEP_PLUGIN_SOURCE)
    (plugins_dir / 'scope_user.py').write_text(USER_PLUGIN_SOURCE)
    return {
        STATIC_CONFIG['environment']['root']: str(root_dir),
        STATIC_CONFIG['environment']['plugins']: str(plugins_dir),
    }


def _load_scope_plugins():
    from meerschaum.plugins import sync_plugins_symlinks, load_plugins
    sync_plugins_symlinks()
    load_plugins(skip_if_loaded=False)


def test_replace_env_invalidates_plugins_cache(project_scope):
    """
    Entering and exiting `replace_env` with a different root/plugins-dir
    must drop the cached `plugins` package from `sys.modules`.
    """
    import meerschaum.config.paths as paths
    plugins_stem = paths.PLUGINS_RESOURCES_PATH.stem

    with replace_env(project_scope):
        assert plugins_stem not in sys.modules
        _load_scope_plugins()
        plugins_mod = sys.modules.get(plugins_stem)
        assert plugins_mod is not None
        assert str(paths.PLUGINS_RESOURCES_PATH) in [
            str(pathlib.Path(path_str)) for path_str in plugins_mod.__path__
        ]

    ### On exit, the package cached under the project scope must be gone.
    assert plugins_stem not in sys.modules


def test_sibling_plugin_import_after_scope_switch(project_scope):
    """
    A plugin doing a module-level `from_plugin_import` of a sibling must be
    importable after switching scopes (the original `mrsm compose` failure).
    """
    with replace_env(project_scope):
        _load_scope_plugins()

    ### Re-enter the scope (like a second compose subaction) — before the fix,
    ### submodule discovery walked the stale scope's directory.
    with replace_env(project_scope):
        _load_scope_plugins()
        plugin = mrsm.Plugin('scope_user')
        assert plugin.module is not None, "Unable to import plugin 'scope_user'."
        assert plugin.module.get_dep_value() == 'scoped-dep-value'
