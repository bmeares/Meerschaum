#! /usr/bin/env python3
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

"""
Regression tests for `from_plugin_import`: already-imported plugins are served from
`sys.modules` without rebuilding `Plugin` objects or activating venvs, and failed imports
return one `None` per requested attribute.
"""

import pathlib
import warnings

import pytest

import meerschaum as mrsm
from meerschaum._internal.static import STATIC_CONFIG
from meerschaum.config.environment import replace_env

PLUGIN_INIT_SOURCE = """
required = []

def fetch(pipe, **kwargs):
    return None
"""

PLUGIN_SUB_SOURCE = """
def photo_url(x):
    return f'/photos/{x}'

VERSION = 1
"""


@pytest.fixture
def project_scope(tmp_path):
    """Create a temporary root + plugins dir containing a package plugin with a submodule."""
    root_dir = tmp_path / 'root'
    plugins_dir = tmp_path / 'project_plugins'
    plugin_dir = plugins_dir / 'fpi_plug'
    plugin_dir.mkdir(parents=True)
    (plugin_dir / '__init__.py').write_text(PLUGIN_INIT_SOURCE)
    (plugin_dir / 'sub.py').write_text(PLUGIN_SUB_SOURCE)
    return {
        STATIC_CONFIG['environment']['root']: str(root_dir),
        STATIC_CONFIG['environment']['plugins']: str(plugins_dir),
    }


def _sync():
    from meerschaum.plugins import sync_plugins_symlinks
    sync_plugins_symlinks()


def test_warm_from_plugin_import_skips_plugin_construction(project_scope, monkeypatch):
    """Once a submodule is imported, later calls must not build a `Plugin` or enter a venv."""
    from meerschaum.plugins import from_plugin_import
    with replace_env(project_scope):
        _sync()
        photo_url = from_plugin_import('fpi_plug.sub', 'photo_url')
        assert photo_url('a') == '/photos/a'

        def _fail(*args, **kwargs):
            raise AssertionError("Plugin should not be constructed for a loaded submodule.")

        monkeypatch.setattr(mrsm, 'Plugin', _fail)
        assert from_plugin_import('fpi_plug.sub', 'photo_url') is photo_url
        assert from_plugin_import('plugins.fpi_plug.sub', 'photo_url') is photo_url
        url, version = from_plugin_import('fpi_plug.sub', 'photo_url', 'VERSION')
        assert url is photo_url
        assert version == 1


def test_from_plugin_import_after_reload(project_scope):
    """Reloading plugins must drop the cached submodule so new code is returned."""
    from meerschaum.plugins import from_plugin_import, reload_plugins
    plugins_dir = pathlib.Path(project_scope[STATIC_CONFIG['environment']['plugins']])
    with replace_env(project_scope):
        _sync()
        assert from_plugin_import('fpi_plug.sub', 'VERSION') == 1
        (plugins_dir / 'fpi_plug' / 'sub.py').write_text(
            ### Change the file size so a same-second rewrite doesn't reuse the stale bytecode.
            PLUGIN_SUB_SOURCE.replace('VERSION = 1', 'VERSION = 22')
        )
        reload_plugins(['fpi_plug'])
        assert from_plugin_import('fpi_plug.sub', 'VERSION') == 22


def test_failed_submodule_import_returns_none_per_attr(project_scope):
    """A missing submodule must still unpack into one `None` per requested name."""
    from meerschaum.plugins import from_plugin_import
    with replace_env(project_scope):
        _sync()
        with warnings.catch_warnings():
            warnings.simplefilter('ignore')
            a, b = from_plugin_import('fpi_plug.missing', 'a', 'b')
            single = from_plugin_import('fpi_plug.missing', 'a')
        assert a is None and b is None
        assert single is None


def test_plugin_module_reuses_loaded_module(project_scope, monkeypatch):
    """A fresh `Plugin` object must reuse the imported module instead of re-importing."""
    import meerschaum.plugins as plugins_module
    with replace_env(project_scope):
        _sync()
        assert plugins_module.from_plugin_import('fpi_plug.sub', 'VERSION') == 1
        module = mrsm.Plugin('fpi_plug').module
        assert module is not None

        def _fail(*args, **kwargs):
            raise AssertionError("import_plugins should not run for a loaded plugin.")

        monkeypatch.setattr(plugins_module, 'import_plugins', _fail)
        assert mrsm.Plugin('fpi_plug').module is module
