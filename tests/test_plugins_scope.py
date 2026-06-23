#! /usr/bin/env python3
# vim:fenc=utf-8

"""
Test that switching the root / plugins-dir scope (via `replace_env`) does not leave a
stale `plugins` package cached in `sys.modules`.

Regression for the 3.4.5 fix: a cached `plugins` package whose `__path__` pointed at a
previous scope's `.internal/plugins` caused plugins to be re-imported under the wrong
scope after the scope was restored, so a plugin doing a module-level
`from_plugin_import` of a sibling failed with "Unable to import plugin 'X'".
"""

import sys
import importlib

import meerschaum as mrsm


def _write_scope_plugins(plugins_dir):
    """Write a `parent` plugin that imports a sibling `child` at module load time."""
    (plugins_dir / 'child.py').write_text(
        "__version__ = '0.0.1'\n"
        "required = []\n"
        "def hello():\n"
        "    return 'hi-from-child'\n",
        encoding='utf-8',
    )
    (plugins_dir / 'parent.py').write_text(
        "__version__ = '0.0.1'\n"
        "required = []\n"
        "from meerschaum.plugins import from_plugin_import\n"
        "hello = from_plugin_import('child', 'hello')\n",
        encoding='utf-8',
    )


def test_invalidate_plugins_cache_drops_package():
    """`invalidate_plugins_cache` removes the cached `plugins` package + submodules."""
    from meerschaum.plugins import invalidate_plugins_cache

    sys.modules['plugins'] = type(sys)('plugins')
    sys.modules['plugins.fake_sub'] = type(sys)('plugins.fake_sub')

    invalidate_plugins_cache()

    assert 'plugins' not in sys.modules
    assert 'plugins.fake_sub' not in sys.modules


def test_replace_env_invalidates_plugins_scope(tmp_path):
    """
    A `parent` plugin resolves its sibling `child` inside a temporary scope, and the
    temporary-scope `plugins` package is invalidated when the scope is restored (so it
    cannot leak into the restored scope).
    """
    from meerschaum.config.environment import replace_env
    from meerschaum.plugins import load_plugins, sync_plugins_symlinks

    root = tmp_path / 'root'
    root.mkdir()
    plugins_dir = tmp_path / 'plugins'
    plugins_dir.mkdir()
    _write_scope_plugins(plugins_dir)

    env = {
        'MRSM_ROOT_DIR': str(root),
        'MRSM_PLUGINS_DIR': str(plugins_dir),
    }

    with replace_env(env):
        sync_plugins_symlinks(warn=False)
        load_plugins(debug=False)
        ### Within the temporary scope the sibling import must resolve.
        parent = importlib.import_module('plugins.parent')
        assert parent.hello() == 'hi-from-child'

    ### The fix pops the temporary-scope `plugins` package on `replace_env` exit so the
    ### restored scope re-discovers plugins cleanly. Without it, this lingers with a
    ### stale `__path__`.
    assert 'plugins' not in sys.modules
    assert 'plugins.parent' not in sys.modules
    assert 'plugins.child' not in sys.modules
