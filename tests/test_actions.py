#! /usr/bin/env python3
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

"""
Tes
"""


def test_upgrade_plugins_clears_update_cache(tmp_path, monkeypatch):
    """A successful plugin upgrade must not leave a stale shell notice behind."""
    from meerschaum.actions import actions
    from meerschaum.actions.upgrade import _upgrade_plugins
    from meerschaum.config import paths

    cache_path = tmp_path / 'plugins.json'
    cache_path.write_text('{}')
    monkeypatch.setattr(paths, 'PLUGIN_UPDATES_CACHE_PATH', cache_path)
    monkeypatch.setitem(actions, 'install', lambda **kw: (True, 'Success'))

    assert _upgrade_plugins(action=['mcp'], force=True) == (True, 'Success')
    assert not cache_path.exists()
