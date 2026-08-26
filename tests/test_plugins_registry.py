#! /usr/bin/env python3


def test_plugin_registries_use_root_module_and_unload(monkeypatch):
    """Decorators in plugin submodules share one owner and unload together."""
    import sys
    import types
    import meerschaum.actions as actions_module
    import meerschaum.connectors as connectors_module
    import meerschaum.plugins as plugins

    def plugin_function(*args, **kw):
        return True, "Success"

    plugin_function.__module__ = 'plugins.example.submodule'
    plugins.make_action(plugin_function, skip_if_loaded=False)
    plugins.pre_sync_hook(plugin_function)
    plugins.post_sync_hook(plugin_function)
    plugins.api_plugin(plugin_function)
    plugins.dash_plugin(plugin_function)

    assert actions_module._custom_actions_plugins[plugin_function.__name__] == 'example'
    assert plugin_function in plugins._pre_sync_hooks['example']
    assert plugin_function in plugins._post_sync_hooks['example']
    assert plugin_function in plugins._api_plugins['example']
    assert plugin_function in plugins._dash_plugins['example']

    monkeypatch.setattr(plugins, 'get_plugins_names', lambda: ['example'])
    monkeypatch.setattr(connectors_module, 'unload_plugin_connectors', lambda *args, **kw: None)
    sys.modules['plugins'] = types.ModuleType('plugins')
    plugins.unload_plugins(['example'], remove_symlinks=False)

    assert 'plugins' not in sys.modules
    assert 'example' not in plugins._pre_sync_hooks
    assert 'example' not in plugins._post_sync_hooks
    assert 'example' not in plugins._api_plugins
    assert 'example' not in plugins._dash_plugins
