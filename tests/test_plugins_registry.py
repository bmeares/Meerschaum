#! /usr/bin/env python3


def test_plugin_setup_deactivates_venv_after_exception(monkeypatch):
    """A failed setup hook must not leak its virtual environment."""
    import types
    from meerschaum.core.Plugin import Plugin

    def fail_setup():
        raise RuntimeError("boom")

    plugin = Plugin('failed_setup')
    plugin._module = types.SimpleNamespace(setup=fail_setup)
    events = []
    monkeypatch.setattr(plugin, 'activate_venv', lambda **kw: events.append('activate'))
    monkeypatch.setattr(plugin, 'deactivate_venv', lambda **kw: events.append('deactivate'))

    assert plugin.setup() == (False, "boom")
    assert events == ['activate', 'deactivate']


def test_failed_plugin_install_does_not_poison_next_attempt(tmp_path, monkeypatch):
    """A failed install must not remain marked as active."""
    import meerschaum.config.paths as paths
    from meerschaum.core.Plugin import Plugin

    monkeypatch.setattr(paths, 'PLUGINS_TEMP_RESOURCES_PATH', tmp_path)
    plugin = Plugin('missing_install', archive_path=tmp_path / 'missing.tar.gz')

    assert plugin.install(skip_deps=True)[0] is False
    assert plugin.install(skip_deps=True)[0] is False


def test_plugin_install_uses_environment_lock(monkeypatch):
    """Plugin source and dependency mutations must share one process lock."""
    from contextlib import contextmanager
    import meerschaum.utils.packages as packages
    from meerschaum.core.Plugin import Plugin

    events = []

    @contextmanager
    def lock(name):
        events.append(('enter', name))
        yield
        events.append(('exit', name))

    plugin = Plugin('locked_install')
    monkeypatch.setattr(packages, '_pip_install_lock', lock)
    monkeypatch.setattr(plugin, '_install', lambda **kw: (True, "Success"))

    assert plugin.install() == (True, "Success")
    assert events == [('enter', 'locked_install'), ('exit', 'locked_install')]


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
