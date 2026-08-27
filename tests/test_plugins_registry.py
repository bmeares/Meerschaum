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
    import meerschaum.config._paths as paths
    from meerschaum.core.Plugin import Plugin

    monkeypatch.setitem(paths.paths, 'PLUGINS_TEMP_RESOURCES_PATH', str(tmp_path))
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
    monkeypatch.setattr(plugin, '_install_with_rollback', lambda **kw: (True, "Success"))

    assert plugin.install() == (True, "Success")
    assert events == [('enter', 'locked_install'), ('exit', 'locked_install')]


def test_failed_plugin_install_restores_source_and_environment(tmp_path, monkeypatch):
    """Dependency failures must leave the prior plugin fully intact."""
    import tarfile
    import meerschaum.config._paths as paths
    import meerschaum.plugins as plugins
    import meerschaum.utils.packages as packages
    import meerschaum.utils.venv as venv
    from packaging import version as packaging_version
    from meerschaum.core.Plugin import Plugin

    plugin_name = 'rollback_install'
    plugins_dir = tmp_path / 'plugins'
    temp_dir = tmp_path / 'temp'
    venvs_dir = tmp_path / 'venvs'
    archive_dir = tmp_path / 'archive'
    for path in (plugins_dir, temp_dir, venvs_dir, archive_dir):
        path.mkdir()

    installed_source = plugins_dir / (plugin_name + '.py')
    installed_source.write_text("__version__ = '1.0.0'\nOLD = True\n", encoding='utf-8')
    environment = venvs_dir / plugin_name
    environment.mkdir()
    marker = environment / 'marker.txt'
    marker.write_text('old', encoding='utf-8')
    new_source = archive_dir / (plugin_name + '.py')
    new_source.write_text("__version__ = '2.0.0'\nNEW = True\n", encoding='utf-8')
    archive_path = tmp_path / (plugin_name + '.tar.gz')
    with tarfile.open(archive_path, 'w:gz') as archive:
        archive.add(new_source, arcname=new_source.name)

    monkeypatch.setattr(paths, 'PLUGINS_DIR_PATHS', [plugins_dir])
    monkeypatch.setitem(paths.paths, 'PLUGINS_RESOURCES_PATH', str(plugins_dir))
    monkeypatch.setitem(paths.paths, 'PLUGINS_TEMP_RESOURCES_PATH', str(temp_dir))
    monkeypatch.setitem(paths.paths, 'VIRTENV_RESOURCES_PATH', str(venvs_dir))
    monkeypatch.setattr(plugins, 'sync_plugins_symlinks', lambda **kw: None)
    monkeypatch.setattr(packages, 'reload_meerschaum', lambda **kw: (True, "Success"))
    monkeypatch.setattr(packages, 'attempt_import', lambda *args, **kw: packaging_version)

    def mutate_environment(*args, **kw):
        venv_name = kw.get('venv', args[0] if args else None)
        if venv_name == plugin_name:
            marker.write_text('new', encoding='utf-8')
        return True

    monkeypatch.setattr(venv, 'init_venv', mutate_environment)
    plugin = Plugin(plugin_name, version='1.0.0', archive_path=archive_path)
    monkeypatch.setattr(plugin, 'install_dependencies', lambda **kw: False)

    success, _ = plugin.install(force=True)

    assert not success
    assert installed_source.read_text(encoding='utf-8') == "__version__ = '1.0.0'\nOLD = True\n"
    assert marker.read_text(encoding='utf-8') == 'old'


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
