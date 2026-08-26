#! /usr/bin/env python3

"""Focused parity tests for the built-in Compose action."""

import json
import pathlib


def test_compose_is_native_and_preserves_arbitrary_action_passthrough(monkeypatch):
    """The core action wins and forwards unknown subactions to Compose's default path."""
    from meerschaum.actions import actions
    import meerschaum.compose.subactions as subactions

    captured = {}

    def fake_do_subaction(subaction, **kwargs):
        captured.update({'subaction': subaction, **kwargs})
        return True, "Success"

    monkeypatch.setattr(subactions, '_do_subaction', fake_do_subaction)
    success, _ = actions['compose'](
        action=['show', 'pipes'],
        file=pathlib.Path('alternate.yaml'),
    )

    assert success
    assert actions['compose'].__module__ == 'meerschaum.actions.compose'
    assert captured['subaction'] == 'show'
    assert captured['action'] == ['show', 'pipes']
    assert captured['file'] == pathlib.Path('alternate.yaml')


def test_compose_arguments_are_available_without_plugin():
    """Former plugin flags are parsed by core, including their legacy aliases."""
    from meerschaum._internal.arguments import parse_arguments

    args = parse_arguments([
        'compose', 'up',
        '--file', 'project.yaml',
        '--env-file', 'project.env',
        '--dry', '--presync', '--no-jobs', '--isolated',
    ])
    down_args = parse_arguments(['compose', 'down', '-v'])

    assert args['file'] == pathlib.Path('project.yaml')
    assert args['env_file'] == pathlib.Path('project.env')
    assert all(args[key] for key in ('dry', 'presync', 'no_jobs', 'isolated'))
    assert down_args['drop'] is True


def test_compose_config_resolves_project_paths_and_environment(tmp_path, monkeypatch):
    """Compose files retain env substitution and multiple project plugin directories."""
    from meerschaum.compose.utils.config import read_compose_config, get_env_dict

    monkeypatch.setenv('COMPOSE_TEST_DATABASE', 'sqlite:///project.db')
    compose_path = tmp_path / 'alternate.yaml'
    compose_path.write_text(
        """\
project_name: test-project
root_dir: ./roots/root
plugins_dir:
  - ./installed-plugins
  - ./plugins
connectors:
  sql:
    project:
      uri: $COMPOSE_TEST_DATABASE
jobs:
  api: start api
""",
        encoding='utf-8',
    )

    config = read_compose_config(compose_path)
    env = get_env_dict(config)

    assert config['project_name'] == 'test-project'
    assert config['root_dir'] == (tmp_path / 'roots' / 'root').resolve()
    assert config['connectors']['sql']['project']['uri'] == 'sqlite:///project.db'
    assert config['plugins_dir'] == [
        (tmp_path / 'installed-plugins').resolve(),
        (tmp_path / 'plugins').resolve(),
    ]
    assert json.loads(env['MRSM_PLUGINS_DIR']) == [
        (tmp_path / 'installed-plugins').resolve().as_posix(),
        (tmp_path / 'plugins').resolve().as_posix(),
    ]


def test_compose_init_does_not_bootstrap_plugin(tmp_path, monkeypatch):
    """Initializing a native project must not inject the old Compose plugin."""
    from meerschaum.compose import utils
    import meerschaum.compose.utils.config as config_module
    import meerschaum.plugins as plugins

    compose_path = tmp_path / 'mrsm-compose.yaml'
    compose_path.touch()
    expected = {'root_dir': tmp_path / 'root'}
    monkeypatch.setattr(config_module, 'infer_compose_file_path', lambda file=None: compose_path)
    monkeypatch.setattr(config_module, 'init_env', lambda *args, **kwargs: None)
    monkeypatch.setattr(config_module, 'read_compose_config', lambda *args, **kwargs: expected)
    monkeypatch.setattr(config_module, 'init_root', lambda *args, **kwargs: True)
    monkeypatch.setattr(
        plugins,
        'inject_plugin_path',
        lambda *args, **kwargs: (_ for _ in ()).throw(AssertionError("plugin was injected")),
    )

    assert utils.init(file=compose_path) is expected
    assert not (tmp_path / 'root' / '.internal' / 'plugins' / 'compose').exists()


def test_compose_plugin_deprecation_warning_is_once(monkeypatch):
    """An installed legacy plugin produces one actionable warning per process."""
    import meerschaum.compose as compose_module
    import meerschaum.utils.warnings as warnings_module

    messages = []
    monkeypatch.setattr(compose_module, 'has_compose_plugin', lambda: True)
    monkeypatch.setattr(warnings_module, 'warn', lambda message, **kwargs: messages.append(message))
    monkeypatch.setattr(compose_module, '_warned_about_plugin', False)

    compose_module.warn_if_compose_plugin_installed()
    compose_module.warn_if_compose_plugin_installed()

    assert len(messages) == 1
    assert 'mrsm uninstall plugin compose' in messages[0]


def test_compose_explicit_jobs_keep_established_flags():
    """Explicit job commands retain tags, names, daemon, and force defaults."""
    from meerschaum.compose.utils.jobs import get_jobs_commands

    commands = get_jobs_commands({
        'project_name': 'test-project',
        'jobs': {'api': 'start api --port 9000'},
    })

    assert commands['api'] == [
        'start', 'api', '--port', '9000',
        '-t', 'test-project', '--name', 'api', '-d', '-f',
    ]


def test_compose_programmatic_plugin_import_resolves_to_core():
    """Legacy programmatic imports survive uninstalling the Compose plugin."""
    from meerschaum.plugins import from_plugin_import

    get_defined_pipes = from_plugin_import('compose.utils.pipes', 'get_defined_pipes')
    assert get_defined_pipes.__module__ == 'meerschaum.compose.utils.pipes'


def test_legacy_compose_plugin_connector_resolves_to_core(monkeypatch):
    """Stored ``plugin:compose`` pipes retain their connector after plugin removal."""
    from meerschaum.core import Plugin
    from meerschaum.connectors.plugin import PluginConnector
    import meerschaum.compose as compose_module

    monkeypatch.setattr(Plugin, 'module', property(lambda self: None))
    connector = PluginConnector('compose')
    assert connector.sync is compose_module.sync


def test_compose_path_resolution_does_not_change_cwd(tmp_path):
    """Project-relative paths and env files resolve without mutating process cwd."""
    import os
    import meerschaum.compose.utils.config as config_module

    project_dir = tmp_path / 'project'
    project_dir.mkdir()
    compose_path = project_dir / 'mrsm-compose.yaml'
    compose_path.write_text('root_dir: ./state\n', encoding='utf-8')
    env_path = project_dir / 'custom.env'
    env_path.write_text('COMPOSE_CWD_TEST=ok\n', encoding='utf-8')
    original_cwd = os.getcwd()

    config = config_module.read_compose_config(compose_path, env_file=pathlib.Path('custom.env'))
    config_module.init_env(compose_path, pathlib.Path('custom.env'))

    assert os.getcwd() == original_cwd
    assert config['root_dir'] == (project_dir / 'state').resolve()
    assert os.environ['COMPOSE_CWD_TEST'] == 'ok'


def test_compose_cache_is_safe_text_and_scoped_per_project(tmp_path):
    """Compose caches are inert hashes, and one project cannot cache another's result."""
    import meerschaum.compose.utils.config as config_module

    configs = []
    for name in ('one', 'two'):
        root = tmp_path / name
        root.mkdir()
        configs.append({'root_dir': root, 'project_name': name})

    config_module.CONFIG_METADATA.clear()
    config_module.write_config_cache(configs[0])
    cache_path = config_module.get_config_cache_path(configs[0])
    assert len(cache_path.read_text(encoding='utf-8')) == 64
    assert config_module.config_has_changed(configs[0]) is False
    assert config_module.config_has_changed(configs[1]) is True

    cache_path.write_bytes(b'\x80unsafe legacy pickle')
    config_module.CONFIG_METADATA.clear()
    assert config_module.read_config_cache(configs[0]) is None


def test_compose_temporary_pipe_retry_returns_success():
    """A failed temporary pipe may succeed on its second verification pass."""
    from meerschaum.compose.subactions.up import run_initial_syncs

    class TemporaryPipe:
        temporary = True

        def __init__(self):
            self.calls = 0

        def sync(self, **kwargs):
            self.calls += 1
            return (self.calls == 2, 'ok' if self.calls == 2 else 'retry')

        def __str__(self):
            return 'temporary pipe'

    pipe = TemporaryPipe()
    success, msg = run_initial_syncs([pipe], {'project_name': 'test'})
    assert success, msg
    assert pipe.calls == 2


def test_compose_restores_host_plugins_after_subaction_error(monkeypatch):
    """Project plugins are unloaded and host plugins reloaded when a subaction raises."""
    from contextlib import nullcontext
    import pytest
    import meerschaum.compose.subactions as subactions
    import meerschaum.compose.utils as compose_utils
    import meerschaum.compose.utils.config as config_module
    import meerschaum.plugins as plugins
    import meerschaum.config as config
    import meerschaum.config.environment as environment

    unloads = []
    loads = []
    plugin_scopes = iter((['host'], ['project']))

    def fail(*args, **kwargs):
        raise RuntimeError('subaction failed')

    monkeypatch.setattr(subactions, 'get_subactions', lambda: ['fail'])
    monkeypatch.setattr(subactions, '_get_subaction_function', lambda name: fail)
    monkeypatch.setattr(compose_utils, 'init', lambda **kwargs: {'config': {}})
    monkeypatch.setattr(config_module, 'get_env_dict', lambda compose_config: {})
    monkeypatch.setattr(plugins, 'get_plugins_names', lambda: next(plugin_scopes))
    monkeypatch.setattr(plugins, 'unload_plugins', lambda names, **kwargs: unloads.append(names))
    monkeypatch.setattr(plugins, 'load_plugins', lambda **kwargs: loads.append(True))
    monkeypatch.setattr(config, 'replace_config', lambda value: nullcontext())
    monkeypatch.setattr(environment, 'replace_env', lambda value: nullcontext())

    with pytest.raises(RuntimeError, match='subaction failed'):
        subactions._do_subaction('fail')

    assert unloads == [['host'], ['project']]
    assert len(loads) == 2
