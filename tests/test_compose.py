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
