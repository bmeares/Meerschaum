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


def test_compose_arguments_do_not_steal_unrelated_subprocess_flags():
    """Native Compose flags remain scoped to the Compose action."""
    from meerschaum._internal.arguments import parse_arguments

    verbose_args = parse_arguments(['install', 'packages', 'example', '-v'])
    file_args = parse_arguments(['install', 'packages', 'example', '--file', 'requirements.txt'])

    assert 'drop' not in verbose_args
    assert verbose_args['sub_args'] == ['-v']
    assert 'file' not in file_args
    assert file_args['sub_args'] == ['--file', 'requirements.txt']


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
    assert env.get('PATH')
    assert env.get('HOME')


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
    """Explicit job commands retain their public names and established flags."""
    from meerschaum.compose.utils.jobs import get_jobs_commands

    commands = get_jobs_commands({
        'project_name': 'test-project',
        'jobs': {'api': 'start api --port 9000'},
    })

    assert commands['api'] == [
        'start', 'api', '--port', '9000',
        '-t', 'test-project', '--name', 'api', '-d', '-f',
    ]


class _ComposeTestJob:
    def __init__(self, sysargs):
        self.sysargs = sysargs


class _ComposeTestConnector:
    def __init__(self, parameters):
        self.parameters = parameters

    def get_pipe_attributes(self, pipe):
        return {'parameters': self.parameters}


class _ComposeTestPipe:
    def __init__(self, tags):
        self.id = 1
        self.instance_keys = 'sql:test'
        self.instance_connector = _ComposeTestConnector({'tags': tags, 'custom': True})
        self.parameters = {}
        self.edits = 0
        self.deletes = 0

    def __str__(self):
        return 'Pipe(test, ownership)'

    @property
    def tags(self):
        return self.parameters.get('tags', [])

    @tags.setter
    def tags(self, tags):
        self.parameters['tags'] = tags

    def edit(self, **kwargs):
        self.edits += 1
        return True, "Success"

    def delete(self, **kwargs):
        self.deletes += 1
        return True, "Success"


def test_compose_job_collisions_and_deletes_require_ownership():
    """Public names remain stable, and replacing them requires matching ownership."""
    from meerschaum.compose.utils.jobs import get_jobs_commands, get_project_job_names

    config = {'project_name': 'alpha', 'jobs': {'api': 'start api'}}
    assert list(get_jobs_commands(config)) == ['api']
    assert get_project_job_names(config, {
        'api': _ComposeTestJob(['start', 'api', '-t', 'alpha']),
        'foreign': _ComposeTestJob(['start', 'api', '-t', 'beta']),
    }) == ['api']

    custom_config = {
        'project_name': 'alpha',
        'jobs': {'api': 'start api --name shared'},
    }
    assert get_project_job_names(custom_config, {
        'shared': _ComposeTestJob(['start', 'api', '--name', 'shared', '-t', 'beta']),
    }) == []


def test_compose_down_deletes_only_exact_owned_job_names(monkeypatch):
    """Compose down never expands into the unfiltered `delete jobs` command."""
    import meerschaum.compose.subactions.down as down_module
    import meerschaum.compose.utils as compose_utils
    import meerschaum.jobs as jobs_module

    commands = []
    monkeypatch.setattr(jobs_module, 'get_jobs', lambda **kwargs: {
        'api': _ComposeTestJob(['start', 'api', '-t', 'alpha']),
        'foreign': _ComposeTestJob(['start', 'api', '-t', 'beta']),
    })
    monkeypatch.setattr(
        compose_utils,
        'run_mrsm_command',
        lambda command, *args, **kwargs: (commands.append(command) or (True, "Success")),
    )

    assert down_module._compose_down({
        'project_name': 'alpha',
        'jobs': {'api': 'start api'},
    })[0]
    assert commands == [['delete', 'job', 'api', '-f']]


def test_compose_down_untags_shared_pipes_and_deletes_only_unshared(monkeypatch):
    """Additional tags are treated as possible project ownership and preserve pipe data."""
    import meerschaum.compose.subactions.down as down_module
    import meerschaum.compose.utils as compose_utils
    import meerschaum.compose.utils.pipes as pipes_module
    import meerschaum.jobs as jobs_module

    shared_pipe = _ComposeTestPipe(['alpha', 'beta'])
    owned_pipe = _ComposeTestPipe(['alpha'])
    pipes = [shared_pipe, owned_pipe]
    monkeypatch.setattr(jobs_module, 'get_jobs', lambda **kwargs: {})
    monkeypatch.setattr(compose_utils, 'run_mrsm_command', lambda *args, **kwargs: (True, "Success"))
    monkeypatch.setattr(pipes_module, 'get_defined_pipes', lambda *args, **kwargs: pipes)
    monkeypatch.setattr(pipes_module, 'build_custom_connectors', lambda *args, **kwargs: {})
    monkeypatch.setattr(down_module, 'print_options', lambda *args, **kwargs: None)
    monkeypatch.setattr(down_module, 'yes_no', lambda *args, **kwargs: True)

    success, _ = down_module._compose_down(
        {'project_name': 'alpha', 'jobs': {'api': 'start api'}},
        drop=True,
        force=True,
    )

    assert success
    assert shared_pipe.tags == ['beta']
    assert shared_pipe.edits == 1 and shared_pipe.deletes == 0
    assert owned_pipe.edits == 0 and owned_pipe.deletes == 1


def test_compose_down_propagates_delete_failures(monkeypatch):
    """A failed exact job deletion stops the destructive workflow."""
    import meerschaum.compose.subactions.down as down_module
    import meerschaum.compose.utils as compose_utils
    import meerschaum.jobs as jobs_module

    monkeypatch.setattr(jobs_module, 'get_jobs', lambda **kwargs: {
        'api': _ComposeTestJob(['start', 'api', '-t', 'alpha']),
    })
    monkeypatch.setattr(
        compose_utils,
        'run_mrsm_command',
        lambda *args, **kwargs: (False, "delete failed"),
    )
    success, msg = down_module._compose_down({
        'project_name': 'alpha',
        'jobs': {'api': 'start api'},
    })

    assert not success
    assert 'delete failed' in msg


def test_compose_up_propagates_job_start_failures(monkeypatch):
    """Compose refuses foreign collisions and propagates background-job failures."""
    import meerschaum as mrsm
    import meerschaum.compose.subactions.up as up_module
    import meerschaum.compose.utils as compose_utils
    import meerschaum.compose.utils.config as config_module
    import meerschaum.compose.utils.jobs as compose_jobs
    import meerschaum.compose.utils.pipes as pipes_module
    import meerschaum.compose.utils.plugins as plugins_module
    import meerschaum.jobs as jobs_module

    monkeypatch.setattr(plugins_module, 'check_and_install_plugins', lambda *args, **kwargs: (True, "Success"))
    monkeypatch.setattr(pipes_module, 'build_custom_connectors', lambda *args, **kwargs: {})
    monkeypatch.setattr(pipes_module, 'get_defined_pipes', lambda *args, **kwargs: [])
    monkeypatch.setattr(config_module, 'config_has_changed', lambda *args, **kwargs: False)
    monkeypatch.setattr(compose_jobs, 'get_jobs_commands', lambda *args, **kwargs: {
        'api': ['start', 'api', '--name', 'api', '-d'],
    })
    monkeypatch.setattr(compose_jobs, 'get_project_job_names', lambda *args, **kwargs: [])
    existing_jobs = {
        'api': _ComposeTestJob(['start', 'api', '--name', 'api', '-t', 'beta']),
    }
    monkeypatch.setattr(jobs_module, 'get_jobs', lambda **kwargs: existing_jobs)
    monkeypatch.setattr(mrsm, 'get_pipes', lambda *args, **kwargs: [])
    monkeypatch.setattr(compose_utils, 'run_mrsm_command', lambda *args, **kwargs: (False, "start failed"))

    success, msg = up_module._compose_up({'project_name': 'alpha'})
    assert not success
    assert 'not owned' in msg

    existing_jobs.clear()
    success, msg = up_module._compose_up({'project_name': 'alpha'})

    assert not success
    assert 'start failed' in msg


def test_compose_programmatic_plugin_import_resolves_to_core():
    """Legacy programmatic imports survive uninstalling the Compose plugin."""
    from meerschaum.plugins import from_plugin_import

    get_defined_pipes = from_plugin_import('compose.utils.pipes', 'get_defined_pipes')
    legacy_sync = from_plugin_import('compose.sync', 'sync')
    legacy_compose, legacy_completer = from_plugin_import(
        'compose', 'compose', 'complete_compose',
    )
    legacy_up = from_plugin_import('compose.subactions', '_compose_up')
    assert get_defined_pipes.__module__ == 'meerschaum.compose.utils.pipes'
    assert legacy_sync.__module__ == 'meerschaum.compose.sync'
    assert legacy_compose.__module__ == 'meerschaum.actions.compose'
    assert legacy_completer.__module__ == 'meerschaum.actions.compose'
    assert legacy_up.func.__module__ == 'meerschaum.compose.subactions'


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

    configs[0]['project_name'] = 'changed'
    assert config_module.config_has_changed(configs[0]) is True

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
    import os
    import pytest
    import meerschaum.compose.subactions as subactions
    import meerschaum.compose.utils as compose_utils
    import meerschaum.compose.utils.config as config_module
    import meerschaum.plugins as plugins
    import meerschaum.config as config

    unloads = []
    loads = []
    plugin_scopes = iter((['host'], ['project']))

    def fail(*args, **kwargs):
        assert os.environ['COMPOSE_LEAK_TEST'] == 'project'
        raise RuntimeError('subaction failed')

    def fake_init(**kwargs):
        os.environ['COMPOSE_LEAK_TEST'] = 'project'
        return {'config': {}}

    monkeypatch.delenv('COMPOSE_LEAK_TEST', raising=False)
    monkeypatch.setattr(subactions, 'get_subactions', lambda: ['fail'])
    monkeypatch.setattr(subactions, '_get_subaction_function', lambda name: fail)
    monkeypatch.setattr(compose_utils, 'init', fake_init)
    monkeypatch.setattr(config_module, 'get_env_dict', lambda compose_config: dict(os.environ))
    monkeypatch.setattr(plugins, 'get_plugins_names', lambda: next(plugin_scopes))
    monkeypatch.setattr(plugins, 'unload_plugins', lambda names, **kwargs: unloads.append(names))
    monkeypatch.setattr(plugins, 'load_plugins', lambda **kwargs: loads.append(True))
    monkeypatch.setattr(config, 'replace_config', lambda value: nullcontext())

    with pytest.raises(RuntimeError, match='subaction failed'):
        subactions._do_subaction('fail')

    assert unloads == [['host'], ['project']]
    assert len(loads) == 2
    assert 'COMPOSE_LEAK_TEST' not in os.environ


def test_compose_restores_environment_and_plugins_when_host_unload_fails(monkeypatch):
    """Setup failures must not leave dotenv values or unloaded host plugins behind."""
    from contextlib import nullcontext
    import os
    import pytest
    import meerschaum.compose.subactions as subactions
    import meerschaum.compose.utils as compose_utils
    import meerschaum.compose.utils.config as config_module
    import meerschaum.plugins as plugins
    import meerschaum.config as config

    loads = []

    def fake_init(**kwargs):
        os.environ['COMPOSE_LEAK_TEST'] = 'project'
        return {'config': {}}

    monkeypatch.delenv('COMPOSE_LEAK_TEST', raising=False)
    monkeypatch.setattr(subactions, 'get_subactions', lambda: ['up'])
    monkeypatch.setattr(subactions, '_get_subaction_function', lambda name: lambda *args, **kw: (True, 'ok'))
    monkeypatch.setattr(compose_utils, 'init', fake_init)
    monkeypatch.setattr(config_module, 'get_env_dict', lambda compose_config: dict(os.environ))
    monkeypatch.setattr(plugins, 'get_plugins_names', lambda: ['host'])
    monkeypatch.setattr(
        plugins,
        'unload_plugins',
        lambda *args, **kwargs: (_ for _ in ()).throw(RuntimeError('unload failed')),
    )
    monkeypatch.setattr(plugins, 'load_plugins', lambda **kwargs: loads.append(True))
    monkeypatch.setattr(config, 'replace_config', lambda value: nullcontext())

    with pytest.raises(RuntimeError, match='unload failed'):
        subactions._do_subaction('up')

    assert 'COMPOSE_LEAK_TEST' not in os.environ
    assert loads == [True]


def test_compose_shell_prompt_shows_the_active_project(monkeypatch):
    """The shell prompt names the Compose project, even under a pre-v4 prompt config."""
    from meerschaum.config._shell import default_shell_config
    from meerschaum.utils.misc import remove_ansi
    from meerschaum._internal.shell.Shell import Shell, shell_attrs

    for charset in ('ascii', 'unicode'):
        assert '{compose}' in default_shell_config[charset]['prompt']

    shell = Shell()
    ### The test root's config predates v4.0.0, so the token must be injected.
    assert '{compose}' in shell_attrs['_prompt']
    assert 'awesome' not in remove_ansi(shell.prompt)

    monkeypatch.setenv(
        'MRSM__COMPOSE_CONFIG',
        json.dumps({'__file__': '/tmp/awesome/mrsm-compose.yaml'}),
    )
    shell.update_prompt()
    assert 'awesome |' in remove_ansi(shell.prompt)

    monkeypatch.delenv('MRSM__COMPOSE_CONFIG')
    shell.update_prompt()
    assert 'awesome' not in remove_ansi(shell.prompt)
