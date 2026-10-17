#! /usr/bin/env python3

"""Focused parity tests for the built-in Compose action."""

import json
import time
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
    def __init__(self, sysargs, status='stopped', restart=False):
        self.sysargs = sysargs
        self.status = status
        self.restart = restart


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


def test_compose_owns_jobs_which_declare_their_own_tags():
    """A job command matching the project's own command is owned, tags notwithstanding."""
    from meerschaum.compose.utils.jobs import get_jobs_commands, get_project_job_names

    ### `-t recompute` selects the pipes to sync, so Compose never appends `-t alpha`
    ### and the tag can't prove ownership.
    config = {
        'project_name': 'alpha',
        'jobs': {'recompute': 'sync pipes -c sql:alpha -t recompute --loop'},
    }
    command = get_jobs_commands(config)['recompute']
    assert '-t' in command and 'alpha' not in command

    running_sysargs = [arg for arg in command if arg != '-d']
    assert get_project_job_names(config, {
        'recompute': _ComposeTestJob(running_sysargs),
    }) == ['recompute']

    ### A different workload under the same name is still foreign.
    assert get_project_job_names(config, {
        'recompute': _ComposeTestJob(
            ['sync', 'pipes', '-c', 'sql:beta', '-t', 'recompute', '--loop', '--name', 'recompute', '-f']
        ),
    }) == []

    ### `--no-daemon` (added under `isolation: subprocess`) doesn't identify the workload.
    assert get_project_job_names(config, {
        'recompute': _ComposeTestJob(running_sysargs + ['--no-daemon']),
    }) == ['recompute']


def test_compose_owns_jobs_it_stamped_after_a_command_edit():
    """A job stamped with the project name stays owned once its command changes."""
    from meerschaum._internal.static import STATIC_CONFIG
    from meerschaum.compose.utils.jobs import get_project_job_names

    compose_project_var = STATIC_CONFIG['environment']['compose_project']
    config = {
        'project_name': 'alpha',
        'jobs': {'recompute': 'sync pipes -c sql:alpha -t recompute --loop --min-seconds 600'},
    }

    ### The running job predates the edit: neither its tags nor its command match.
    stale_job = _ComposeTestJob(
        ['sync', 'pipes', '-c', 'sql:alpha', '-t', 'recompute', '--loop',
         '--min-seconds', '300', '--name', 'recompute', '-f']
    )
    assert get_project_job_names(config, {'recompute': stale_job}) == []

    stale_job.env = {compose_project_var: 'alpha'}
    assert get_project_job_names(config, {'recompute': stale_job}) == ['recompute']

    ### Another project's stamp is not ownership.
    stale_job.env = {compose_project_var: 'beta'}
    assert get_project_job_names(config, {'recompute': stale_job}) == []


def test_compose_stamps_the_project_name_onto_started_jobs(tmp_path, monkeypatch):
    """A job started inside a Compose environment records its project's name."""
    import uuid
    import meerschaum as mrsm
    from meerschaum._internal.static import STATIC_CONFIG
    from meerschaum.compose.utils.config import get_env_dict

    ### Unique per run: under xdist several workers share one root, and a fixed job name
    ### lets one worker delete (or reuse) another's job.
    suffix = uuid.uuid4().hex[:12]
    project_name = f'alpha-{suffix}'
    compose_project_var = STATIC_CONFIG['environment']['compose_project']
    env = get_env_dict({'project_name': project_name, '__file__': tmp_path / 'mrsm-compose.yaml'})
    assert env[compose_project_var] == project_name

    ### The project's config carries credentials and must not reach the job.
    monkeypatch.setenv(compose_project_var, project_name)
    monkeypatch.setenv('MRSM__COMPOSE_CONFIG', json.dumps({'project_name': project_name}))

    job_name = f'test-compose-stamp-{suffix}'
    success, _ = mrsm.entry(['show', 'version', '-d', '--name', job_name, '-f', '-y'])
    try:
        assert success
        ### In python mode `start` returns once the daemon process is launched; the daemon writes
        ### its `properties.json` (with the `env`) a moment later, once its interpreter is up.
        ### Wait for that file instead of racing the daemon's start-up: a job which is never
        ### stamped still fails after the deadline.
        deadline = time.time() + 60
        while True:
            job = mrsm.Job(job_name)
            if job.env.get(compose_project_var, None) is not None or time.time() > deadline:
                break
            time.sleep(0.1)
        assert job.env.get(compose_project_var, None) == project_name, (
            f"exists={job.exists()}, executor={job.executor_keys!r}, "
            f"properties_path={job.daemon.properties_path}, "
            f"properties={job.daemon.properties}"
        )
        assert 'MRSM__COMPOSE_CONFIG' not in job.env
    finally:
        mrsm.entry(['delete', 'job', job_name, '-f', '-y'])


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
        assert '{executor_keys}' not in default_shell_config[charset]['prompt']

    shell = Shell()
    ### The test root's config predates v4.0.0, so the executor slot must be swapped.
    assert '{compose}' in shell_attrs['_prompt']
    assert '{executor_keys}' not in shell_attrs['_prompt']
    assert 'awesome' not in remove_ansi(shell.prompt)

    monkeypatch.setenv(
        'MRSM__COMPOSE_CONFIG',
        json.dumps({'__file__': '/tmp/awesome/mrsm-compose.yaml'}),
    )
    shell.update_prompt()
    assert '| awesome ' in remove_ansi(shell.prompt)

    monkeypatch.delenv('MRSM__COMPOSE_CONFIG')
    shell.update_prompt()
    assert 'awesome' not in remove_ansi(shell.prompt)


def test_compose_up_starts_jobs_instead_of_syncing_pipes(monkeypatch):
    """Explicitly configured jobs are the workload, so `up` does not sync the pipes."""
    import meerschaum as mrsm
    import meerschaum.compose.subactions.up as up_module
    import meerschaum.compose.utils as compose_utils
    import meerschaum.compose.utils.config as config_module
    import meerschaum.compose.utils.jobs as compose_jobs
    import meerschaum.compose.utils.pipes as pipes_module
    import meerschaum.compose.utils.plugins as plugins_module
    import meerschaum.jobs as jobs_module

    pipe = mrsm.Pipe('plugin:foo', 'bar', instance='sql:local', temporary=True)
    started_commands = []

    monkeypatch.setattr(plugins_module, 'check_and_install_plugins', lambda *args, **kwargs: (True, "Success"))
    monkeypatch.setattr(pipes_module, 'build_custom_connectors', lambda *args, **kwargs: {})
    monkeypatch.setattr(pipes_module, 'get_defined_pipes', lambda *args, **kwargs: [pipe])
    monkeypatch.setattr(config_module, 'config_has_changed', lambda *args, **kwargs: True)
    monkeypatch.setattr(compose_jobs, 'get_jobs_commands', lambda *args, **kwargs: {
        'hello': ['show', 'version', '--name', 'hello', '-d'],
    })
    monkeypatch.setattr(compose_jobs, 'get_project_job_names', lambda *args, **kwargs: [])
    monkeypatch.setattr(jobs_module, 'get_jobs', lambda **kwargs: {})
    monkeypatch.setattr(mrsm, 'get_pipes', lambda *args, **kwargs: [])
    monkeypatch.setattr(
        compose_utils,
        'run_mrsm_command',
        lambda command, *args, **kwargs: (started_commands.append(command), (True, "Success"))[1],
    )

    def _fail_on_sync(*args, **kwargs):
        raise AssertionError("`up` must not sync the pipes when jobs are configured.")

    monkeypatch.setattr(up_module, 'run_initial_syncs', _fail_on_sync)

    compose_config = {
        'project_name': 'alpha',
        'jobs': {'hello': 'show version'},
    }
    success, msg = up_module._compose_up(compose_config)

    assert success, msg
    assert started_commands == [['show', 'version', '--name', 'hello', '-d']]
    assert 'Running 1 background job' in msg

    ### `--presync` still forces a sync pass.
    monkeypatch.setattr(up_module, 'run_initial_syncs', lambda *args, **kwargs: (True, "Success"))
    success, msg = up_module._compose_up(compose_config, presync=True, no_jobs=True)
    assert success, msg


def test_compose_owns_a_compound_pipeline_job():
    """A compound job command is adopted despite being stored as `start pipeline`."""
    from meerschaum.compose.utils.jobs import get_jobs_commands, get_project_job_names

    config = {
        'project_name': 'maps',
        'jobs': {
            'maps': (
                'sync pipes -c plugin:osm -t maps '
                '+ sync pipes -c sql:sra -m segments -t maps '
                ': --loop --min-seconds 604800 -w 1'
            ),
        },
    }
    command = get_jobs_commands(config)['maps']
    assert '-d' in command and command[-1] == '-f'

    ### What the daemon actually records: Meerschaum rewrites the compound command
    ### into a `start pipeline` invocation carrying the sub-command as a parameter.
    running_sysargs = [
        'start', 'pipeline', '--loop', '--min-seconds', '604800', '-w', '1',
        '--name', 'maps', '-f',
        '-P',
        json.dumps(
            {
                'sub_args_line': (
                    'sync pipes -c plugin:osm -t maps '
                    '+ sync pipes -c sql:sra -m segments -t maps'
                ),
                'patch_args': None,
            },
            separators=(',', ':'),
        ),
    ]
    assert get_project_job_names(config, {
        'maps': _ComposeTestJob(running_sysargs),
    }) == ['maps']

    ### A pipeline running a different sub-command is still foreign.
    foreign_sysargs = list(running_sysargs)
    foreign_sysargs[-1] = json.dumps(
        {'sub_args_line': 'sync pipes -c sql:other -t maps', 'patch_args': None},
        separators=(',', ':'),
    )
    assert get_project_job_names(config, {
        'maps': _ComposeTestJob(foreign_sysargs),
    }) == []


def test_compose_init_provisions_plugins_on_first_run(tmp_path, monkeypatch):
    """
    A fresh project (no plugins symlinked yet) must still run the
    `install required` / `setup plugins` pass on the FIRST `compose init`.
    """
    from meerschaum.compose import utils
    import meerschaum.compose.utils.config as config_module
    import meerschaum.compose.utils.plugins as plugins_module
    import meerschaum.compose.utils.stack as stack_module
    from meerschaum.compose.subactions.init import _compose_init

    compose_path = tmp_path / 'mrsm-compose.yaml'
    compose_path.touch()
    compose_config = {'root_dir': tmp_path / 'root', 'project_name': 'fresh-project'}

    commands = []
    discovery_calls = []

    def fake_get_installed_plugins(*args, **kwargs):
        ### Empty before `check_and_install_plugins()` symlinks the project
        ### plugins, non-empty afterwards — the fresh-worktree sequence.
        discovery_calls.append(True)
        return [] if len(discovery_calls) == 1 else ['proj_plugin']

    monkeypatch.setattr(config_module, 'infer_compose_file_path', lambda file=None: compose_path)
    monkeypatch.setattr(config_module, 'get_env_dict', lambda *args, **kwargs: {})
    monkeypatch.setattr(utils, 'init', lambda *args, **kwargs: compose_config)
    monkeypatch.setattr(stack_module, 'get_project_name', lambda *args, **kwargs: 'fresh-project')
    monkeypatch.setattr(plugins_module, 'get_installed_plugins', fake_get_installed_plugins)
    monkeypatch.setattr(
        plugins_module,
        'check_and_install_plugins',
        lambda *args, **kwargs: (True, "Success"),
    )
    monkeypatch.setattr(
        utils,
        'run_mrsm_command',
        lambda args, *a, **kw: commands.append(list(args)) or (True, "Success"),
    )

    success, msg = _compose_init(None, file=compose_path, yes=True)
    assert success, msg
    assert ['install', 'required'] in commands
    assert ['setup', 'plugins'] in commands


def _run_compose_default(monkeypatch, action, jobs, sysargs=None, **kw):
    """Run the default Compose subaction with fake jobs, returning the commands it ran."""
    import meerschaum.compose.subactions.default as default_module
    import meerschaum.compose.utils as compose_utils
    import meerschaum.jobs as jobs_module
    import meerschaum.utils.formatting._jobs as jobs_formatting

    commands = []
    monkeypatch.setattr(jobs_module, 'get_jobs', lambda **kwargs: jobs)
    monkeypatch.setattr(jobs_formatting, 'pprint_jobs', lambda *args, **kwargs: None)
    monkeypatch.setattr(
        compose_utils,
        'run_mrsm_command',
        lambda command, *args, **kwargs: (commands.append(command) or (True, "Success")),
    )
    result = default_module._compose_default(
        {'project_name': 'alpha', 'jobs': {'api': 'start api', 'worker': 'sync pipes --loop'}},
        action=action,
        sysargs=(sysargs or (['compose'] + action + ['--file', 'alpha.yaml', '-y'])),
        **{'yes': True, **kw}
    )
    return result, commands


def _stamped_job(sysargs, project_name, **kw):
    from meerschaum._internal.static import STATIC_CONFIG
    job = _ComposeTestJob(sysargs, **kw)
    job.env = {STATIC_CONFIG['environment']['compose_project']: project_name}
    return job


def test_compose_job_actions_without_names_are_scoped_to_the_project(monkeypatch):
    """`compose start jobs` must not start other projects' jobs sharing the root."""
    jobs = {
        'api': _stamped_job(['start', 'api'], 'alpha', status='running', restart=True),
        'worker': _ComposeTestJob(['sync', 'pipes', '--loop', '-t', 'alpha', '--name', 'worker', '-f']),
        'sixfab': _stamped_job(['sync', 'pipes', '-c', 'sixfab:sra'], 'beta'),
        'manual': _ComposeTestJob(['show', 'version']),
    }
    ### Each bare command expands to the project jobs it would have acted on:
    ### `start` never restarts the running `api`, and `restart` only checks `--restart` jobs.
    expected = {
        'start': ['worker'],
        'stop': ['api'],
        'pause': ['api'],
        'restart': ['api'],
        'delete': ['api', 'worker'],
    }
    for verb, job_names in expected.items():
        (success, _), commands = _run_compose_default(monkeypatch, [verb, 'jobs'], jobs)
        assert success
        assert commands == [[verb, 'jobs'] + job_names + ['-y']]


def test_compose_start_jobs_leaves_running_jobs_alone(monkeypatch):
    """A bare `compose start jobs -y` must not restart the project's running jobs."""
    jobs = {
        'api': _stamped_job(['start', 'api'], 'alpha', status='running'),
        'worker': _ComposeTestJob(
            ['sync', 'pipes', '--loop', '-t', 'alpha', '--name', 'worker', '-f'],
            status='running',
        ),
    }
    (success, msg), commands = _run_compose_default(monkeypatch, ['start', 'jobs'], jobs)
    assert success
    assert msg == "All jobs are running."
    assert commands == []


def test_compose_bare_stop_jobs_still_asks_for_confirmation(monkeypatch):
    """Expanding `stop jobs` into names must keep the bare command's confirmation."""
    import meerschaum.utils.prompt as prompt_module

    questions = []
    monkeypatch.setattr(
        prompt_module,
        'yes_no',
        lambda question, *args, **kwargs: (questions.append(question) or False),
    )
    jobs = {'api': _stamped_job(['start', 'api'], 'alpha', status='running')}
    (success, _), commands = _run_compose_default(
        monkeypatch, ['stop', 'jobs'], jobs,
        sysargs=['compose', 'stop', 'jobs'], yes=False,
    )
    assert not success
    assert questions == ['Stop the above jobs?']
    assert commands == []


def test_compose_job_actions_refuse_other_projects_jobs(monkeypatch):
    """Naming another project's job must not run it with this project's config."""
    jobs = {
        'api': _stamped_job(['start', 'api'], 'alpha'),
        'sixfab': _stamped_job(['sync', 'pipes', '-c', 'sixfab:sra'], 'beta'),
        'manual': _ComposeTestJob(['show', 'version']),
    }
    (success, msg), commands = _run_compose_default(monkeypatch, ['start', 'jobs', 'api', 'sixfab'], jobs)
    assert not success
    assert 'sixfab' in msg and 'api' not in msg.split(':')[-1]
    assert commands == []

    ### This project's jobs and unstamped jobs may still be named explicitly.
    (success, _), commands = _run_compose_default(monkeypatch, ['start', 'job', 'api', 'manual'], jobs)
    assert success
    assert commands == [['start', 'job', 'api', 'manual', '-y']]


def test_compose_job_actions_with_no_project_jobs_do_nothing(monkeypatch):
    """With no jobs of its own, a project must not fall through to every job in the root."""
    jobs = {'sixfab': _stamped_job(['sync', 'pipes', '-c', 'sixfab:sra'], 'beta')}
    (success, msg), commands = _run_compose_default(monkeypatch, ['stop', 'jobs'], jobs)
    assert success
    assert 'No jobs' in msg
    assert commands == []


def test_compose_other_actions_pass_through_unscoped(monkeypatch):
    """Read-only job actions and unrelated actions are forwarded unchanged."""
    jobs = {'sixfab': _stamped_job(['sync', 'pipes'], 'beta')}
    for action in (['show', 'jobs'], ['show', 'pipes'], ['sync', 'pipes']):
        (success, _), commands = _run_compose_default(monkeypatch, action, jobs)
        assert success
        assert commands == [action + ['-y']]
