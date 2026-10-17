#! /usr/bin/env python3

"""Focused parity tests for the built-in Compose action."""

import json
import time
import pathlib


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
    assert legacy_compose.__module__ == 'meerschaum.compose'
    assert legacy_completer.__module__ == 'meerschaum.compose'
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


def test_compose_owns_a_compound_pipeline_job():
    """A compound job command is adopted despite being stored as `start pipeline`."""
    from meerschaum.compose.utils.jobs import get_jobs_commands, get_project_job_names

    config = {
        'project_name': 'maps',
        'jobs': {
            'maps': (
                'sync pipes -c plugin:osm -t maps '
                '+ sync pipes -c sql:acme -m segments -t maps '
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
                    '+ sync pipes -c sql:acme -m segments -t maps'
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


def test_compose_subactions_delegate_to_the_native_action(monkeypatch):
    """Every subaction is `_core.run_action(['compose', ...])`; the file selects the project."""
    from meerschaum import _core
    from meerschaum.actions import actions
    import meerschaum.compose as compose_module
    import meerschaum.compose.subactions as subactions
    from meerschaum.compose.subactions.up import _compose_up

    calls = []

    def fake_run_action(action, kwargs=None, context=None):
        calls.append((list(action), dict(kwargs or {})))
        return True, "fake"

    monkeypatch.setattr(_core, 'run_action', fake_run_action, raising=False)
    assert subactions.get_subactions() == [
        'default', 'down', 'explain', 'init', 'logs', 'ps', 'run', 'up',
    ]
    assert subactions._do_subaction('up', file='a.yaml') == (True, "fake")
    ### Any other word is the passthrough: `compose show pipes`.
    assert subactions._do_subaction('default', action=['show', 'pipes']) == (True, "fake")
    assert _compose_up({'__file__': pathlib.Path('b.yaml')}, debug=True, no_jobs=True) == (
        True, "fake",
    )
    assert compose_module.compose(['ps'], file='c.yaml') == (True, "fake")
    assert actions['compose'](['down'], drop=True) == (True, "fake")
    assert subactions._compose_logs(file='d.yaml') == (True, "fake")
    assert calls == [
        (['compose', 'up'], {'file': 'a.yaml', 'debug': False}),
        (['compose', 'show', 'pipes'], {'debug': False}),
        (['compose', 'up'], {'file': pathlib.Path('b.yaml'), 'no_jobs': True, 'debug': True}),
        (['compose', 'ps'], {'file': 'c.yaml'}),
        (['compose', 'down'], {'drop': True}),
        (['compose', 'logs'], {'file': 'd.yaml', 'debug': False}),
    ]


def test_compose_runs_natively_from_the_facade(tmp_path, monkeypatch):
    """`explain` reads a real project in the native dispatcher."""
    import meerschaum as mrsm

    project = tmp_path / 'project'
    project.mkdir()
    (project / 'mrsm-compose.yaml').write_text(
        "project_name: w16-facade\nroot_dir: ./root\njobs:\n  api: start api\n",
        encoding='utf-8',
    )
    monkeypatch.chdir(project)
    success, message = mrsm.entry(['compose', 'explain', '--nopretty'])
    assert success, message


def test_compose_dir_paths_and_env_come_from_the_native_helpers(tmp_path, monkeypatch):
    from meerschaum.compose.utils.config import get_dir_paths, get_env_dict

    monkeypatch.delenv('MRSM_PLUGINS_DIR', raising=False)
    config = {
        '__file__': tmp_path / 'mrsm-compose.yaml',
        'root_dir': './root',
        'environment': {'COMPOSE_W16': 'yes'},
    }
    assert get_dir_paths(config, 'root') == [(tmp_path / 'root').resolve()]
    assert get_env_dict(config)['COMPOSE_W16'] == 'yes'
