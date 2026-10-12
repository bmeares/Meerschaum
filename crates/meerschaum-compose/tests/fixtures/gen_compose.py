#! /usr/bin/env python3
# vim:fenc=utf-8

"""
Generate the `meerschaum-compose` parity fixtures from the Python compose implementation.

Run from the repository root with the `mrsm` venv interpreter (it has `rich`, `python-dotenv`,
`PyYAML` and `envyaml`):

    PYTHONPATH=$(pwd) ~/.config/meerschaum/venvs/mrsm/bin/python \
        crates/meerschaum-compose/tests/fixtures/gen_compose.py [project ...]

For every synthetic project under `projects/<name>/` (a compose file, optional `.env` files,
plugins, and a `case.json` describing the runs), this script:

1. creates a temporary base directory `{TMP}` with an empty host root (`MRSM_ROOT_DIR`), an empty
   host plugins directory (`MRSM_PLUGINS_DIR`), a home directory, and a venvs directory holding a
   copy of the `compose` venv (for `envyaml`) and a link to the `mrsm` venv;
2. starts one child interpreter per project with a minimal, controlled environment
   (`env -i`-style) so that nothing from the real environment or the real root directory leaks in;
3. in the child, runs each subaction through the real entrypoint
   (`meerschaum.actions.compose.compose`) on a fresh copy of the project
   (`{TMP}/runs/<n>/<project>`), with these fakes:
   - `run_mrsm_command` executes nothing: the command it would run (final sysargs, in-process or
     subprocess, captured output, replaced environment) is recorded;
   - the instance connector is an in-memory fake seeded from `case.json["registered"]`
     (`mrsm.get_pipes`, `Pipe.attributes`, `Pipe.id`, `Pipe.edit`, `Pipe.delete`);
   - `meerschaum.jobs.get_jobs` returns the fake jobs from `case.json["jobs"]`;
   - the rich console renders without color at the case's width;
4. writes `expected/<name>.json` with, per run: the result tuple (or the raised error), the
   recorded commands and instance events, the warnings, the resolved compose config, the
   project environment (`get_env_dict`), the defined pipes (metadata and constructed pipes),
   the config hash, and the `explain` text.

Paths are normalized: the project copy becomes `{PROJECT}` (or `{RUN}` for the run directory),
the temporary base `{TMP}`, the repository `{REPO}`.
"""

import copy
import io
import json
import os
import pathlib
import shutil
import subprocess
import sys
import tempfile
import traceback

HERE = pathlib.Path(__file__).resolve().parent
PROJECTS = HERE / 'projects'
EXPECTED = HERE / 'expected'
REPO = HERE.parent.parent.parent.parent
COMPOSE_VENV = REPO / 'test_root' / 'venvs' / 'compose'
MRSM_VENV = pathlib.Path.home() / '.config' / 'meerschaum' / 'venvs' / 'mrsm'

ALL_RUNS = [
    {'name': 'explain', 'action': ['explain']},
    {'name': 'up-dry', 'action': ['up'], 'flags': {'dry': True}},
    {'name': 'up', 'action': ['up']},
    {'name': 'up-follow', 'action': ['up'], 'flags': {'force': True}},
    {'name': 'up-presync-no-jobs', 'action': ['up'], 'flags': {'presync': True, 'no_jobs': True}},
    {'name': 'run', 'action': ['run']},
    {'name': 'down', 'action': ['down']},
    {'name': 'down-volumes', 'action': ['down'], 'flags': {'drop': True, 'yes': True}},
    {'name': 'ps', 'action': ['ps']},
    {'name': 'logs', 'action': ['logs']},
    {'name': 'sync-pipes', 'action': ['sync', 'pipes']},
    {'name': 'start-jobs', 'action': ['start', 'jobs'], 'flags': {'yes': True}},
    {'name': 'stop-jobs', 'action': ['stop', 'jobs'], 'flags': {'yes': True}},
    {'name': 'delete-jobs', 'action': ['delete', 'jobs'], 'flags': {'yes': True}},
    {'name': 'start-api', 'action': ['start', 'api']},
    {'name': 'shell', 'action': []},
    {'name': 'init', 'action': ['init']},
]

FLAG_ARGS = {
    'dry': '--dry',
    'force': '-f',
    'presync': '--presync',
    'no_jobs': '--no-jobs',
    'isolated': '--isolated',
    'drop': '-v',
    'yes': '-y',
    'noask': '--noask',
    'nopretty': '--nopretty',
    'debug': '--debug',
}


def build_sysargs(run):
    sysargs = ['compose'] + list(run['action'])
    for flag, value in (run.get('flags') or {}).items():
        if value:
            sysargs.append(FLAG_ARGS[flag])
    if run.get('file'):
        sysargs += ['--file', run['file']]
    if run.get('env_file'):
        sysargs += ['--env-file', run['env_file']]
    return sysargs


def normalize(value, replacements):
    if isinstance(value, dict):
        return {str(normalize(k, replacements)): normalize(v, replacements) for k, v in value.items()}
    if isinstance(value, (list, tuple)):
        return [normalize(v, replacements) for v in value]
    if isinstance(value, pathlib.PurePath):
        value = value.as_posix()
    if isinstance(value, str):
        for old, new in replacements:
            value = value.replace(old, new)
        return value
    return value


# ---------------------------------------------------------------------------------------------
# Child process
# ---------------------------------------------------------------------------------------------

def child(project_name, tmp, out_path):
    import faulthandler
    import warnings
    ### Abort with a traceback instead of hanging (e.g. on an unexpected prompt or connection).
    faulthandler.dump_traceback_later(300, exit=True)
    import meerschaum as mrsm
    assert os.path.realpath(mrsm.__file__).startswith(str(REPO)), mrsm.__file__

    from meerschaum.config._patch import apply_patch_to_config
    import meerschaum.compose.utils as compose_utils
    import meerschaum.compose.utils.config as config_module
    import meerschaum.compose.utils.pipes as pipes_module
    import meerschaum.utils.formatting as formatting
    import meerschaum.jobs as jobs_module
    import meerschaum._internal.entry as entry_module
    import meerschaum.utils.packages as packages_module
    import meerschaum.compose as compose_module
    from meerschaum.actions.compose import compose
    from meerschaum.core import Pipe
    from rich.console import Console

    project_dir = PROJECTS / project_name
    case = json.loads((project_dir / 'case.json').read_text(encoding='utf-8'))
    runs = ALL_RUNS if case.get('runs', 'all') == 'all' else case['runs']

    state = {}

    def reset_state():
        state.clear()
        state.update({
            'commands': [],
            'events': [],
            'config': None,
            'env': None,
            'pipes_meta': None,
            'pipes': None,
            'pending': None,
            'messages': [],
        })

    ### Fake instance: {(instance, ck, mk, lk): {'id': int, 'parameters': {...}}}
    registry = {}

    def reg_key(pipe):
        return (str(pipe.instance_keys), pipe.connector_keys, pipe.metric_key, pipe.location_key)

    def seed_registry():
        registry.clear()
        for rec in case.get('registered', []):
            key = (rec['instance'], rec['connector'], rec['metric'], rec.get('location', None))
            registry[key] = {'id': rec['id'], 'parameters': copy.deepcopy(rec['parameters'])}

    def fake_get_pipes(*args, tags=None, instance=None, as_list=False, **kw):
        instance_keys = str(instance)
        pipes = []
        for (ik, ck, mk, lk), rec in registry.items():
            if ik != instance_keys:
                continue
            rec_tags = rec['parameters'].get('tags', []) or []
            if tags and not any(tag in rec_tags for tag in tags):
                continue
            pipe = mrsm.Pipe(ck, mk, lk, instance=ik, parameters=copy.deepcopy(rec['parameters']))
            pipes.append(pipe)
        if as_list:
            return pipes
        out = {}
        for pipe in pipes:
            out.setdefault(pipe.connector_keys, {}).setdefault(pipe.metric_key, {})[pipe.location_key] = pipe
        return out

    def fake_attributes(self):
        if self.temporary:
            return self._attributes
        rec = registry.get(reg_key(self))
        if rec is not None and not self.__dict__.get('_fake_loaded', False):
            self._attributes = apply_patch_to_config(
                {'parameters': copy.deepcopy(rec['parameters'])},
                self._attributes,
            )
            self.__dict__['_fake_loaded'] = True
        return self._attributes

    def fake_get_id(self, **kw):
        rec = registry.get(reg_key(self))
        return rec['id'] if rec is not None else None

    def fake_edit(self, patch=False, interactive=False, debug=False, **kw):
        params = copy.deepcopy(self._attributes.get('parameters', {}))
        state['events'].append(['edit', str(self), params])
        rec = registry.get(reg_key(self))
        if rec is not None:
            rec['parameters'] = copy.deepcopy(params)
        return True, "Success"

    def fake_delete(self, drop=True, debug=False, **kw):
        state['events'].append(['delete', str(self)])
        registry.pop(reg_key(self), None)
        return True, "Success"

    class FakeInstanceConnector:
        def __init__(self, keys):
            self.keys = keys

        def __str__(self):
            return self.keys

        def get_pipe_attributes(self, pipe, **kw):
            rec = registry.get(reg_key(pipe))
            return {'parameters': copy.deepcopy(rec['parameters'])} if rec is not None else {}

    mrsm.get_pipes = fake_get_pipes
    Pipe.attributes = property(fake_attributes)
    Pipe.id = property(lambda self: fake_get_id(self))
    Pipe.get_id = fake_get_id
    Pipe.edit = fake_edit
    Pipe.delete = fake_delete
    Pipe.instance_connector = property(lambda self: FakeInstanceConnector(str(self.instance_keys)))

    ### Fake jobs.
    class FakeJob:
        def __init__(self, spec):
            self.sysargs = list(spec.get('sysargs', []))
            self.env = dict(spec.get('env', {}))
            self.status = spec.get('status', 'stopped')
            self.restart = spec.get('restart', False)
            self.hidden = False

    def fake_get_jobs(*args, **kw):
        return {name: FakeJob(spec) for name, spec in (case.get('jobs') or {}).items()}

    jobs_module.get_jobs = fake_get_jobs
    ### `compose stop jobs` prints the jobs before asking; the fake jobs aren't printable.
    import meerschaum.utils.formatting._jobs as jobs_formatting
    jobs_formatting.pprint_jobs = lambda *args, **kw: None

    ### Command recording: wrap `run_mrsm_command` to see its arguments, and replace the leaves
    ### (`entry`, `run_python_package`) so nothing is executed.
    original_run_mrsm_command = compose_utils.run_mrsm_command

    def recording_run_mrsm_command(args, compose_config, capture_output=False, debug=False,
                                   _subprocess=None, _replace=True, **kw):
        state['pending'] = {'capture_output': capture_output, 'replace': _replace}
        return original_run_mrsm_command(
            args, compose_config, capture_output=capture_output, debug=debug,
            _subprocess=_subprocess, _replace=_replace, **kw
        )

    def fake_entry(sysargs, *args, **kw):
        pending = state.pop('pending', None) or {}
        state['commands'].append({
            'sysargs': list(sysargs),
            'subprocess': False,
            'capture_output': pending.get('capture_output', False),
            'replace': pending.get('replace', True),
        })
        state['pending'] = None
        return True, "Success"

    original_run_python_package = packages_module.run_python_package

    def fake_run_python_package(package, sysargs=None, *args, **kw):
        if package != 'meerschaum':
            return original_run_python_package(package, sysargs, *args, **kw)
        pending = state.pop('pending', None) or {}
        state['commands'].append({
            'sysargs': list(sysargs),
            'subprocess': True,
            'capture_output': pending.get('capture_output', False),
            'replace': True,
        })
        state['pending'] = None
        return 0

    compose_utils.run_mrsm_command = recording_run_mrsm_command
    ### Plugin virtual environments are irrelevant to compose and slow to create.
    import meerschaum.utils.venv as venv_module
    venv_module.init_venv = lambda *args, **kw: True
    entry_module.entry = fake_entry
    packages_module.run_python_package = fake_run_python_package

    ### Messages: compose modules bind `warn` / `info` at import time (or import them at call
    ### time from `meerschaum.utils.warnings`), so patch every binding to record them.
    import importlib
    import meerschaum.utils.warnings as warnings_module

    def recording_warn(*args, **kw):
        state['messages'].append(['warn', str(args[0]) if args else ''])

    def recording_info(*args, **kw):
        state['messages'].append(['info', str(args[0]) if args else ''])

    warnings_module.warn = recording_warn
    warnings_module.info = recording_info
    for mod_name in (
        'meerschaum.compose.utils.config',
        'meerschaum.compose.utils.stack',
        'meerschaum.compose.utils.pipes',
        'meerschaum.compose.utils.plugins',
        'meerschaum.compose.subactions.up',
        'meerschaum.compose.subactions.down',
        'meerschaum.compose.subactions.init',
        'meerschaum.compose.subactions.default',
    ):
        mod = importlib.import_module(mod_name)
        if hasattr(mod, 'warn'):
            mod.warn = recording_warn
        if hasattr(mod, 'info'):
            mod.info = recording_info

    ### Recording wrappers.
    original_read = config_module.read_compose_config

    def recording_read(*args, **kw):
        result = original_read(*args, **kw)
        state['config'] = copy.deepcopy(result)
        state['config_hash'] = config_module.hash_config(result)
        return result

    config_module.read_compose_config = recording_read

    original_get_env_dict = config_module.get_env_dict

    def recording_get_env_dict(compose_config):
        result = original_get_env_dict(compose_config)
        if state['env'] is None:
            state['env'] = copy.deepcopy(result)
        return result

    config_module.get_env_dict = recording_get_env_dict

    original_get_defined_pipes = pipes_module.get_defined_pipes

    def recording_get_defined_pipes(compose_config, as_meta=False, cache=True, debug=False):
        meta = original_get_defined_pipes(compose_config, as_meta=True, cache=cache, debug=debug)
        if state['pipes_meta'] is None:
            state['pipes_meta'] = copy.deepcopy(meta)
        result = original_get_defined_pipes(compose_config, as_meta=as_meta, cache=cache, debug=debug)
        if not as_meta and state['pipes'] is None:
            state['pipes'] = [
                {
                    'str': str(pipe),
                    'connector_keys': pipe.connector_keys,
                    'metric_key': pipe.metric_key,
                    'location_key': pipe.location_key,
                    'instance_keys': str(pipe.instance_keys),
                    'temporary': bool(pipe.temporary),
                    'parameters': copy.deepcopy(pipe._attributes.get('parameters', {})),
                }
                for pipe in result
            ]
        return result

    pipes_module.get_defined_pipes = recording_get_defined_pipes

    results = {'case': case, 'base_env': dict(os.environ), 'runs': []}

    for run_ix, run in enumerate(runs):
        sys.__stderr__.write(f"  {project_name}: {run['name']}\n")
        reset_state()
        seed_registry()
        config_module.CONFIG_METADATA.clear()
        compose_module._warned_about_plugin = False
        run_dir = pathlib.Path(tmp) / 'runs' / str(run_ix)
        copy_dir = run_dir / project_name
        shutil.copytree(project_dir, copy_dir, ignore=shutil.ignore_patterns('case.json', '__pycache__'))
        cwd = (copy_dir / run.get('cwd', case.get('cwd', '.'))).resolve()

        width = run.get('width', case.get('width', 100))
        buffer = io.StringIO()
        formatting.console = Console(
            file=buffer,
            width=width,
            color_system=None,
            force_terminal=False,
            legacy_windows=False,
            highlight=False,
        )

        sysargs = build_sysargs(run)
        flags = dict(run.get('flags') or {})
        file = run.get('file', case.get('file', None))
        env_file = run.get('env_file', case.get('env_file', None))

        old_cwd = os.getcwd()
        old_environ = dict(os.environ)
        os.chdir(cwd)
        out_buf, err_buf = io.StringIO(), io.StringIO()
        record = {
            'name': run['name'],
            'action': run['action'],
            'flags': flags,
            'file': file,
            'env_file': env_file,
            'cwd': cwd.as_posix(),
            'sysargs': sysargs,
            'width': width,
        }
        with warnings.catch_warnings(record=True) as caught:
            warnings.simplefilter('always')
            saved_stdout, saved_stderr = sys.stdout, sys.stderr
            sys.stdout, sys.stderr = out_buf, err_buf
            try:
                result = compose(
                    action=list(run['action']),
                    file=(pathlib.Path(file) if file else None),
                    env_file=(pathlib.Path(env_file) if env_file else None),
                    sysargs=sysargs,
                    **flags
                )
                record['result'] = [bool(result[0]), str(result[1])]
            except BaseException as e:
                record['error'] = {'type': type(e).__name__, 'message': str(e)}
                record['traceback'] = traceback.format_exc().splitlines()[-3:]
            finally:
                sys.stdout, sys.stderr = saved_stdout, saved_stderr
                os.chdir(old_cwd)
                os.environ.clear()
                os.environ.update(old_environ)

        from meerschaum.utils.misc import remove_ansi
        warning_texts = []
        for w in caught:
            text = remove_ansi(str(w.message))
            if 'Meerschaum is installing' in text:
                continue
            warning_texts.append(text)

        record.update({
            'commands': state['commands'],
            'events': state['events'],
            'warnings': warning_texts,
            'messages': state['messages'],
            'config': state['config'],
            'config_hash': state.get('config_hash'),
            'env': state['env'],
            'pipes_meta': state['pipes_meta'],
            'pipes': state['pipes'],
            'explain': buffer.getvalue() if run['action'][:1] == ['explain'] else None,
            'written_files': {},
        })
        if run['action'][:1] == ['init']:
            for name in ('mrsm-compose.yaml', 'mrsm-compose.yml'):
                path = copy_dir / name
                if path.exists() and not (project_dir / name).exists():
                    record['written_files'][name] = path.read_text(encoding='utf-8')

        replacements = [
            (copy_dir.as_posix(), '{PROJECT}'),
            (run_dir.as_posix(), '{RUN}'),
            (str(tmp), '{TMP}'),
            (str(REPO), '{REPO}'),
        ]
        ### The hash covers absolute paths, so also record the hash of the normalized config.
        record['config_hash_normalized'] = (
            config_module.hash_config(normalize(state['config'], replacements))
            if state['config'] is not None
            else None
        )
        results['runs'].append(normalize(record, replacements))

    results['base_env'] = normalize(results['base_env'], [(str(tmp), '{TMP}'), (str(REPO), '{REPO}')])
    with open(out_path, 'w', encoding='utf-8') as f:
        json.dump(results, f, default=str)


# ---------------------------------------------------------------------------------------------
# Parent process
# ---------------------------------------------------------------------------------------------

def generate(project_name):
    project_dir = PROJECTS / project_name
    case = json.loads((project_dir / 'case.json').read_text(encoding='utf-8'))
    tmp = pathlib.Path(tempfile.mkdtemp(prefix='mrsm-compose-')).resolve()
    try:
        for sub in ('host-root', 'host-plugins', 'home', 'venvs', 'runs'):
            (tmp / sub).mkdir()
        for name, text in (case.get('host_plugins') or {}).items():
            (tmp / 'host-plugins' / name).write_text(text, encoding='utf-8')
        shutil.copytree(COMPOSE_VENV, tmp / 'venvs' / 'compose', symlinks=True)
        (tmp / 'venvs' / 'mrsm').symlink_to(MRSM_VENV)
        env = {
            'PATH': '/usr/bin:/bin',
            'HOME': (tmp / 'home').as_posix(),
            'LANG': 'C.UTF-8',
            'MRSM_ROOT_DIR': (tmp / 'host-root').as_posix(),
            'MRSM_PLUGINS_DIR': (tmp / 'host-plugins').as_posix(),
            'MRSM_VENVS_DIR': (tmp / 'venvs').as_posix(),
            'MRSM_NO_AUTO_INSTALL': '1',
            'MRSM_CORE': 'python',
            'PYTHONPATH': str(REPO),
            'PYTHONHASHSEED': '0',
        }
        env.update(case.get('process_env') or {})
        out_path = tmp / 'out.json'
        proc = subprocess.run(
            [str(MRSM_VENV / 'bin' / 'python'), __file__, '--child', project_name, str(tmp), str(out_path)],
            env=env,
            cwd=str(tmp),
            stdin=subprocess.DEVNULL,
            capture_output=True,
            text=True,
        )
        if proc.returncode != 0:
            sys.stderr.write(proc.stdout + proc.stderr)
            raise RuntimeError(f"Child failed for '{project_name}'.")
        data = json.loads(out_path.read_text(encoding='utf-8'))
        EXPECTED.mkdir(exist_ok=True)
        (EXPECTED / f'{project_name}.json').write_text(
            json.dumps(data, indent=1, ensure_ascii=False) + '\n',
            encoding='utf-8',
        )
        print(f"Wrote expected/{project_name}.json ({len(data['runs'])} runs).")
    finally:
        shutil.rmtree(tmp, ignore_errors=True)


def main():
    if len(sys.argv) > 1 and sys.argv[1] == '--child':
        child(sys.argv[2], sys.argv[3], sys.argv[4])
        return
    names = sys.argv[1:] or sorted(p.name for p in PROJECTS.iterdir() if (p / 'case.json').exists())
    for name in names:
        generate(name)


if __name__ == '__main__':
    main()
