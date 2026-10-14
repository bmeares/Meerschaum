#! /usr/bin/env python3
# vim:fenc=utf-8

"""
Generate the wave-9 compose-instance parity fixture from the released v4.1.4 reference.

    scripts/parity/run.sh scripts/parity/gen_w9_compose_instance.py

Unlike `crates/meerschaum-compose/tests/fixtures/gen_compose.py` (an in-memory fake instance),
this generator runs `compose explain` and `compose up --dry` against a REAL instance connector,
a SQLite database that holds the pipes in `case.json["registered"]`, so the recorded statuses and
parameters are what v4 reads from an actual registry. Only the commands compose would run
(`register pipes`, `show version`) are recorded instead of executed.

Scenarios (projects under `tests/fixtures/w9_compose_instance/projects/`):

- `main`: the pipes registered, then `compose explain` and `compose up --dry` (which edits the
  registrations, as v4 does).
- `nodb`: the same project, but the SQLite file does not exist; records the files compose
  created (v4's explain creates the database).
- `nodb_up`: `up --dry` on a database that does not exist.
- `envlayer`: the same project, with an `MRSM_SQL_W9V` variable in the environment that points at
  another database holding only `alpha`: the connector the file defines wins (v4 builds it with
  `get_connector(keys, **attributes)`, whose keyword attributes override the config's).
- `envonly`: a project without connectors and an `MRSM_SQL_W9V` variable that points at a
  database holding every pipe: the environment connector is the instance.
- `unreachable`: an API instance nobody listens on (v4 raises).

Everything runs in a temporary directory with an isolated root, plugins directory, home and
environment (`MRSM_*` is rebuilt from scratch) and the output is scrubbed of temporary paths.
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
REPO = HERE.parent.parent
FIXTURES = REPO / 'crates' / 'meerschaum-cli' / 'tests' / 'fixtures' / 'w9_compose_instance'
PROJECTS = FIXTURES / 'projects'
OUTPUT = FIXTURES / 'expected.json'

SCENARIOS = [
    {
        'name': 'main',
        'project': 'main',
        'seed': True,
        'runs': [
            {'name': 'explain', 'action': ['explain']},
            {'name': 'up-dry', 'action': ['up'], 'flags': {'dry': True}},
            ### After `up --dry` edited the registrations everything is up to date.
            {'name': 'explain-after', 'action': ['explain']},
        ],
    },
    {
        'name': 'nodb',
        'project': 'main',
        'seed': False,
        'runs': [{'name': 'explain', 'action': ['explain']}],
    },
    {
        'name': 'nodb_up',
        'project': 'main',
        'seed': False,
        'runs': [{'name': 'up-dry', 'action': ['up'], 'flags': {'dry': True}}],
    },
    {
        'name': 'envlayer',
        'project': 'main',
        'seed': True,
        'env_db': 'alpha',
        'runs': [{'name': 'explain', 'action': ['explain']}],
    },
    {
        'name': 'envonly',
        'project': 'envonly',
        'seed': False,
        'env_db': 'all',
        'runs': [{'name': 'explain', 'action': ['explain']}],
    },
    {
        'name': 'unreachable',
        'project': 'unreachable',
        'seed': False,
        'runs': [
            {'name': 'explain', 'action': ['explain']},
            {'name': 'up-dry', 'action': ['up'], 'flags': {'dry': True}},
        ],
    },
]

FLAG_ARGS = {'dry': '--dry'}


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


def child(scenario_name, tmp, out_path, source):
    import faulthandler
    faulthandler.dump_traceback_later(300, exit=True)
    import warnings
    import meerschaum as mrsm
    assert mrsm.__version__ == source.split('=', 1)[1], (mrsm.__version__, source)
    package_dir = os.path.dirname(os.path.realpath(mrsm.__file__))

    import importlib
    import meerschaum.compose.utils as compose_utils
    import meerschaum.compose.utils.config as config_module
    import meerschaum.utils.formatting as formatting
    import meerschaum._internal.entry as entry_module
    import meerschaum.utils.packages as packages_module
    import meerschaum.utils.warnings as warnings_module
    import meerschaum.compose as compose_module
    from meerschaum.actions.compose import compose
    from rich.console import Console

    scenario = next(s for s in SCENARIOS if s['name'] == scenario_name)
    case = json.loads((FIXTURES / 'case.json').read_text(encoding='utf-8'))
    tmp = pathlib.Path(tmp)
    project_dir = tmp / 'proj'
    shutil.copytree(PROJECTS / scenario['project'], project_dir)
    root = project_dir / 'root'
    root.mkdir()

    def seed(database, label, registered):
        conn = mrsm.get_connector('sql', label, flavor='sqlite', database=database)
        for rec in registered:
            pipe = mrsm.Pipe(
                'sql:w9v', rec['metric'], rec.get('location'),
                instance=conn, parameters=copy.deepcopy(rec['parameters']),
            )
            success, msg = pipe.register()
            assert success, msg

    if scenario['seed']:
        seed((root / 'w9v.db').as_posix(), 'w9seed', case['registered'])
    if scenario.get('env_db'):
        ### The database the `MRSM_SQL_W9V` variable points at.
        registered = [
            rec for rec in case['registered']
            if scenario['env_db'] == 'all' or rec['metric'] == scenario['env_db']
        ]
        seed((tmp / 'envdb.sqlite').as_posix(), 'w9envseed', registered)
    ### Building the pipes created connectors for `sql:w9v` from the environment; compose must
    ### build its own, as in a fresh process.
    from meerschaum.connectors import connectors as connectors_registry
    connectors_registry.get('sql', {}).pop('w9v', None)

    state = {'commands': [], 'messages': [], 'pending': None}

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
    import meerschaum.utils.venv as venv_module
    venv_module.init_venv = lambda *args, **kw: True
    entry_module.entry = fake_entry
    packages_module.run_python_package = fake_run_python_package

    def recording_warn(*args, **kw):
        text = str(args[0]) if args else ''
        if text.lstrip().startswith('Missing package '):
            return
        state['messages'].append(['warn', text])

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

    def listing():
        """The database files under the project (cache files and bytecode vary by machine)."""
        return sorted(
            p.relative_to(project_dir).as_posix()
            for p in project_dir.rglob('*')
            if p.is_file() and p.suffix in ('.db', '.sqlite', '.gpkg')
        )

    def registry_snapshot():
        """The registered pipes and parameters read straight from the SQLite file."""
        db = root / 'w9v.db'
        if not db.exists():
            return None
        import sqlite3
        con = sqlite3.connect(db.as_posix())
        try:
            rows = con.execute(
                'SELECT connector_keys, metric_key, location_key, parameters FROM mrsm_pipes'
            ).fetchall()
        except Exception:
            return None
        finally:
            con.close()
        out = []
        for ck, mk, lk, params in sorted(rows, key=lambda r: (r[0], r[1], str(r[2]))):
            params = json.loads(params) if isinstance(params, str) else params
            out.append({'connector_keys': ck, 'metric_key': mk, 'location_key': lk, 'parameters': params})
        return out

    results = {'scenario': scenario_name, 'files_before': listing(), 'runs': []}
    for run in scenario['runs']:
        sys.__stderr__.write(f"  {scenario_name}: {run['name']}\n")
        state['commands'], state['messages'], state['pending'] = [], [], None
        config_module.CONFIG_METADATA.clear()
        compose_module._warned_about_plugin = False

        buffer = io.StringIO()
        formatting.console = Console(
            file=buffer, width=100, color_system=None, force_terminal=False,
            legacy_windows=False, highlight=False,
        )
        flags = dict(run.get('flags') or {})
        sysargs = ['compose'] + list(run['action']) + [FLAG_ARGS[f] for f, v in flags.items() if v]

        old_cwd = os.getcwd()
        old_environ = dict(os.environ)
        os.chdir(project_dir)
        out_buf, err_buf = io.StringIO(), io.StringIO()
        record = {'name': run['name'], 'sysargs': sysargs}
        with warnings.catch_warnings(record=True) as caught:
            warnings.simplefilter('always')
            saved_stdout, saved_stderr = sys.stdout, sys.stderr
            sys.stdout, sys.stderr = out_buf, err_buf
            try:
                result = compose(action=list(run['action']), sysargs=sysargs, **flags)
                record['result'] = [bool(result[0]), str(result[1])]
            except BaseException as e:
                record['error'] = {'type': type(e).__name__, 'message': str(e)}
                sys.__stderr__.write(traceback.format_exc())
            finally:
                sys.stdout, sys.stderr = saved_stdout, saved_stderr
                os.chdir(old_cwd)
                os.environ.clear()
                os.environ.update(old_environ)

        from meerschaum.utils.misc import remove_ansi
        record.update({
            'commands': state['commands'],
            'messages': state['messages'],
            'warnings': [
                remove_ansi(str(w.message)) for w in caught
                if 'Meerschaum is installing' not in str(w.message)
            ],
            'explain': buffer.getvalue() if run['action'][:1] == ['explain'] else None,
            'registry': registry_snapshot(),
            'files': listing(),
        })
        replacements = [
            (project_dir.as_posix(), '{PROJECT}'),
            (str(tmp), '{TMP}'),
            (package_dir, '{SITE}/meerschaum'),
            (str(REPO), '{REPO}'),
        ]
        results['runs'].append(normalize(record, replacements))

    with open(out_path, 'w', encoding='utf-8') as f:
        json.dump(results, f, default=str)


def generate(scenario_name):
    scenario = next(s for s in SCENARIOS if s['name'] == scenario_name)
    reference = os.environ.get('MRSM_PARITY_REFERENCE')
    if not reference:
        raise SystemExit("Run this generator through scripts/parity/run.sh (released v4.1.4).")
    tmp = pathlib.Path(tempfile.mkdtemp(prefix='mrsm-w9v-')).resolve()
    try:
        for sub in ('host-root', 'host-plugins', 'home', 'venvs'):
            (tmp / sub).mkdir()
        env = {
            'PATH': '/usr/bin:/bin',
            'HOME': (tmp / 'home').as_posix(),
            'LANG': 'C.UTF-8',
            'MRSM_ROOT_DIR': (tmp / 'host-root').as_posix(),
            'MRSM_PLUGINS_DIR': (tmp / 'host-plugins').as_posix(),
            'MRSM_VENVS_DIR': (tmp / 'venvs').as_posix(),
            'MRSM_NO_AUTO_INSTALL': '1',
            'PYTHONHASHSEED': '0',
        }
        if scenario.get('env_db'):
            env['MRSM_SQL_W9V'] = json.dumps(
                {'flavor': 'sqlite', 'database': (tmp / 'envdb.sqlite').as_posix()}
            )
        out_path = tmp / 'out.json'
        proc = subprocess.run(
            [sys.executable, '-s', __file__, '--child', scenario_name, str(tmp), str(out_path), f'--reference={reference}'],
            env=env,
            cwd=str(tmp),
            stdin=subprocess.DEVNULL,
            capture_output=True,
            text=True,
        )
        if proc.returncode != 0:
            sys.stderr.write(proc.stdout + proc.stderr)
            raise RuntimeError(f"Child failed for '{scenario_name}'.")
        return json.loads(out_path.read_text(encoding='utf-8'))
    finally:
        shutil.rmtree(tmp, ignore_errors=True)


def main():
    if len(sys.argv) > 1 and sys.argv[1] == '--child':
        child(sys.argv[2], sys.argv[3], sys.argv[4], sys.argv[5])
        return
    out = {'scenarios': {}}
    for scenario in SCENARIOS:
        out['scenarios'][scenario['name']] = generate(scenario['name'])
    OUTPUT.write_text(json.dumps(out, indent=1, ensure_ascii=False) + '\n', encoding='utf-8')
    print(f"Wrote {OUTPUT.relative_to(REPO)}.")


if __name__ == '__main__':
    main()
