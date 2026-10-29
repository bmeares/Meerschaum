#! /usr/bin/env python3
# vim:fenc=utf-8

"""
Generate the config-registry parity fixtures.

Run from the repository root:

    PYTHONPATH=$(pwd) python crates/meerschaum-core/tests/fixtures/gen_config.py

For every case under `config_cases/<case>/`:
- `root/` is a synthetic `MRSM_ROOT_DIR` (copied to a temporary directory first, because
  `get_config` writes missing files and deletes patch directories).
- `case.json` lists extra environment variables and the `get_config` queries, run in order in
  one fresh Python process. Optional `setup_queries` initialize a namespace before capture.
- `expected.json` (written here) holds each query's `(valid, value)`, the files in the config
  directory afterwards (with the content of written JSON files), whether the patch directory
  still exists, and the CPU count used for the defaults.

Every occurrence of the temporary root path is replaced with `{ROOT}`.
"""

import json
import os
import pathlib
import shutil
import subprocess
import sys
import tempfile

HERE = pathlib.Path(__file__).parent
CASES = HERE / 'config_cases'


def child_environment(tmp: str, root: pathlib.Path, case_env: dict) -> dict:
    """Isolate each case without losing the harness's pinned dependency environment."""
    reference = {}
    if os.environ.get('MRSM_PARITY_REFERENCE'):
        venvs_dir = os.environ.get('MRSM_VENVS_DIR')
        prefix = pathlib.Path(sys.prefix).resolve()
        if (
            not venvs_dir
            or not (prefix / 'pyvenv.cfg').is_file()
            or (pathlib.Path(venvs_dir) / 'mrsm').resolve() != prefix
        ):
            raise RuntimeError(
                'Config parity requires MRSM_VENVS_DIR/mrsm to point to the active '
                'reference environment; run scripts/parity/run.sh.'
            )
        reference = {
            'MRSM_VENVS_DIR': str(pathlib.Path(venvs_dir).resolve()),
            'MRSM_NO_AUTO_INSTALL': '1',
        }
    env = {
        k: v for k, v in os.environ.items()
        if not k.startswith('MRSM_')
        and k not in ('TMUX', 'TMUX_PANE', 'PYTHONPATH', 'PYTHONHOME', 'VIRTUAL_ENV')
    }
    env.update(MRSM_CORE='python', MRSM_REPO=str(pathlib.Path.cwd()), HOME=tmp)
    env['MRSM_ROOT_DIR'] = str(root)
    plugins_dir = pathlib.Path(tmp) / 'plugins'
    plugins_dir.mkdir()
    env['MRSM_PLUGINS_DIR'] = str(plugins_dir)
    env.update({k: v.replace('{ROOT}', str(root)) for k, v in case_env.items()})
    env.update(reference)
    return env


def child(case_dir: str, root: str, out_path: str):
    import multiprocessing
    import meerschaum
    import meerschaum.config as mc
    import meerschaum.config.paths as paths
    assert meerschaum.__file__.startswith(os.environ['MRSM_REPO']), meerschaum.__file__
    if os.environ.get('MRSM_VENVS_DIR') and any(
        path.suffix in ('.yaml', '.yml') for path in pathlib.Path(root).rglob('*')
    ):
        # v4's first YAML dependency import consults this setting while creating its
        # venv. Reuse that config read without bootstrapping another environment.
        from meerschaum.utils.packages import is_uv_enabled
        is_uv_enabled()
    with open(os.path.join(case_dir, 'case.json'), encoding='utf-8') as f:
        case = json.load(f)
    for query in case.get('setup_queries', []):
        mc.get_config(*query['keys'], **query.get('opts', {}))
    results = []
    for query in case['queries']:
        keys = query['keys']
        opts = query.get('opts', {})
        valid, value = mc.get_config(*keys, as_tuple=True, **opts)
        results.append([valid, value])
    config_dir = paths.CONFIG_DIR_PATH
    files = {}
    for name in sorted(os.listdir(config_dir)):
        path = config_dir / name
        if name.endswith('.json'):
            with open(path, encoding='utf-8') as f:
                files[name] = f.read()
        else:
            files[name] = None
    out = {
        'cpu_count': multiprocessing.cpu_count(),
        'results': results,
        'config_files': files,
        'patch_dir_exists': (pathlib.Path(root) / 'patch_config').exists(),
    }
    text = json.dumps(out, default=str)
    with open(out_path, 'w', encoding='utf-8') as f:
        f.write(text.replace(root, '{ROOT}'))


def main():
    for case_dir in sorted(CASES.iterdir()):
        if not case_dir.is_dir() or not (case_dir / 'case.json').is_file():
            continue
        with open(case_dir / 'case.json', encoding='utf-8') as f:
            case = json.load(f)
        with tempfile.TemporaryDirectory() as tmp:
            root = pathlib.Path(tmp).resolve() / 'root'
            shutil.copytree(case_dir / 'root', root)
            env = child_environment(tmp, root, case['env'])
            out_path = pathlib.Path(tmp) / 'out.json'
            subprocess.run(
                [sys.executable, '-s', '-E', __file__, '--child', str(case_dir), str(root), str(out_path)],
                env=env,
                stdin=subprocess.DEVNULL,
                stdout=subprocess.DEVNULL,
                check=True,
            )
            with open(out_path, encoding='utf-8') as f:
                expected = json.load(f)
        with open(case_dir / 'expected.json', 'w', encoding='utf-8') as f:
            json.dump(expected, f, indent=2, ensure_ascii=False)
            f.write('\n')
        print(f"Wrote {case_dir.name}/expected.json")


if __name__ == '__main__':
    if '--child' in sys.argv:
        child(sys.argv[2], sys.argv[3], sys.argv[4])
    else:
        main()
