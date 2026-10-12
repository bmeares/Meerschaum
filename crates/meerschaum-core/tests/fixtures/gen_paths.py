#! /usr/bin/env python3
# vim:fenc=utf-8

"""
Generate `paths.json`: every name in `meerschaum.config._paths.paths` (plus `PLUGINS_DIR_PATHS`
and `DOT_CONFIG_DIR_PATH`) for several environment setups.

Run from the repository root:

    PYTHONPATH=$(pwd) python crates/meerschaum-core/tests/fixtures/gen_paths.py

Temporary paths are replaced with `{TMP}`, and the package directory with `{PACKAGE}`.
"""

import json
import os
import pathlib
import subprocess
import sys
import tempfile

HERE = pathlib.Path(__file__).parent

CASES = {
    'root_only': {},
    'overrides': {
        'MRSM_CONFIG_DIR': '{TMP}/cfg',
        'MRSM_PLUGINS_DIR': '["{TMP}/p1", "{TMP}/p2", "{TMP}/p1", "{TMP}/p1/../p2"]',
        'MRSM_VENVS_DIR': '{TMP}/venvs-new',
        'XDG_CONFIG_HOME': '{TMP}/xdg',
    },
    'colon_plugins': {
        'MRSM_PLUGINS_DIR': '{TMP}/a::{TMP}/b:{TMP}/a',
    },
}


def child(out_path):
    import meerschaum.config._paths as _paths
    import meerschaum.config.paths as paths
    out = {}
    for name in _paths.paths:
        value = getattr(paths, name)
        out[name] = value if isinstance(value, str) else value.as_posix()
    out['PLUGINS_DIR_PATHS'] = [p.as_posix() for p in paths.PLUGINS_DIR_PATHS]
    out['DOT_CONFIG_DIR_PATH'] = paths.DOT_CONFIG_DIR_PATH.as_posix()
    with open(out_path, 'w', encoding='utf-8') as f:
        json.dump(out, f)


def main():
    results = {}
    for case, extra in CASES.items():
        with tempfile.TemporaryDirectory() as tmp:
            tmp = str(pathlib.Path(tmp).resolve())
            root = os.path.join(tmp, 'root')
            os.makedirs(os.path.join(tmp, 'cfg'))
            os.makedirs(root)
            env = {k: v for k, v in os.environ.items() if not k.startswith('MRSM_')}
            env.pop('XDG_CONFIG_HOME', None)
            env['MRSM_CORE'] = 'python'
            env['HOME'] = tmp
            env['MRSM_ROOT_DIR'] = root
            # Never load real plugins: an empty temporary plugins directory by default.
            os.makedirs(os.path.join(tmp, 'plugins'))
            env['MRSM_PLUGINS_DIR'] = os.path.join(tmp, 'plugins')
            for k, v in extra.items():
                env[k] = v.replace('{TMP}', tmp)
            out_path = os.path.join(tmp, 'out.json')
            subprocess.run(
                [sys.executable, __file__, '--child', out_path],
                env=env, stdin=subprocess.DEVNULL, stdout=subprocess.DEVNULL, check=True,
            )
            with open(out_path, encoding='utf-8') as f:
                text = f.read()
            package = str(pathlib.Path.cwd() / 'meerschaum')
            text = text.replace(package, '{PACKAGE}').replace(tmp, '{TMP}')
            results[case] = {'env': extra, 'paths': json.loads(text)}
    with open(HERE / 'paths.json', 'w', encoding='utf-8') as f:
        json.dump(results, f, indent=2)
        f.write('\n')


if __name__ == '__main__':
    if '--child' in sys.argv:
        child(sys.argv[2])
    else:
        main()
