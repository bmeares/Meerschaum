#! /usr/bin/env python3
# vim:fenc=utf-8

"""
Generate the parity fixture of v4's default configuration.

Run from the repository root:

    PYTHONPATH=$(pwd) python crates/meerschaum-core/tests/fixtures/gen_default_config.py

Writes `crates/meerschaum-core/tests/fixtures/default_config.json`: v4's
`meerschaum.config._default.default_config` for a known root directory and CPU count.
`config_parity.rs::default_config_matches_python` resolves the Rust defaults for the same root and CPU
count and compares them with it, reading the intentional v5 differences (PARITY.md) as v4's.

The Rust defaults themselves, `crates/meerschaum-core/src/config/default_config.json`, are maintained by
hand: they are where v5 changes a default, and this generator never writes them (it once created them
from v4.1.4). They hold the machine-dependent values as markers, resolved at run time:
- `{"__mrsm_dynamic__": "cpu_count*3"}`: `multiprocessing.cpu_count() * 3`
- `{"__mrsm_dynamic__": "max(cpu_count/2,1)"}`: `max(int(multiprocessing.cpu_count() / 2), 1)`
- `{"__mrsm_dynamic__": "not_windows"}`: `platform.system() != 'Windows'`
- The substring `{__MRSM_ROOT_DIR__}`: the resolved root directory path.
"""

import json
import multiprocessing
import os
import pathlib
import platform
import subprocess
import sys
import tempfile

HERE = pathlib.Path(__file__).parent
FIXTURE_ROOT = '/mrsm-fixture-root'


def normalize(value):
    if isinstance(value, dict):
        return {str(k): normalize(v) for k, v in value.items()}
    if isinstance(value, set):
        return sorted(normalize(v) for v in value)
    if isinstance(value, (list, tuple)):
        return [normalize(v) for v in value]
    return value


def replace_root(value, root, replacement):
    if isinstance(value, dict):
        return {k: replace_root(v, root, replacement) for k, v in value.items()}
    if isinstance(value, list):
        return [replace_root(v, root, replacement) for v in value]
    if isinstance(value, str):
        return value.replace(root, replacement)
    return value


def child():
    import meerschaum
    import meerschaum.config.paths as paths
    from meerschaum.config._default import default_config
    assert meerschaum.__file__.startswith(os.environ['MRSM_REPO']), meerschaum.__file__
    print(json.dumps({
        'root': paths.ROOT_DIR_PATH.as_posix(),
        'cpu_count': multiprocessing.cpu_count(),
        'windows': platform.system() == 'Windows',
        'default_config': normalize(default_config),
    }))


def main():
    with tempfile.TemporaryDirectory() as tmp:
        env = {
            k: v for k, v in os.environ.items()
            if not k.startswith('MRSM_')
        }
        env['MRSM_ROOT_DIR'] = tmp
        # Never load real plugins: point the plugins directory at an empty temp dir.
        plugins_dir = os.path.join(tmp, 'plugins')
        os.makedirs(plugins_dir, exist_ok=True)
        env['MRSM_PLUGINS_DIR'] = plugins_dir
        env['MRSM_CORE'] = 'python'
        env['MRSM_REPO'] = str(pathlib.Path.cwd())
        out = subprocess.check_output(
            [sys.executable, __file__, '--child'], env=env, stdin=subprocess.DEVNULL,
        )
    doc = json.loads(out)
    root = doc['root']
    cpu = doc['cpu_count']
    cfg = doc['default_config']

    fixture = {
        'root': FIXTURE_ROOT,
        'cpu_count': cpu,
        'windows': doc['windows'],
        'default_config': replace_root(cfg, root, FIXTURE_ROOT),
    }
    with open(HERE / 'default_config.json', 'w', encoding='utf-8') as f:
        json.dump(fixture, f, indent=2, ensure_ascii=False)
        f.write('\n')


if __name__ == '__main__':
    if '--child' in sys.argv:
        child()
    else:
        main()
