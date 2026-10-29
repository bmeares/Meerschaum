#! /usr/bin/env python3
# vim:fenc=utf-8

"""
Record an upgraded v4.1.4 root: the config files v4 leaves behind, and what v4's own `mrsm stack`
does to them and to the generated compose file when it runs again.

Run with the reference interpreter (never the working tree):

    scripts/parity/run.sh crates/meerschaum-core/tests/fixtures/gen_v4_root.py

Each case runs in a fresh interpreter against a fresh temporary root, with `docker` hidden from
`PATH` (the `stack` action writes its files, then stops at "Failed to connect to the Docker
engine": no Docker command ever runs) and no inherited `MRSM_*` variables:

1. `stack config` (the real v4 action), then `write_config({'stack': ..., 'meerschaum': ...})`,
   which leaves v4's `meerschaum.yaml`, `stack.yaml` and `system.json` (an upgraded v4 root);
2. the case's edit of `stack.yaml` (`edited`: the user removed `retries` from the `db`
   healthcheck);
3. `before`: the config files and the compose file;
4. `stack config` twice more, in two processes; `after`: the config files and the compose file;
5. the typical `COMMANDS`, one process each; `after_commands`: the same snapshot.

Every occurrence of the temporary root path is replaced with `{ROOT}`. Output:
`fixtures/v4_root.json`.
"""

import json
import os
import pathlib
import subprocess
import sys
import tempfile

HERE = pathlib.Path(__file__).parent
OUT = HERE / 'v4_root.json'

WRITE = r'''
from meerschaum.config import get_config, write_config
write_config({
    'stack': get_config('stack', substitute=False, sync_files=False),
    'meerschaum': get_config('meerschaum', substitute=False),
})
'''

EDIT_RETRIES = r'''
import yaml
import meerschaum.config.paths as paths
path = paths.CONFIG_DIR_PATH / 'stack.yaml'
with open(path, encoding='utf-8') as f:
    text = f.read()
assert text.count('        retries: 5\n') >= 1, text
db_start = text.index('    db:\n')
db_end = text.index('    api:\n')
db = text[db_start:db_end].replace('        retries: 5\n', '', 1)
with open(path, 'w', encoding='utf-8') as f:
    f.write(text[:db_start] + db + text[db_end:])
'''

### v4 derives defaults from the CPU count (`system:cli:max_daemons`, `api:uvicorn:workers`): pin
### it so the fixture does not depend on the machine that generates it.
PIN_CPU_COUNT = (
    'import multiprocessing, os\n'
    'multiprocessing.cpu_count = lambda: 4\n'
    'os.cpu_count = lambda: 4\n'
)

### `mrsm stack config`, exactly as `python -m meerschaum` runs it.
STACK = ['-m', 'meerschaum', 'stack', 'config']

### Typical commands after that (no network: the instance is the root's SQLite `sql:local`);
### `after_commands` records the config files afterwards.
COMMANDS = [
    ['show', 'version'],
    ['show', 'config'],
    ['show', 'connectors'],
    ['show', 'pipes', '-i', 'sql:local'],
    ['stack', 'config'],
]

CASES = {
    'upgraded': [],
    'edited': [EDIT_RETRIES],
}


def run(code, tmp: pathlib.Path, docker_hidden: bool = True, check: bool = False):
    ### `docker` is hidden whenever the `stack` action runs; the other steps get a normal `PATH`
    ### (v4 bootstraps its `mrsm` venv with it to import PyYAML).
    env = {
        'HOME': str(tmp),
        'PATH': '/nonexistent' if docker_hidden else '/usr/bin:/bin',
        'LANG': 'C.UTF-8',
        'MRSM_ROOT_DIR': str(tmp / 'root'),
        'MRSM_PLUGINS_DIR': str(tmp / 'plugins'),
        'MRSM_NO_AUTO_INSTALL': '1',
    }
    if isinstance(code, list):
        script = (
            'import runpy, sys\n'
            f'sys.argv = {["meerschaum"] + code[2:]!r}\n'
            "runpy.run_module('meerschaum', run_name='__main__', alter_sys=True)\n"
        )
    else:
        script = code
    proc = subprocess.run(  # noqa: PLW1510
        [sys.executable, '-s', '-E', '-P', '-c', PIN_CPU_COUNT + script],
        env=env, cwd=str(tmp), capture_output=True, text=True, timeout=300,
    )
    if check:
        if proc.returncode != 0:
            raise RuntimeError(' '.join(code) + '\n' + proc.stdout + proc.stderr)
    elif isinstance(code, list):
        ### Without Docker the action fails after writing its files (exit status 1).
        if 'Failed to connect to the Docker engine.' not in proc.stdout + proc.stderr:
            raise RuntimeError(proc.stdout + proc.stderr)
    elif proc.returncode != 0:
        raise RuntimeError(proc.stdout + proc.stderr)


def snapshot(tmp: pathlib.Path):
    root = tmp / 'root'
    config_dir = root / 'config'
    files = {}
    for name in sorted(os.listdir(config_dir)):
        path = config_dir / name
        if path.is_file() and name != '.mrsm.lock':
            files[name] = path.read_text(encoding='utf-8').replace(str(root), '{ROOT}')
    compose = (root / 'stack' / 'docker-compose.yaml').read_text(encoding='utf-8')
    return {'config_files': files, 'compose': compose.replace(str(root), '{ROOT}')}


def main():
    import meerschaum
    reference = os.environ.get('MRSM_PARITY_REFERENCE')
    assert reference and meerschaum.__version__ == reference, meerschaum.__version__
    cases = []
    for name, edits in CASES.items():
        cache = pathlib.Path(os.environ.get('XDG_CACHE_HOME') or pathlib.Path.home() / '.cache')
        cache.mkdir(parents=True, exist_ok=True)
        with tempfile.TemporaryDirectory(dir=cache, prefix='mrsm-gen-v4-root-') as tmp_text:
            tmp = pathlib.Path(tmp_text).resolve()
            (tmp / 'root').mkdir()
            (tmp / 'plugins').mkdir()
            run(STACK, tmp)
            run(WRITE, tmp, docker_hidden=False)
            for edit in edits:
                run(edit, tmp, docker_hidden=False)
            before = snapshot(tmp)
            run(STACK, tmp)
            run(STACK, tmp)
            after = snapshot(tmp)
            for command in COMMANDS:
                run(['-m', 'meerschaum'] + command, tmp, docker_hidden=command[0] == 'stack', check=command[0] != 'stack')
            after_commands = snapshot(tmp)
            cases.append({
                'name': name,
                'before': before,
                'after': after,
                'commands': [' '.join(c) for c in COMMANDS],
                'after_commands': after_commands,
            })
    with open(OUT, 'w', encoding='utf-8') as f:
        json.dump({'version': meerschaum.__version__, 'cases': cases}, f, indent=1)
        f.write('\n')
    print(f'Wrote {OUT}')


if __name__ == '__main__':
    main()
