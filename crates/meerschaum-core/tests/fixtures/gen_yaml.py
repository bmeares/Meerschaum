#! /usr/bin/env python3
# vim:fenc=utf-8

"""
Generate the YAML codec parity fixture `yaml_parity.json`.

Run from the repository root with the released reference (never the working tree):

    scripts/parity/run.sh crates/meerschaum-core/tests/fixtures/gen_yaml.py

Three groups of cases, all synthetic, each recorded with released `meerschaum==4.1.4` and the
PyYAML it runs on:

- `load_cases`: YAML text -> `meerschaum.utils.yaml.yaml.load` and `.safe_load`, as a typed JSON
  projection (`{"t": "int", "v": "12"}`, `{"t": "dict", "v": [[key, value], ...]}`, ...) or the
  exception class and message (what `read_config` warns with).
- `dump_cases`: typed data (the same projection) plus `yaml.dump` keywords -> the exact text
  Meerschaum's `CustomDumper` writes. `{"t": "shared", "id": "x", "v": ...}` defines a value that
  later `{"t": "shared", "id": "x"}` references reuse by identity (PyYAML writes anchors).
- `config_cases`: `read_config` / `write_config` on throwaway directories: yaml/yml/json
  preference, header and key order, malformed-file preservation, `write_missing`, filetype
  selection, `with_filenames`, `substitute=False`, `MRSM{}` symlink restoration.

The inputs live in `yaml_corpus.py` (shared with the Rust tests and `tests/test_yaml.py`); this
file only records what the reference does with them.
"""

import json
import os
import pathlib
import sys
import tempfile

HERE = pathlib.Path(__file__).resolve().parent
OUT = HERE / 'yaml_parity.json'

### Isolate before `meerschaum` is imported (a generator never touches a real root). v4 reads every
### `MRSM_<TYPE>_<LABEL>` variable as a connector, `MRSM_PARITY_REFERENCE` included (it would
### become a `parity:reference` connector inside the recorded default config), so drop them all and
### keep the reference version for the guard below.
_TMP = tempfile.mkdtemp(prefix='mrsm-yaml-gen-')
_REFERENCE = os.environ.get('MRSM_PARITY_REFERENCE')
for _k in [k for k in os.environ if k.startswith('MRSM_')]:
    del os.environ[_k]
os.environ['MRSM_ROOT_DIR'] = os.path.join(_TMP, 'root')
os.environ['MRSM_PLUGINS_DIR'] = os.path.join(_TMP, 'plugins')
os.environ['HOME'] = _TMP
os.makedirs(os.environ['MRSM_ROOT_DIR'], exist_ok=True)
os.makedirs(os.environ['MRSM_PLUGINS_DIR'], exist_ok=True)

if not _REFERENCE:
    sys.exit('Run this generator through scripts/parity/run.sh (released meerschaum only).')
import meerschaum
assert meerschaum.__version__ == _REFERENCE, meerschaum.__version__
import yaml as _pyyaml
from meerschaum.utils.yaml import yaml as mrsm_yaml


sys.path.insert(0, str(HERE))
import yaml_corpus as corpus
from yaml_corpus import outcome, untag


def dump_outcome(case):
    data = untag(case['data'])
    try:
        return {'ok': mrsm_yaml.dump(data, **case['kwargs'])}
    except Exception as e:
        return {'error': type(e).__name__, 'message': str(e)}


def main():
    out = {
        'reference': {
            'meerschaum': meerschaum.__version__,
            'pyyaml': _pyyaml.__version__,
        },
        'load_cases': [],
        'dump_cases': [],
        'config_cases': [],
    }
    for name, text in corpus.LOAD_CASES:
        out['load_cases'].append({
            'name': name,
            'text': text,
            'load': outcome(mrsm_yaml.load, text),
            'safe_load': outcome(mrsm_yaml.safe_load, text),
        })
    for case in corpus.DUMP_CASES:
        out['dump_cases'].append({**case, 'expected': dump_outcome(case)})
    from meerschaum.config import paths
    import shutil
    for case in corpus.CONFIG_CASES:
        ### v4's `get_keyfile_path()` ignores an explicit directory and reads the config
        ### directory of the root, so the cases run in the root's own (emptied) config directory.
        directory = pathlib.Path(paths.CONFIG_DIR_PATH)
        shutil.rmtree(directory, ignore_errors=True)
        directory.mkdir(parents=True)
        out['config_cases'].append({**case, 'outcome': corpus.run_config_case(case, directory)})
    with open(OUT, 'w', encoding='utf-8') as f:
        json.dump(out, f, indent=1, ensure_ascii=False)
        f.write('\n')
    print(
        f"Wrote {OUT.name}: {len(out['load_cases'])} load, {len(out['dump_cases'])} dump, "
        f"{len(out['config_cases'])} config cases",
        file=sys.stderr,
    )


if __name__ == '__main__':
    main()
