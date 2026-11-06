#! /usr/bin/env python3
# vim:fenc=utf-8

"""
Generate `v4_pickles.json`: the `pickle.pkl` files Meerschaum 4 writes for its jobs, so that
`meerschaum_jobs::v4_pickle` reads a v4 job's command without unpickling it.

Run from the repository root against the released reference (what CI does; the output must be
byte-identical from run to run):

    MRSM_PLUGINS_DIR=$(mktemp -d) scripts/parity/run.sh \
        crates/meerschaum-jobs/tests/fixtures/gen_v4_pickles.py

Each case builds a `Daemon` the way `meerschaum.jobs.Job` does (target `entry`, `target_args`
`[sysargs]`), writes its pickle without starting it, and records the pickle's bytes (hex) with
the sysargs a reader must find (`null` when the target is not `entry`).
"""

import json
import os
import pathlib
import sys
import tempfile

OUT = pathlib.Path(__file__).parent / 'v4_pickles.json'

CASES = [
    {
        'name': 'api-created',
        'label': '',
        'sysargs': [
            'sync', 'pipes', '-c', 'plugin:example', '-m', 'parking', '-i', 'sql:main',
            '+', 'sync', 'pipes', '-t', 'parking', ':', '-s', 'every 10 minutes',
        ],
    },
    {
        'name': 'labeled',
        'label': None,
        'sysargs': ['sync', 'pipes', '--loop', '--min-seconds', '60', '--name', 'labeled'],
    },
    {
        'name': 'unicode',
        'label': '',
        'sysargs': ['sh', 'echo "héllo wörld ✓"', '--restart'],
    },
]


def main() -> int:
    root = pathlib.Path(tempfile.mkdtemp(prefix='mrsm-v4-pickles-'))
    os.environ['MRSM_ROOT_DIR'] = str(root)
    os.environ.setdefault('MRSM_PLUGINS_DIR', str(root / 'plugins'))
    os.environ['MRSM_NO_AUTO_INSTALL'] = 'true'
    (root / 'plugins').mkdir(exist_ok=True)

    import shlex
    from meerschaum._internal.entry import entry
    from meerschaum.utils.daemon import Daemon

    cases = []
    for case in CASES:
        label = case['label'] if case['label'] is not None else shlex.join(case['sysargs'])
        daemon = Daemon(
            target=entry,
            target_args=[case['sysargs']],
            target_kw={},
            daemon_id=case['name'],
            label=label,
        )
        success, msg = daemon.write_pickle()
        assert success, msg
        cases.append({
            'name': case['name'],
            'sysargs': case['sysargs'],
            'pickle_hex': daemon.pickle_path.read_bytes().hex(),
        })

    daemon = Daemon(
        target=print,
        target_args=['not', 'a', 'job'],
        target_kw={},
        daemon_id='not-entry',
        label='',
    )
    success, msg = daemon.write_pickle()
    assert success, msg
    cases.append({
        'name': 'not-entry',
        'sysargs': None,
        'pickle_hex': daemon.pickle_path.read_bytes().hex(),
    })

    text = json.dumps({'cases': cases}, indent=2, ensure_ascii=False) + '\n'
    assert str(root) not in text
    OUT.write_text(text, encoding='utf-8')
    return 0


if __name__ == '__main__':
    sys.exit(main())
