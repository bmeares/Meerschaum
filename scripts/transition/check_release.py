"""Compare one v4.1.4 SQLite pipe with the same pipe reopened by an installed candidate wheel."""

import json
import os
from pathlib import Path
import subprocess
import sys


def seed():
    import meerschaum as mrsm
    assert mrsm.__version__ == '4.1.4'
    conn = mrsm.get_connector('sql:trn')
    assert Path(conn.value("SELECT file FROM pragma_database_list WHERE name='main'")).resolve() == Path(
        os.environ['MRSM_TRANSITION_DB']
    ).resolve()
    pipe = mrsm.Pipe('sql:trn', 'upgrade', instance=conn, columns={'id': 'id'})
    result = pipe.sync([{'id': 1, 'value': 42}])
    assert result[0], result[1]


def compare():
    from common import HERE, Run, SCRATCH
    from stage_v5 import Findings, snapshots
    (SCRATCH / 'tmp').mkdir(parents=True, exist_ok=True)
    run = Run(SCRATCH / 'work' / 'run-release')
    (run.work / 'spec_pipes.json').write_text(json.dumps({
        'pipes': [{'instance': 'sql:trn', 'ck': 'sql:trn', 'mk': 'upgrade', 'lk': None}],
    }))
    run.py('v4', Path(__file__).resolve(), 'seed', check=True,
           extra_env={'MRSM_TRANSITION_DB': SCRATCH / 'scratch.db'})
    snapshots(run, Findings(run), 'installed')
    subprocess.run([sys.executable, HERE / 'report.py', '--strict', 'run-release'], check=True)


if __name__ == '__main__':
    if sys.argv[1:] == ['seed']:
        seed()
    else:
        compare()
