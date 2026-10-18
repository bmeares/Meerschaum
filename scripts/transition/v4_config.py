"""Write v4 config. Run with the reference interpreter: `.venv-ref/bin/python -s -E v4_build.py <spec.json>`."""
import json
import os
import pathlib
import sys

HERE = pathlib.Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))
import meerschaum as mrsm  # noqa: E402
assert mrsm.__version__ == '4.1.4', mrsm.__version__
assert 'site-packages' in mrsm.__file__, mrsm.__file__
from meerschaum.config._edit import write_config  # noqa: E402
from meerschaum.core import User, Plugin, Token  # noqa: E402
from flavors import flavor_attrs  # noqa: E402

spec = json.loads(pathlib.Path(sys.argv[1]).read_text())
flavors = spec['flavors']
db_dir = spec['db_dir']

def ok(label, tup):
    print(f'[v4] {label}: {tup}')

# ---- config through v4 ------------------------------------------------------------------------
conns = {f"trn_{f}": flavor_attrs(f, db_dir) for f in flavors}
conns['trn_ref'] = {'flavor': 'sqlite', 'database': f"MRSM{{meerschaum:connectors:sql:trn_{flavors[0]}:database}}"} \
    if flavors[0] == 'sqlite' else {'flavor': 'sqlite', 'database': f"{db_dir}/ref.db"}
conns['trn_second'] = {'flavor': 'sqlite', 'database': f"{db_dir}/second.db"}
import copy  # noqa: E402
d = copy.deepcopy(mrsm.get_config('meerschaum'))
d['connectors']['sql'].update(conns)
d['connectors']['api']['trn_api'] = {'host': 'localhost', 'port': 59981}
ok('write_config', (write_config({'meerschaum': d}), ''))
print(mrsm.get_config('meerschaum', 'connectors', 'sql', 'trn_ref'))

