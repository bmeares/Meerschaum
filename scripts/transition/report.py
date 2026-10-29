#!/usr/bin/env python3
"""Print the findings of the runs under the scratch directory: `report.py [run-name ...]`; exit 1 on any FAIL."""
import json
import pathlib
import sys

sys.path.insert(0, str(pathlib.Path(__file__).parent))
from common import SCRATCH  # noqa: E402

names = sys.argv[1:] or sorted(p.name for p in (SCRATCH / 'work').glob('run-*') if (p / 'out' / 'findings.json').exists())
failed = 0
for n in names:
    items = json.loads((SCRATCH / 'work' / n / 'out' / 'findings.json').read_text())
    c = {s: sum(1 for i in items if i['status'] == s) for s in ('PASS', 'FAIL', 'DIFF', 'EXPECTED', 'INFO')}
    print(f"== {n}: {c}")
    for i in items:
        if i['status'] != 'PASS':
            print(f"  [{i['status']}] {i['area']}: {i['name']}")
            if i['detail']:
                print('      ' + i['detail'][:400].replace('\n', '\n      '))
    failed += c['FAIL']
sys.exit(1 if failed else 0)
