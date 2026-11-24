#!/usr/bin/env python3
"""Print the findings of the runs under the scratch directory: `report.py [run-name ...]`; exit 1 on any FAIL."""
import argparse
import json
import pathlib
import sys

sys.path.insert(0, str(pathlib.Path(__file__).parent))
from common import SCRATCH  # noqa: E402

parser = argparse.ArgumentParser(description=__doc__)
parser.add_argument('--strict', action='store_true', help='Fail on unexplained differences or empty findings.')
parser.add_argument('names', nargs='*')
args = parser.parse_args()
names = args.names or sorted(p.name for p in (SCRATCH / 'work').glob('run-*') if (p / 'out' / 'findings.json').exists())
failed = int(args.strict and not names)
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
    if args.strict:
        failed += c['DIFF'] + int(not items)
sys.exit(1 if failed else 0)
