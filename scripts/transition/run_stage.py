#!/usr/bin/env python3
"""Re-run one stage of a finished run: python3 run_stage.py <run-name> <stage> [stage args...] (e.g. dash sqlite)."""
import json
import pathlib
import sys

sys.path.insert(0, str(pathlib.Path(__file__).parent))
from common import *  # noqa: E402,F403
import stage_v5  # noqa: E402

run = Run(SCRATCH / 'work' / sys.argv[1])
run.state = json.loads(run.state_file.read_text())
F = stage_v5.Findings(run)
F.items = [i for i in json.loads((run.out / 'findings.json').read_text()) if i['area'] != sys.argv[2].split('_')[0]] if (run.out / 'findings.json').exists() else []
getattr(stage_v5, sys.argv[2])(run, F, *sys.argv[3:])
