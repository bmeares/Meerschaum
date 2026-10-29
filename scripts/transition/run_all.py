#!/usr/bin/env python3
"""Run the whole v4 -> v5 transition for one flavor.

usage: python3 run_all.py <run-name> [flavor] [--skip-build] [--extras] [--only stage,stage]
  (stages: config pipes cli plugins python-api ddl engine api env-uri dash jobs compose; the v4 build always runs
  unless --skip-build)

  --extras   also run the flavor-independent checks (env-URI connectors, the v4-style Dash plugin)
"""
import json
import pathlib
import shutil
import sys

sys.path.insert(0, str(pathlib.Path(__file__).parent))
from common import *  # noqa: E402,F403
import stage_v4  # noqa: E402
import stage_v5  # noqa: E402

name = sys.argv[1]
flavor = sys.argv[2] if len(sys.argv) > 2 and not sys.argv[2].startswith('--') else 'sqlite'
skip_build = '--skip-build' in sys.argv
only = sys.argv[sys.argv.index('--only') + 1].split(',') if '--only' in sys.argv else None
extras = '--extras' in sys.argv
work = SCRATCH / 'work' / name
if not skip_build and work.exists():
    shutil.rmtree(work)
run = Run(work)
F = stage_v5.Findings(run)
if not skip_build:
    stage_v4.build(run, [flavor])
    stage_v4.snapshot(run, 'v4', 'v4_after_build')
    if only is None or 'jobs' in only or 'compose' in only:
        stage_v4.jobs(run)
        stage_v4.compose(run)
else:
    run.state = json.loads(run.state_file.read_text())
try:
    def want(n):
        return only is None or n in only
    if want('config'):
        stage_v5.config(run, F, flavor)
    if want('pipes'):
        stage_v5.snapshots(run, F, 'first')
    if want('cli'):
        stage_v5.cli_diffs(run, F, flavor)
    if want('pipes'):
        stage_v5.continue_pipes(run, F, flavor)
        stage_v5.snapshots(run, F, 'post')
    if want('plugins'):
        stage_v5.plugins(run, F, flavor)
    if want('pipes'):
        stage_v5.copy_verify_drop(run, F, flavor)
    if want('python-api'):
        stage_v5.pyapi(run, F)
    if want('ddl'):
        stage_v5.ddl(run, F, flavor)
    if want('engine'):
        stage_v5.engine_free(run, F, flavor)
    if want('api'):
        stage_v5.api(run, F, flavor)
    if extras and want('env-uri'):
        stage_v5.envuri(run, F, flavor)
    if extras and want('dash'):
        stage_v5.dash(run, F, flavor)
    if want('jobs'):
        stage_v5.jobs(run, F, flavor)
    if want('compose'):
        stage_v5.compose(run, F)
finally:
    # a stage that raised leaves the v4 daemons and the compose job running: stop them (only this run's roots)
    # BEFORE their database is dropped under them
    left = stop_roots(run.work, run.work / 'compose-proj' / 'root')
    F.add('cleanup', 'processes left under the run roots (stopped now)' if left else 'processes left under the run roots', 'PASS' if not left else 'FAIL', left)
    if flavor != 'sqlite':
        run.py('v4', HERE / 'v4_cleanup.py', flavor)
    n = {s: sum(1 for i in F.items if i['status'] == s) for s in ('PASS', 'FAIL', 'DIFF', 'EXPECTED', 'INFO')}
    print('SUMMARY', n)
