"""Stage 1: build v4 state (python API + CLI jobs + compose)."""
import json, pathlib, shutil, sys, time, os
from common import *


def build(run: Run, flavors):
    run.state['flavors'] = flavors
    run.state['fetch_n'] = 5
    shutil.copy(HERE / 'plugin' / 'trn_plug.py', run.plugins / 'trn_plug.py')
    shutil.copy(HERE / 'plugin' / 'trn_plain.py', run.plugins / 'trn_plain.py')
    spec = {'flavors': flavors, 'db_dir': str(run.db), 'out': str(run.work / 'v4_out.json')}
    (run.work / 'spec.json').write_text(json.dumps(spec))
    for c in (['show', 'version'], ['show', 'config', 'meerschaum'], ['show', 'pipes'], ['show', 'plugins']):
        r = run.mrsm('v4', *c, timeout=900)  # v4's first run installs its dependencies into <root>/venvs/mrsm
        print('v4 bootstrap', c, r.returncode, round(r.elapsed, 1))
    pc = run.py('v4', HERE / 'v4_config.py', run.work / 'spec.json', timeout=300)
    print(strip_ansi(pc.stdout)[-1500:], pc.stderr[-1500:])
    if pc.returncode != 0:
        raise SystemExit('v4 config failed')
    pi = run.mrsm('v4', 'install', 'required', 'trn_plug', timeout=900)
    print('install required:', pi.returncode, strip_ansi(pi.stdout)[-300:])
    pi = run.mrsm('v4', 'install', 'required', 'trn_plain', timeout=900)
    p = run.py('v4', HERE / 'v4_build.py', run.work / 'spec.json', timeout=1800)
    (run.out / 'v4_build.log').write_text(p.stdout + '\n--STDERR--\n' + p.stderr)
    print(strip_ansi(p.stdout)[-3000:])
    if p.returncode != 0:
        print(p.stderr[-3000:])
        raise SystemExit('v4 build failed')
    v4out = json.loads((run.work / 'v4_out.json').read_text())
    (run.work / 'spec_pipes.json').write_text(json.dumps({'pipes': v4out['pipes']}))
    run.state['v4_out'] = v4out
    run.save()


def jobs(run: Run):
    inst = f"sql:trn_{run.state['flavors'][0]}"
    r = run.py('v4', HERE / 'v4_jobs.py', inst, timeout=300)
    print(strip_ansi(r.stdout)[-1500:], strip_ansi(r.stderr)[-500:])
    r = run.mrsm('v4', 'show', 'jobs')
    print(strip_ansi(r.stdout))
    run.state['v4_show_jobs'] = strip_ansi(r.stdout)
    run.save()


COMPOSE_YAML = '''\
project_name: trn-proj-FLAVOR
sync:
  schedule: "every 120 seconds"
plugins_dir: ["./plugins"]
config:
  meerschaum:
    instance: "sql:trnproj"
    connectors:
      sql:
        trnproj:
          flavor: sqlite
          database: "{MRSM_ROOT_DIR}/proj.db"
pipes:
  - connector: "plugin:trn_plug"
    metric: "readings"
    location: "projloc"
    columns:
      datetime: "ts"
      id: "sensor"
  - connector: "sql:trnproj"
    metric: "derived"
    parameters:
      fetch:
        definition: "SELECT ts, sensor, reading * 2 AS doubled FROM {{ Pipe('plugin:trn_plug', 'readings', 'projloc', instance='sql:trnproj') }}"
      columns:
        datetime: "ts"
        id: "sensor"
'''


def compose(run: Run):
    proj = run.work / 'compose-proj'
    (proj / 'plugins').mkdir(parents=True, exist_ok=True)
    shutil.copy(HERE / 'plugin' / 'trn_plug.py', proj / 'plugins' / 'trn_plug.py')
    (proj / 'mrsm-compose.yaml').write_text(COMPOSE_YAML.replace('FLAVOR', run.state['flavors'][0]))
    run.state['compose_dir'] = str(proj)
    for sub in (['compose', 'init'], ['compose', 'up'], ['compose', 'ps']):
        r = run.mrsm('v4', *sub, cwd=proj, timeout=600)
        print(sub, r.returncode, strip_ansi(r.stdout)[-600:], strip_ansi(r.stderr)[-300:])
        run.state['v4_compose_' + sub[1]] = strip_ansi(r.stdout) + strip_ansi(r.stderr)
        if sub[1] == 'up':
            time.sleep(8)
    run.save()


def snapshot(run: Run, which: str, name: str):
    p = run.py(which, HERE / 'snapshot.py', run.work / 'spec_pipes.json', run.out / f'{name}.pkl', timeout=900)
    print(strip_ansi(p.stdout)[-300:], p.stderr[-1000:])
    return p
