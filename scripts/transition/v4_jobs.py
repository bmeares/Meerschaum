"""v4 jobs through the v4 Job API (same as `-d --name`; v4's CLI refuses -d without a tty)."""
import json, sys, time, pathlib
import meerschaum as mrsm
from meerschaum.jobs import Job
assert mrsm.__version__ == '4.1.4'
inst = sys.argv[1]
from common import job_tag
flv = job_tag(inst.split('trn_', 1)[-1])  # job names carry the flavor: v4 finds running daemons by scanning the process table, so concurrent runs must not share names
base = f"sync pipes -i {inst} -c plugin:trn_plug -m readings"
res = {}
j = Job(f'trn-{flv}-finished', base)
res['finished_start'] = j.start()
t0 = time.time()
while j.status == 'running' and time.time() - t0 < 60:
    time.sleep(0.5)
l = Job(f'trn-{flv}-looper', base + ' --loop --min-seconds 2')
res['looper_start'] = l.start()
time.sleep(7)
res['looper_stop'] = l.stop()
time.sleep(2)
r = Job(f'trn-{flv}-runner', base + ' --loop --min-seconds 3')
res['runner_start'] = r.start()
time.sleep(5)
res['status'] = {x.name: x.status for x in (j, l, r)}
print(json.dumps(res, default=str, indent=1))
