import sys, pathlib
sys.path.insert(0, str(pathlib.Path(__file__).parent))
from common import *
import stage_v5, stage_pip
work = SCRATCH / 'work' / 'run-pip'
work.mkdir(parents=True, exist_ok=True)
run = Run(work)
F = stage_v5.Findings(run)
v = stage_pip.main(sys.argv[1], F)
print('venv', v)
stage_pip.bare_host(sys.argv[1], F)
