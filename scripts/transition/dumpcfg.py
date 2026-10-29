import json, sys
import meerschaum as mrsm
from meerschaum.config import get_config
cfg = get_config()
json.dump(cfg, open(sys.argv[1], 'w'), indent=1, sort_keys=True, default=str)
print(mrsm.__version__)
