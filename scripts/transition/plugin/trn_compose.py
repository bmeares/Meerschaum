# The compose project's plugin for the transition harness (synthetic). It has no `required`
# packages, so v4's `compose init` / `up` never run an installer: package reuse is tested by
# `trn_plug` in the plugin stage. Its pipe exercises a real v4 compose job and the v5 handover.
__version__ = '1.0.0'
required = []

import os
import datetime


def _log(msg):
    p = os.environ.get('TRN_LOG')
    if p:
        with open(p, 'a') as f:
            f.write(f"{os.getpid()} compose {msg}\n")


def fetch(pipe, begin=None, end=None, debug=False, **kw):
    from meerschaum.utils.packages import attempt_import
    pd = attempt_import('pandas')
    n = int(os.environ.get('TRN_FETCH_N', '5'))
    base = datetime.datetime(2024, 3, 1)
    rows = [
        {'ts': base + datetime.timedelta(hours=i), 'sensor': f's{i % 2}', 'reading': i * 0.5}
        for i in range(n)
    ]
    _log(f"fetch n={n}")
    return pd.DataFrame(rows)
