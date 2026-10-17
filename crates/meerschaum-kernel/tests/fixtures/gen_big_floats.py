#!/usr/bin/env python3
"""Record v4's integral-float overflow behavior without any connectors or processes."""
import json
import os
from pathlib import Path
import tempfile

with tempfile.TemporaryDirectory(prefix='mrsm-core-floats-') as root:
    for key in list(os.environ):
        if key.startswith('MRSM_'):
            del os.environ[key]
    os.environ['MRSM_ROOT_DIR'] = root
    os.environ['MRSM_PLUGINS_DIR'] = root + '/plugins'
    os.mkdir(os.environ['MRSM_PLUGINS_DIR'])
    os.environ['MRSM_CORE'] = 'python'
    os.environ['MRSM_NO_AUTO_INSTALL'] = 'true'
    import pandas as pd
    from meerschaum.utils.dataframe import enforce_dtypes
    cases = []
    for value in [1e20, -1e20, float(2**63), float(2**64), -float(2**63), 1.0]:
        df = pd.DataFrame([{'a': value}]).convert_dtypes()
        out = enforce_dtypes(df, {'a': 'int'}, debug=False)
        cases.append({'input': value, 'converted_dtype': str(df['a'].dtype),
                      'converted_value': float(df['a'].iloc[0]),
                      'enforced_value': float(out['a'].iloc[0])})
    Path(__file__).with_name('big_floats.json').write_text(json.dumps(cases, indent=2) + '\n')
