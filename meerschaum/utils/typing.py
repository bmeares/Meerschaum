from __future__ import annotations
try:
    from typing import *
except:
    import urllib.request, sys
    url = 'https://raw.githubusercontent.com/python/typing/master/src/typing.py'
    response = urllib.request.urlopen(url)
    if response.code != 200:
        print(f"Could not download typing. Please install typing via pip or upgrade Python.")
        sys.exit(1)
    fname = 'typing_hotfix' 
    with open(fname, 'w+') as f:
        f.write(response.fp.read())
    from typing_hotfix import *

    
SuccessTuple = Tuple[bool, str]
InstanceConnector = Union[
    'meerschaum.connectors.sql.SQLConnector',
    'meerschaum.connectors.api.APIConnector'
]
PipesDict = Mapping[
    str, Mapping[                   ### connector_keys : metrics
        str, Mapping[               ### metric_key     : locations
            str, 'meerschaum.Pipe.Pipe'  ### location_key   : Pipe
        ]
    ]
]
#  from meerschaum.utils.packages import attempt_import
### trigger an install of typing_extensions, which is needed for rich
#  typing_extensions = attempt_import('typing_extensions', lazy=False)


