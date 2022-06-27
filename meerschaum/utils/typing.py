from __future__ import annotations
try:
    from typing import (
        Tuple,
        Optional,
        Dict,
        List,
        Mapping,
        Sequence,
        Callable,
        Union,
        Any,
        Iterable,
    )
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
PipesDict = Dict[
    str, Dict[                           ### connector_keys : metrics
        str, Dict[                       ### metric_key     : locations
            str, 'meerschaum.Pipe'       ### location_key   : Pipe
        ]
    ]
]
WebState = Dict[str, str]
