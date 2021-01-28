from __future__ import annotations
from typing import *
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


