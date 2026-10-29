"""
A synthetic v4-pattern streaming connector (the shape of the public `mqtt-connector` plugin):
a `@make_connector` class assembled from imported methods whose `sync(pipe)` subscribes and
returns at once, and whose message callback calls `pipe.sync(docs)`. It also has an unrelated
`subscribe(topic, callback)` method of its own and no `IS_STREAMING`.

A `@post_sync_hook` appends what it receives to the file named by `W9_HOOK_LOG`.
"""

import json
import os

from meerschaum.plugins import post_sync_hook

from .connector import W9MQTTConnector

__version__ = '0.0.1'
required = []


@post_sync_hook
def record_sync(
    pipe,
    success_tuple=None,
    sync_timestamp=None,
    sync_complete_timestamp=None,
    sync_duration=None,
    **kwargs
):
    if not str(pipe.connector_keys).startswith('w9mqtt:'):
        return True, "Skipped."
    received = {
        'success_tuple': success_tuple,
        'sync_timestamp': sync_timestamp,
        'sync_complete_timestamp': sync_complete_timestamp,
        'sync_duration': sync_duration,
    }
    with open(os.environ['W9_HOOK_LOG'], 'a') as f:
        f.write(json.dumps({
            'pipe': str(pipe),
            'success': bool(success_tuple[0]) if success_tuple else None,
            'message': str(success_tuple[1]) if success_tuple else None,
            'present': sorted(k for k, v in received.items() if v is not None),
        }) + '\n')
    return True, "Recorded the sync."
