#! /usr/bin/env python3
# vim:fenc=utf-8

"""
Define the `fetch` method for reading from Valkey databases.
"""

import json
from datetime import datetime, timezone

import meerschaum as mrsm
from meerschaum.utils.typing import Optional, Dict, Any, List, Union
from meerschaum.utils.warnings import warn, dprint


def fetch(
    self,
    pipe: mrsm.Pipe,
    begin: Union[datetime, int, None] = None,
    end: Union[datetime, int, None] = None,
    params: Optional[Dict[str, Any]] = None,
    debug: bool = False,
    **kwargs: Any
) -> List[Dict[str, Any]]:
    """
    Return data from a source database.
    """
    source_key = pipe.parameters.get('valkey', {}).get('key', None)
    if not source_key:
        return []

    try:
        key_type = self.client.type(source_key).decode('utf-8')
    except Exception:
        warn(f"Could not determine the type for key '{source_key}'.")
        return []

    begin_ts = (
        (
            int(begin.replace(tzinfo=timezone.utc).timestamp())
            if isinstance(begin, datetime)
            else int(begin)
        )
        if begin is not None else '-inf'
    )
    end_ts = (
        (
            int(end.replace(tzinfo=timezone.utc).timestamp())
            if isinstance(end, datetime)
            else int(end)
        )
        if end is not None else '+inf'
    )

    if debug:
        dprint(f"Reading documents with {begin_ts=}, {end_ts=}")

    if key_type == 'set':
        return [
            json.loads(doc_bytes.decode('utf-8'))
            for doc_bytes in self.client.smembers(source_key)
        ]

    if key_type == 'zset':
        return [
            json.loads(doc_bytes.decode('utf-8'))
            for doc_bytes in self.client.zrangebyscore(
                source_key,
                begin_ts,
                end_ts,
                withscores=False,
            )
        ]

    return [{source_key: self.get(source_key)}]
