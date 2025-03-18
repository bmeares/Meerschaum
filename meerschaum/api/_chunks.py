#! /usr/bin/env python3
# vim:fenc=utf-8

"""
Utility functions for the retrieval, caching, and response of chunk data.
"""

import random
from datetime import datetime, timedelta, timezone
from typing import Dict, Generator, Any, Union, Optional, List

import meerschaum as mrsm
from meerschaum.api import get_cache_connector
from meerschaum.utils.misc import generate_password

CHUNKS_TOKENS_GENERATORS: Dict[str, Dict[str, Union[Generator[Any, None, None], datetime, int]]]
DEFAULT_TTL_SECONDS = mrsm.get_config('system', 'api', 'data', 'chunks', 'ttl_seconds')


def generate_chunks_cursor_token(
    pipe: mrsm.Pipe,
    select_columns: Optional[List[str]] = None,
    omit_columns: Optional[List[str]] = None,
    begin: Union[datetime, int, None] = None,
    end: Union[datetime, int, None] = None,
    params: Optional[Dict[str, Any]] = None,
    limit: Optional[int] = None,
    order: Optional[str] = 'asc',
    ttl_seconds: Optional[int] = None,
    debug: bool = False,
) -> str:
    """
    Store a generator in the `CHUNKS_TOKENS_GENERATORS`
    """
    now = datetime.now(timezone.utc)
    cache_connector = get_cache_connector()
    if cache_connector is None:
        pass

    ttl_seconds = ttl_seconds or DEFAULT_TTL_SECONDS
    chunk_bounds = pipe.get_chunk_bounds(
        begin=begin,
        end=end,
        bounded=True,
    )

    while True:
        chunk_token = prefix + generate_password(random.randint(6, 12))
        if chunk_token in CHUNKS_TOKENS_GENERATORS:
            continue
        break

    CHUNKS_TOKENS_GENERATORS[chunk_token] = {
        'generator': chunk_generator,
        'created': now,
        'ttl': ttl_seconds,
        'last_accessed': now,
    }

    return chunk_token


def deallocate_expired_generators():
    """
    Periodically delete chunk tokens with an expired ttl timestamp.
    """
    chunk_tokens = list(CHUNKS_TOKENS_GENERATORS)
