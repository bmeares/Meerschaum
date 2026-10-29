#! /usr/bin/env python3
# vim:fenc=utf-8

"""
Define the `fetch` method for reading from Valkey databases.
"""

from datetime import datetime

import meerschaum as mrsm
from meerschaum.utils.typing import Optional, Dict, Any, List, Union


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
    Return the documents of the source key `parameters['valkey']['key']`
    (a set's members, a sorted set's members within `begin` and `end`, or `{key: value}`).
    """
    return self._rust_client().fetch(pipe, begin=begin, end=end, params=params)
