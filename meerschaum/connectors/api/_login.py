#! /usr/bin/env python
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

"""
Log into the API instance or refresh the token (the Rust client).
"""

from __future__ import annotations

from meerschaum.utils.typing import SuccessTuple, Any, Union


def login(
    self,
    debug: bool = False,
    warn: bool = True,
    **kw: Any
) -> SuccessTuple:
    """Log in and set the session token."""
    if warn and self.__dict__.get('_warn', None) is False:
        warn = False
    return self._rust_client().login(warn=warn)


def test_connection(
    self,
    **kw: Any
) -> Union[bool, None]:
    """
    Test if a successful connection to the API may be made.

    The keyword arguments are those of `meerschaum.connectors.poll.retry_connect()`:
    `max_retries` (default 1), `retry_wait` (default 0), `warn` (default `False`),
    `enforce_chaining` and `enforce_login` (default `False`). With `warn`, a failed attempt
    says why (`cannot connect to localhost:8000 (connection refused)`).
    """
    from meerschaum.connectors.poll import retry_connect
    from meerschaum.utils.misc import filter_keywords
    _default_kw = {
        'max_retries': 1, 'retry_wait': 0, 'warn': False,
        'connector': self, 'enforce_chaining': False,
        'enforce_login': False,
    }
    _default_kw.update(kw)
    try:
        return retry_connect(**filter_keywords(retry_connect, **_default_kw))
    except Exception:
        return False
