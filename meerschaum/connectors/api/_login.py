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

    The API instance must answer `/chaining`; keyword arguments are those of `retry_connect()`:
    `max_retries` (default 1), `retry_wait` (default 0), `warn` (default `False`),
    `enforce_chaining` and `enforce_login` (default `False`).
    """
    import time
    from meerschaum.utils.warnings import warn as _warn
    max_retries = kw.get('max_retries', 1)
    retry_wait = kw.get('retry_wait', 0)
    warn = kw.get('warn', False)
    enforce_chaining = kw.get('enforce_chaining', False)
    enforce_login = kw.get('enforce_login', False)

    try:
        retries = 0
        chaining_status = None
        while retries < max_retries:
            connected = None
            if not isinstance(chaining_status, bool):
                chaining_status = self.get_chaining_status()
                if chaining_status is False:
                    if enforce_chaining:
                        if warn:
                            _warn(
                                f"Meerschaum instance '{self}' does not allow chaining "
                                + "and cannot be used as the parent for this instance.",
                                stack=False,
                            )
                        return False
                    chaining_status = True

            if chaining_status:
                connected = self.login(warn=warn)[0] if enforce_login else True
                if not connected and warn:
                    _warn(f"Unable to login to '{self}'!", stack=False)

            if connected:
                return True

            if warn:
                _warn(f"Connection to '{self}' failed.", stack=False)
            if retry_wait > 0:
                time.sleep(retry_wait)
            retries += 1
        return False
    except Exception:
        return False
