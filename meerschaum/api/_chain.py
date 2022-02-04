#! /usr/bin/env python
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

"""
Functions useful when chaining API instances together.
"""

from __future__ import annotations

DISALLOW_CHAINING_MESSAGE = """API chaining is not enabled on this Meerschaum instance!

If you manage this server and want to allow this API instance be a parent instance for other API instances,
run `edit config system` and search for `permissions`.
Under 'api:permissions:chaining', change the value of `child_apis` to `true`, and restart the API.

It is highly encouraged that you serve this API over HTTPS! Sensitive data like pipe data and salted
password hashes will be shared between APIs."""

def check_allow_chaining() -> bool:
    """Consult the configuration to see if"""
    from meerschaum.config import get_config
    return get_config('system', 'api', 'permissions', 'chaining', 'child_apis')

