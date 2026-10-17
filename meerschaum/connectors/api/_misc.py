#! /usr/bin/env python
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

"""
Misc API routes (the Rust client).
"""

from __future__ import annotations

from meerschaum.utils.typing import Optional


def get_mrsm_version(self, **kw) -> Optional[str]:
    """
    Return the Meerschaum version of the API instance.
    """
    return self._rust_client().get_mrsm_version()


def get_chaining_status(self, **kw) -> Optional[bool]:
    """
    Fetch the chaining status of the API instance.
    """
    return self._rust_client().get_chaining_status()
