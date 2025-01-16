#! /usr/bin/env python3
# vim:fenc=utf-8

"""
Define custom API exceptions.
"""

import meerschaum as mrsm

_ = mrsm.attempt_import('fastapi', lazy=False)
from fastapi import HTTPException


class APIPermissionError(HTTPException):
    """Raise if the configured Meerschaum API permissions disallow an action."""

    def __init__(self, detail: str = "Permission denied.", status_code: int = 403):
        super().__init__(status_code=status_code, detail=detail)
