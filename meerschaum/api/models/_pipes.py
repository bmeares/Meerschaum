#! /usr/bin/env python
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

"""
Register new Pipes
"""

from __future__ import annotations
from meerschaum.utils.typing import Any, Dict, Optional

from meerschaum.utils.packages import attempt_import
pydantic = attempt_import('pydantic', warn=False)

class MetaPipe(pydantic.BaseModel):
    connector_keys: str ### e.g. sql:main
    metric_key: str
    location_key: Optional[str] = None
    instance: Optional[str] = None


