#! /usr/bin/env python
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

"""
Pydantic model for a pipe's keys.
"""

from __future__ import annotations

import meerschaum as mrsm
from meerschaum.utils.typing import Optional

pydantic = mrsm.attempt_import('pydantic', warn=False, lazy=False)


class MetaPipe(pydantic.BaseModel):
    connector_keys: str
    metric_key: str
    location_key: Optional[str] = None
    instance_keys: Optional[str] = None
