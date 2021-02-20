#! /usr/bin/env python
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

"""
Register new Pipes
"""
from meerschaum.utils.packages import attempt_import
pydantic = attempt_import('pydantic', warn=False)

class MetaPipe(pydantic.BaseModel):
    connector_keys : str ### e.g. sql:main
    metric_key : str
    location_key : str = None
    parameters : dict = None

