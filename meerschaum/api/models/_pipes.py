#! /usr/bin/env python
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

"""
Register new Pipes
"""
from meerschaum.utils.misc import attempt_import
pydantic, sqlalchemy, databases = attempt_import('pydantic', 'sqlalchemy', 'databases')
from meerschaum.connectors import SQLConnector

class PipeIn(pydantic.BaseModel):
    building_key : str
    metric : str

class Pipe(pydantic.BaseModel):
    pipe_id : int
    building_key : str
    metric : str

