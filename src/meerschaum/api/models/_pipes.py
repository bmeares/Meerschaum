#! /usr/bin/env python
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

"""
Register new Pipes
"""

from pydantic import BaseModel
import sqlalchemy, databases
from meerschaum.connectors import SQLConnector

class PipeIn(BaseModel):
    building_key : str
    metric : str

class Pipe(BaseModel):
    pipe_id : int
    building_key : str
    metric : str

