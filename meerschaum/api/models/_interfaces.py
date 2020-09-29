#! /usr/bin/env python
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

"""
ORM Model for Interfaces
"""

from meerschaum.utils.misc import attempt_import
pydantic = attempt_import('pydantic')

class Interface(pydantic.BaseModel):
    connector_keys : str


