#! /usr/bin/env python
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

"""
Locations model
"""

from meerschaum.utils.misc import attempt_import
pydantic = attempt_import('pydantic')

class Location(pydantic.BaseModel):
    location_key : str
    location_name : str
