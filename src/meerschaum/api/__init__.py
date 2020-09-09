#! /usr/bin/env python
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

from meerschaum.config import system_config
from fastapi import FastAPI

sys_config = system_config['api']
fast_api = FastAPI()
__version__ = sys_config['version']
port = sys_config['port']

### import WebAPI routes
import meerschaum.api.routes as routes

