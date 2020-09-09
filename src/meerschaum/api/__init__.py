#! /usr/bin/env python
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

"""
Resources are for Meerschaum endpoints
and routes are for WebAPI endpoints only.

e.g.
   '/meerschaum/version' => resource
       Returns the Meerschaum version
   '/version' => route
       Returns the Meerschaum WebAPI version
"""

from flask import Flask 
from meerschaum.api.GunicornApp import GunicornApp
#  from flask_script import Manager
from flask_restful import Api
from meerschaum.config import system_config

sys_config = system_config['api']
flask_app = Flask(__name__.split('.')[0])
flask_api = Api(flask_app)
gunicorn_app = GunicornApp()
#  manager = Manager(flask_app)
__version__ = sys_config['version']
port = sys_config['port']

### add resources to flask_api using the `urls` member to
### specify endpoint urls
import meerschaum.api.resources as resources
import inspect
for member in inspect.getmembers(resources):
    if inspect.isclass(member[1]):
        if hasattr(member[1], 'urls'):
            flask_api.add_resource(member[1], *member[1].urls)

### import WebAPI routes
import meerschaum.api.routes as routes

