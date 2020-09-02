#! /usr/bin/env python
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

"""
Modules belonging to the Meerschaum WebAPI reside in this package
"""

from flask import Flask 
from flask_restful import Resource, Api

flask_app = Flask(__name__.split('.')[0])

class WebAPI:
    pass
