#! /usr/bin/env python
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

"""
Definition for GunicornApp class
"""

### NOTE: The commented-out code adds compatability
###       with Flask-Script. However, Flask-Script
###       has been dropped as a dependency in favor
###       of the Meerschaum actions interface.

#  from flask_script import Command, Option

class GunicornApp():
    #  def get_options(self):
        #  from gunicorn.config import make_settings

        #  settings = make_settings()
        #  options = (
            #  Option(*klass.cli, dest=klass.name, default=klass.default)
            #  for setting, klass in settings.items() if klass.cli
        #  )
        #  return options

    def __call__(self, app=None, *args, **kwargs):
        from gunicorn.app.base import Application
        class FlaskApplication(Application):
            def init(self, parser, opts, args):
                return kwargs

            def load(self):
                return app

        FlaskApplication().run()

