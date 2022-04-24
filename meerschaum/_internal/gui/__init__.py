#! /usr/bin/env python3
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

"""
Meerschaum GUI definition. Start the GUI with `start gui`.
"""

from meerschaum.config.static import _static_config
from meerschaum.utils.packages import attempt_import
from meerschaum.config import __version__
from meerschaum.config._paths import PACKAGE_ROOT_PATH
from meerschaum.utils.threading import Lock

from meerschaum._internal.gui.app import MeerschaumApp

icon_path = PACKAGE_ROOT_PATH / 'api' / 'dash' / 'assets' / 'logo_500x500.png'

locks = {'app': Lock()}
_app = None

def get_app(**kw) -> MeerschaumApp:
    """Instantiate and return the main app."""
    global _app
    if _app is None:
        with locks['app']:
            _app = build_app(**kw)
    return _app

def build_app(**kw) -> MeerschaumApp:
    """Construct and return an instance of the GUI application."""
    _kw = dict(
        formal_name = _static_config()['setup']['formal_name'],
        app_id = _static_config()['setup']['app_id'],
        app_name = _static_config()['setup']['name'],
        author = _static_config()['setup']['author'],
        description = _static_config()['setup']['description'],
        icon = icon_path,
        version = __version__,
        home_page = _static_config()['setup']['url'],
    )
    _kw.update(kw)
    return MeerschaumApp(**_kw)
