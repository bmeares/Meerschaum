#! /usr/bin/env python3
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

"""
Define different windows for the GUI application.
"""

from __future__ import annotations
from meerschaum.utils.typing import List, Dict, Optional

from meerschaum._internal.gui.app import toga

def get_windows(**kw) -> Dict[str, toga.window.Window]:
    return {
        #  'main': get_main_window(**kw),
        'terminal': get_terminal_window(**kw),
    }


def get_main_window(instance: Optional[str], debug: bool = False, **kw) -> toga.window.Window:
    from meerschaum.config.static import _static_config
    from meerschaum.utils.misc import get_connector_labels
    from meerschaum.connectors import instance_types
    from meerschaum._internal.gui.app.pipes import build_pipes_tree

    main_window = toga.MainWindow(title=_static_config()['setup']['formal_name'], size=(1280, 720))
    tree = build_pipes_tree(mrsm_instance=instance, debug=debug, **kw)
    sub_menu = toga.Group("Sub Menu", parent=toga.Group.COMMANDS, order=2)

    left_box = toga.Box(children=[
        toga.Box(children=[
            toga.Selection(items=get_connector_labels(*instance_types), style=toga.style.Pack(flex=1)),
            tree,
            toga.Button('Hello, world!', style=toga.style.Pack(flex=1, padding=10), on_press=show_test_window),
        ], style=toga.style.Pack(flex=1, padding=10, direction='column', width=200))

    ])
    label = toga.Label("foo!")
    right_box = toga.Box(children=[], style=toga.style.Pack(flex=1))

    main_box = toga.Box(children=[left_box, right_box])

    option_container = toga.OptionContainer()
    option_container.add('foo', label)
    #  option_container.add('Terminal', self.webview)
    #  main_box = toga.Box(option_container)
    #  main_box = option_container
    main_window.content = main_box

    from meerschaum.config._paths import PACKAGE_ROOT_PATH
    icon_path = PACKAGE_ROOT_PATH / 'api' / 'dash' / 'assets' / 'logo_500x500.png'
    command = toga.Command(_open_webterm, label='Open Terminal', icon=icon_path, tooltip=_open_webterm.__doc__)
    #  main_window.toolbar.add(command)
    #  self._windows['main_window'] = main_window
    #  return self._windows['main_window']
    return main_window


def get_terminal_window(**kw):
    window = toga.Window(title='Terminal')
    webview = toga.WebView(url='http://localhost:8765', style=toga.style.Pack(flex=1))
    box = toga.Box(children=[webview])

    window.content = box
    return window


def _open_webterm():
    """Foo bar"""

def show_test_window(button):
    from meerschaum._internal.gui import get_app
    get_app()._windows['terminal'].show()
