#! /usr/bin/env python3
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

"""
Define different windows for the GUI application.
"""

from meerschaum.gui.app import toga

def get_main_window(self) -> toga.window.Window:
    from meerschaum.utils.misc import get_connector_labels
    if 'main_window' in self._windows:
        return self._windows['main_window']

    main_window = toga.MainWindow(title=self.name)
    self.add_actions_as_commands()
    self.tree = self.build_pipes_tree(
        mrsm_instance=self._instance, debug=self._debug, **self._kw
    )
    sub_menu = toga.Group("Sub Menu", parent=toga.Group.COMMANDS, order=2)

    self.left_box = toga.Box(children=[
        toga.Box(children=[
            toga.Selection(items=get_connector_labels('sql', 'api'), style=toga.style.Pack(flex=1)),
            self.tree,
            toga.Button('Hello, world!', style=toga.style.Pack(flex=1, padding=10)),
        ], style=toga.style.Pack(flex=1, padding=10, direction='column', width=200))

    ])
    self.label = toga.Label("foo!")
    self.webview = toga.WebView(url='http://localhost:8765', style=toga.style.Pack(flex=1))
    self.right_box = toga.Box(children=[
        self.webview,
        #  self.label,
    ], style=toga.style.Pack(flex=1))

    main_box = toga.Box(children=[self.left_box, self.right_box])
    main_window.content = main_box

    from meerschaum.config._paths import PACKAGE_ROOT_PATH
    icon_path = PACKAGE_ROOT_PATH / 'api' / 'dash' / 'assets' / 'logo_500x500.png'
    command = toga.Command(_open_webterm, label='Open Terminal', icon=icon_path, tooltip=_open_webterm.__doc__)
    #  main_window.toolbar.add(command)
    self._windows['main_window'] = main_window
    return self._windows['main_window']

def _open_webterm():
    """
    Foo bar
    """
    pass
