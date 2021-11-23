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

    left_box = toga.Box(children=[
        toga.Box(children=[
            toga.Selection(items=get_connector_labels('sql', 'api'), style=toga.style.Pack(flex=1)),
            self.tree,
            toga.Button('Hello, world!', style=toga.style.Pack(flex=1, padding=10)),
        ], style=toga.style.Pack(flex=1, padding=10, direction='column', width=200))

    ])
    right_box = toga.Box(children=[
        toga.Label("Foo!"),
    ])

    main_box = toga.Box(children=[left_box, right_box])
    main_window.content = main_box
    self._windows['main_window'] = main_window
    return self._windows['main_window']

