#! /usr/bin/env python3
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

"""
Wrap the Meerschaum actions into Toga commands.
"""

from meerschaum.utils.packages import attempt_import
toga = attempt_import('toga', lazy=False, venv=None)

from meerschaum.config._paths import PACKAGE_ROOT_PATH
icon_path = PACKAGE_ROOT_PATH / 'api' / 'dash' / 'assets' / 'logo_500x500.png'

def add_actions_as_commands(app) -> None:
    """Add the standard Meerschaum actions as commands.

    Parameters
    ----------
    app :
        

    Returns
    -------

    """
    from meerschaum.actions import actions
    commands = []
    for action, fn in actions.items():
        try:
            doc = fn.__doc__
        except Exception as e:
            doc = "No help available."
        commands.append(toga.Command(_action_to_command_wrapper, label=action, tooltip=doc, icon=icon_path))
    app.commands.add(*commands)
    #  app.main_window.toolbar.add(*commands)


def _action_to_command_wrapper(widget, **kw):
    print(widget.key)
