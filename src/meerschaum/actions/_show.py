#! /usr/bin/env python
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

"""
This module contains functions for printing elements.
"""

def show(
        action:list=[],
        **kw
    ) -> tuple:
    """
    Show elements of a certain type.
    
    command: `show actions`

    Options:
        - actions
        - pipes

    params:
        action : list
            What items to show 
            The 'action' list contains strings following the 'show' command'.
            e.g.: `show actions` becomes ['actions']
    """
    
    show_options = {
        'actions' : _show_actions,
        'pipes'   : _show_pipes,
    }

    if len(action) == 0: action = ['']
    show_choice = action[0]

    if show_choice not in show_options:
        print(f"Cannot show '{show_choice}'. Choose one:")
        for option in show_options:
            print(f"  - {option}")
        return (False, f"Invalid choice '{show_choice}'")

    return show_options[show_choice](**kw)


def _show_actions(nopretty=False, **kw) -> tuple:
    """
    Show available actions
    """
    from meerschaum.actions import actions
    if not nopretty:
        header = "Available actions:"
        print("\n" + header)
        ### calculate underline length
        underline_len = len(header)
        for a in actions:
            if len(a) + 4 > underline_len:
                underline_len = len(a) + 4
        ### print underline
        for i in range(underline_len): print('-', end="")
        print("\n", end="")
    ### print actions
    for action in sorted(actions):
        if not nopretty: print("  - ", end="")
        print(action)
    return (True, "Success")

### TODO
def _show_pipes(**kw):
    pass
