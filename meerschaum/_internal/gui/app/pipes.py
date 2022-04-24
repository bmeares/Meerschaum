#! /usr/bin/env python3
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

"""
Interact with Meerschaum Pipes via the GUI.
"""

from meerschaum._internal.gui.app import toga

def build_pipes_tree(**kw) -> toga.Tree:
    """Retrieve pipes and return a `toga.Tree` object.

    Parameters
    ----------
    **kw :
        

    Returns
    -------

    """
    from meerschaum.utils.formatting import ANSI, CHARSET, UNICODE
    from meerschaum.config import get_config
    from meerschaum import get_pipes

    icons = {'connector': '', 'metric': '', 'location': '',}
    if UNICODE:
        icons['connector'] = get_config('formatting', 'emoji', 'connector')
        icons['metric'] = get_config('formatting', 'emoji', 'metric')
        icons['location'] = get_config('formatting', 'emoji', 'location')

    kw.pop('as_list', None)
    pipes = get_pipes(as_list=False, **kw)
    tree = toga.Tree(
        headings = ["Pipes"],
        style = toga.style.Pack(flex=1, padding=10, direction='column'),
        on_select = _pipes_tree_on_select_handler,
    )
    for ck in pipes:
        ck_root = tree.data.append(None, pipes=(icons['connector'] + '  ' + ck))
        for mk in pipes[ck]:
            mk_root = tree.data.append(ck_root, pipes=(icons['metric'] + '  ' + mk))

            for lk in pipes[ck][mk]:
                _lk = lk if lk is not None else "None"
                tree.data.append(mk_root, pipes=(icons['location'] + '  ' + _lk + '  '))
    return tree


def _pipes_tree_on_select_handler(
        widget: toga.Widget,
        node: toga.sources.tree_source.Node,
    ):
    pass
    #  self.right_box._children = [toga.Label("memes")]
    #  self.label.text = 'memes'
