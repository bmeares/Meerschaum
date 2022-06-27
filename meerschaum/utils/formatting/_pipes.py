#! /usr/bin/env python
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

"""
Formatting functions for printing pipes
"""

from __future__ import annotations
from meerschaum.utils.typing import PipesDict, Dict, Union, Optional

def pprint_pipes(pipes: PipesDict) -> None:
    """Print a stylized tree of a Pipes dictionary.
    Supports ANSI and UNICODE global settings."""
    from meerschaum.utils.warnings import error
    from meerschaum.utils.packages import attempt_import, import_rich
    from meerschaum.utils.misc import sorted_dict, replace_pipes_in_dict
    from meerschaum.utils.formatting import UNICODE, ANSI, CHARSET, pprint, colored, get_console
    from meerschaum.config import get_config
    import copy
    rich = import_rich('rich', warn=False)
    Text = None
    if rich is not None:
        rich_text = attempt_import('rich.text')
        Text = rich_text.Text

    icons = get_config('formatting', 'pipes', CHARSET, 'icons')
    styles = get_config('formatting', 'pipes', 'ansi', 'styles')
    if not ANSI:
        styles = {k: '' for k in styles}
    print()

    def ascii_print_pipes():
        """Print the dictionary with no unicode allowed. Also works in case rich fails to import
        (though rich should auto-install when `attempt_import()` is called)."""
        asciitree = attempt_import('asciitree')
        ascii_dict, replace_dict = {}, {'connector': {}, 'metric': {}, 'location': {}}
        for conn_keys, metrics in pipes.items():
            _colored_conn_key = colored(icons['connector'] + conn_keys, style=styles['connector'])
            if Text is not None:
                replace_dict['connector'][_colored_conn_key] = (
                    Text(conn_keys, style=styles['connector'])
                )
            ascii_dict[_colored_conn_key] = {}
            for metric, locations in metrics.items():
                _colored_metric_key = colored(icons['metric'] + metric, style=styles['metric'])
                if Text is not None:
                    replace_dict['metric'][_colored_metric_key] = (
                        Text(metric, style=styles['metric'])
                    )
                ascii_dict[_colored_conn_key][_colored_metric_key] = {}
                for location, pipe in locations.items():
                    _location_style = styles[('none' if location is None else 'location')]
                    pipe_addendum = '\n         ' + pipe.__repr__() + '\n'
                    _colored_location = colored(
                        icons['location'] + str(location), style=_location_style
                    )
                    _colored_location_key = _colored_location + pipe_addendum
                    if Text is not None:
                        replace_dict['location'][_colored_location] = (
                            Text(str(location), style=_location_style)
                        )
                    ascii_dict[_colored_conn_key][_colored_metric_key][_colored_location_key] = {}

        tree = asciitree.LeftAligned()
        output = ''
        cols = []

        ### This is pretty terrible, unreadable code.
        ### Please know that I'm normally better than this.
        key_str = (
            (Text("     ") if Text is not None else "     ") +
            (
                Text("Key", style='underline') if Text is not None else
                colored("Key", style='underline')
            ) + (Text('\n\n  ') if Text is not None else '\n\n  ') +
            (
                Text("Connector", style=styles['connector']) if Text is not None else
                colored("Connector", style=styles['connector'])
            ) + (Text('\n   +-- ') if Text is not None else '\n   +-- ') +
            (
                Text("Metric", style=styles['metric']) if Text is not None else
                colored("Metric", style=styles['metric'])
            ) + (Text('\n       +-- ') if Text is not None else '\n       +-- ') +
            (
                Text("Location", style=styles['location']) if Text is not None else
                colored("Location", style=styles['location'])
            ) + (Text('\n\n') if Text is not None else '\n\n')
        )

        output += str(key_str)
        cols.append(key_str)

        def replace_tree_text(tree_str : str) -> Text:
            """Replace the colored words with stylized Text instead.
            Is not executed if ANSI and UNICODE are disabled."""
            tree_text = Text(tree_str) if Text is not None else None
            for k, v in replace_dict.items():
                for _colored, _text in v.items():
                    parts = []
                    lines = tree_text.split(_colored)
                    for part in lines:
                        parts += [part, _text]
                    if lines[-1] != Text(''):
                        parts = parts[:-1]
                    _tree_text = Text('')
                    for part in parts:
                        _tree_text += part
                    tree_text = _tree_text
            return tree_text

        tree_output = ""
        for k, v in ascii_dict.items():
            branch = {k : v}
            tree_output += tree(branch) + '\n\n'
            if not UNICODE and not ANSI:
                _col = (Text(tree(branch)) if Text is not None else tree(branch))
            else:
                _col = replace_tree_text(tree(branch))
            cols.append(_col)
        if len(output) > 0:
            tree_output = tree_output[:-2]
        output += tree_output

        if rich is None:
            return print(output)

        rich_columns = attempt_import('rich.columns')
        Columns = rich_columns.Columns
        columns = Columns(cols)
        get_console().print(columns)

    if not UNICODE:
        return ascii_print_pipes()

    rich_panel, rich_tree, rich_text, rich_columns, rich_table = attempt_import(
        'rich.panel',
        'rich.tree',
        'rich.text',
        'rich.columns',
        'rich.table',
    )
    from rich import box
    Panel = rich_panel.Panel
    Tree = rich_tree.Tree
    Text = rich_text.Text
    Columns = rich_columns.Columns
    Table = rich_table.Table

    key_panel = Panel(
        (
            Text("\n") +
            Text(icons['connector'] + "Connector", style=styles['connector']) + Text("\n\n") +
            Text(icons['metric'] + "Metric", style=styles['metric']) + Text("\n\n") +
            Text(icons['location'] + "Location", style=styles['location']) + Text("\n")
        ),
        title = Text(icons['key'] + "Keys", style=styles['guide']),
        border_style = styles['guide'],
        expand = True
    )

    cols = []
    conn_trees = {}
    metric_trees = {}
    pipes = sorted_dict(pipes)
    for conn_keys, metrics in pipes.items():
        conn_trees[conn_keys] = Tree(
            Text(
                icons['connector'] + conn_keys,
                style = styles['connector'],
            ),
            guide_style = styles['connector']
        )
        metric_trees[conn_keys] = {}
        for metric, locations in metrics.items():
            metric_trees[conn_keys][metric] = Tree(
                Text(
                    icons['metric'] + metric,
                    style = styles['metric']
                ),
                guide_style = styles['metric']
            )
            conn_trees[conn_keys].add(metric_trees[conn_keys][metric])
            for location, pipe in locations.items():
                _location = (
                    Text(str(location), style=styles['none']) if location is None
                    else Text(location, style=styles['location'])
                )
                _location = (
                    Text(icons['location'])
                    + _location + Text('\n')
                    + pipe_repr(pipe, as_rich_text=True) + Text('\n')
                )
                metric_trees[conn_keys][metric].add(_location)

    cols += [key_panel]
    for k, t in conn_trees.items():
        cols.append(t)

    columns = Columns(cols)
    get_console().print(columns)

def pprint_pipe_columns(
        pipe: meerschaum.Pipe,
        nopretty: bool = False,
        debug: bool = False,
    ) -> None:
    """Pretty-print a pipe's columns."""
    import json
    from meerschaum.utils.warnings import info
    from meerschaum.utils.formatting import pprint, print_tuple, get_console
    from meerschaum.utils.formatting._shell import make_header
    from meerschaum.utils.packages import attempt_import, import_rich

    exists = pipe.exists(debug=debug)
    _cols = pipe.columns if exists else {}
    _cols_types = pipe.get_columns_types(debug=debug) if exists else {}

    def _nopretty_print():
        print(json.dumps(pipe.__getstate__()))
        print(json.dumps(_cols))
        print(json.dumps(_cols_types))
        print(json.dumps(pipe.dtypes))

    def _pretty_print():
        rich = import_rich()
        rich_table = attempt_import('rich.table')

        table = rich_table.Table(title=f"Data Types for {pipe}")
        table.add_column('Column')
        table.add_column('DB Type', justify='right')
        table.add_column('PD Type', justify='left')

        info(make_header(f"\nIndex Columns for {pipe}:"), icon=False)
        if _cols:
            pprint(_cols, nopretty=nopretty)
            print()
        else:
            print_tuple((False, f"No registered columns for {pipe}."))

        for c, t in pipe.dtypes.items():
            table.add_row(c, _cols_types.get(c, None), t)

        if pipe.dtypes:
            get_console().print(table)
        else:
            print_tuple((False, f"No table columns for {pipe}. Does the pipe exist?"))

    if nopretty:
        _nopretty_print()
    else:
        _pretty_print()


def pipe_repr(
        pipe: 'meerschaum.Pipe',
        as_rich_text: bool=False,
        ansi: Optional[bool] = None,
    ) -> Union[str, 'rich.text.Text']:
    """
    Return a formatted string for representing a `meerschaum.Pipe`.
    """
    from meerschaum.config import get_config
    from meerschaum.utils.formatting import UNICODE, ANSI, CHARSET, colored, rich_text_to_str
    from meerschaum.utils.packages import import_rich, attempt_import
    rich = import_rich()
    Text = attempt_import('rich.text').Text

    styles = get_config('formatting', 'pipes', '__repr__', 'ansi', 'styles')
    if not ANSI or (ansi is False):
        styles = {k: '' for k in styles}
    _pipe_style_prefix, _pipe_style_suffix = (
        (("[" + styles['Pipe'] + "]"), ("[/" + styles['Pipe'] + "]")) if styles['Pipe']
        else ('', '')
    )
    text_obj = (
        Text.from_markup(_pipe_style_prefix + "Pipe(" + _pipe_style_suffix)
        + colored(("'" + pipe.connector_keys + "'"), style=styles['connector'], as_rich_text=True)
        + Text.from_markup(_pipe_style_prefix + ", " + _pipe_style_suffix)
        + colored(("'" + pipe.metric_key + "'"), style=styles['metric'], as_rich_text=True)
        + (
            (
                colored(', ', style=styles['punctuation'], as_rich_text=True)
                + colored(
                    ("'" + pipe.location_key + "'"),
                    style=styles['location'], as_rich_text=True
                )
            ) if pipe.location_key is not None
            else colored('', style='', as_rich_text=True)
        ) + (
            ( ### Add the `instance=` argument.
                colored(', instance=', style=styles['punctuation'], as_rich_text=True)
                + colored(
                    ("'" + pipe.instance_keys + "'"),
                    style=styles['instance'], as_rich_text=True
                )
            ) if pipe.instance_keys != get_config('meerschaum', 'instance')
            else colored('', style='', as_rich_text=True)
        )
        + Text.from_markup(_pipe_style_prefix + ")" + _pipe_style_suffix)
    )
    if as_rich_text:
        return text_obj
    return rich_text_to_str(text_obj)


def highlight_pipes(message: str) -> str:
    """
    Add syntax highlighting to an info message containing stringified `meerschaum.Pipe` objects.
    """
    if 'Pipe(' not in message or ')' not in message:
        return message
    from meerschaum import Pipe
    segments = message.split('Pipe(')
    msg = ''
    _d = {}
    for i, segment in enumerate(segments):
        if ',' in segment and ')' in segment:
            paren_index = segment.find(')') + 1
            code = "_d['pipe'] = Pipe(" + segment[:paren_index]
            try:
                exec(code)
                _to_add = pipe_repr(_d['pipe']) + segment[paren_index:]
            except Exception as e:
                _to_add = 'Pipe(' + segment
            msg += _to_add
            continue
        msg += segment
    return msg

