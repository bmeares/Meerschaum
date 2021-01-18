#! /usr/bin/env python
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

"""
Formatting functions for printing pipes
"""

def pprint_pipes(pipes : dict):
    """
    Build and print a rich.tree.Tree object from a pipes dictionary.
    """
    from meerschaum.utils.warnings import error
    from meerschaum.utils.packages import attempt_import
    from meerschaum.utils.misc import sorted_dict, replace_pipes_in_dict
    from meerschaum.utils.formatting import UNICODE, ANSI, pprint
    import copy

    def ascii_print_pipes():
        asciitree = attempt_import('asciitree')
        def _replace_pipe_ascii_tree(pipe):
            return {str(pipe) : {}}
        ascii_dict = replace_pipes_in_dict(pipes, _replace_pipe_ascii_tree)
        tree = asciitree.LeftAligned()
        output = ''
        for k, v in ascii_dict.items():
            output += tree(v) + '\n\n'
        if len(output) > 0: output = output[:-2]
        print(output)

    if not UNICODE:
        return ascii_print_pipes()
        #  return pprint(pipes, width=1, expand_all=True, indent_guides=False)
    rich = attempt_import('rich', warn=False)
    if rich is None:
        return pprint(pipes)

    from rich import box
    from rich.panel import Panel
    from rich.tree import Tree
    from rich.text import Text
    from rich.columns import Columns
    from rich.table import Table

    icons = {'connector' : '', 'metric' : '', 'location' : '', 'key' : ''}
    styles = {'connector' : '', 'metric' : '', 'location' : ''}
    guide_style, none_style = '', ''
    ### NOTE: unicode should always be true, at least until there's an ASCII way to print Trees
    if UNICODE:
        icons['connector'] = '🔌 '
        icons['metric'] = '📊 '
        icons['location'] = '📍 '
        icons['key'] = '🔑 '
        icons['info'] = '💡 '
    if ANSI:
        styles['connector'] = 'green'
        styles['metric'] = 'bright_blue'
        styles['location'] = 'magenta'
        guide_style = 'dim'
        none_style = 'black on magenta'

    key_table = Table(
        title=Text(icons['key'] + "Keys"),
        show_header=False, expand=False, style="", header_style="", border_style=""
    )
    key_table.add_column()
    #  key_table.add_column(justify='right')
    #  key_table.add_column(icons['key'] + "Keys")
    key_table.add_row(Text('\n' + icons['connector'] + "Connector", style=styles['connector']))
    key_table.add_row(Text('\n' + icons['metric'] + "Metric", style=styles['metric']))
    key_table.add_row(Text('\n' + icons['location'] + "Location\n", style=styles['location']))

    key_panel = Panel(
        (
            Text(icons['connector'] + "Connector", style=styles['connector']) + "\n"
        ),
        title = Text(icons['key'] + "Keys", style=guide_style),
        border_style = guide_style
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
        metric_trees[conn_keys] = dict()
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
                if location is None: _location = Text(str(location), style=none_style)
                else: _location = Text(location, style=styles['location'])
                _location = Text(icons['location']) + _location + Text("\n" + str(pipe) + "\n")
                metric_trees[conn_keys][metric].add(_location)

    cols += [key_table]
    for k, t in conn_trees.items():
        cols.append(t)

    columns = Columns(cols)
    rich.print(columns)

