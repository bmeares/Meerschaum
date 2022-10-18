#! /usr/bin/env python3
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

"""
Interactive "wizards" to guide the user.
"""

from __future__ import annotations

def select_pipes(
        yes: bool = False,
        force: bool = False,
        debug: bool = False,
    ) -> List[Pipe]:
    """Prompt the user for the keys to identify a list of pipes.

    Parameters
    ----------
    yes: bool :
         (Default value = False)
    force: bool :
         (Default value = False)
    debug: bool :
         (Default value = False)

    Returns
    -------

    """
    from meerschaum.utils.misc import get_connector_labels
    from meerschaum.utils.prompt import prompt, choose, yes_no
    from meerschaum.utils.get_pipes import get_pipes
    from meerschaum.utils.formatting._shell import clear_screen
    from meerschaum.utils.formatting import pprint_pipes
    from meerschaum.config import get_config
    from meerschaum.utils.warnings import warn
    from meerschaum.utils.misc import flatten_pipes_dict
    from meerschaum.connectors import instance_types
    clear_screen(debug=debug)
    while True:
        instance = choose(
            "On which instance are the pipes stored?",
            get_connector_labels(*instance_types),
            numeric = True,
            default = get_config('meerschaum', 'instance'),
        )
        pipes_dict = get_pipes(instance=instance, debug=debug)
        if pipes_dict:
            break
        warn(f"There are no pipes registered on instance '{instance}'.", stack=False)
        print("    Please choose another instance.")

    conn_keys = sorted(list(pipes_dict.keys()))
    clear_screen(debug=debug)
    chosen_conn_keys = choose(
        "What are the connectors for the pipes?",
        conn_keys,
        numeric = True,
        multiple = True,
        default = (conn_keys[0] if len(conn_keys) == 1 else None)
    )

    pipes_dict = get_pipes(chosen_conn_keys, instance=instance, debug=debug)
    metric_keys = []
    for ck, metrics in pipes_dict.items():
        for mk in metrics:
            metric_keys.append(mk)
    metric_keys = sorted(metric_keys)
    clear_screen(debug=debug)
    chosen_metric_keys = choose(
        "What are the metrics for the pipes?",
        metric_keys,
        numeric = True,
        multiple = True,
        default = (metric_keys[0] if len(metric_keys) == 1 else None)
    )
   
    pipes_dict = get_pipes(chosen_conn_keys, chosen_metric_keys, instance=instance, debug=debug)
    location_keys = []
    for ck, metrics in pipes_dict.items():
        for mk, locations in metrics.items():
            for lk, p in locations.items():
                location_keys.append(str(lk))
    location_keys = sorted(location_keys)
    clear_screen(debug=debug)
    chosen_location_keys = [(lk if lk != 'None' else None) for lk in choose(
        "What are the locations for the pipes?",
        location_keys,
        numeric = True,
        multiple = True,
        default = (location_keys[0] if len(location_keys) == 1 else None)
    )]

    pipes_dict = get_pipes(
        chosen_conn_keys,
        chosen_metric_keys,
        chosen_location_keys,
        instance = instance,
        debug = debug,
    )
    clear_screen(debug=debug)
    pprint_pipes(pipes_dict)
    if force or yes_no("Choose these pipes?", yes=yes):
        return flatten_pipes_dict(pipes_dict)
    return select_pipes(yes=yes, force=force, debug=debug)

