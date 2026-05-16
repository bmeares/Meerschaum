#! /usr/bin/env python3
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

"""
Functions for copying elements.
"""

from __future__ import annotations
from meerschaum.utils.typing import Any, SuccessTuple, Optional, List


def copy(
    action: Optional[List[str]] = None,
    **kw : Any
) -> SuccessTuple:
    """
    Duplicate connectors or pipes.
    
    Command:
        `copy {pipes, connectors}`
    
    Example:
        `copy pipes`

    """
    from meerschaum.actions import choose_subaction
    options = {
        'pipes'      : _copy_pipes,
        'connectors' : _copy_connectors,
    }
    return choose_subaction(action, options, **kw)


def _complete_copy(
    action: Optional[List[str]] = None,
    **kw: Any
) -> List[str]:
    """
    Override the default Meerschaum `complete_` function.
    """
    if action is None:
        action = []

    options = {
        'connector': _complete_copy_connectors,
        'connectors': _complete_copy_connectors,
    }

    if (
        len(action) > 0 and action[0] in options
            and kw.get('line', '').split(' ')[-1] != action[0]
    ):
        sub = action[0]
        del action[0]
        return options[sub](action=action, **kw)

    from meerschaum._internal.shell import default_action_completer
    return default_action_completer(action=(['copy'] + action), **kw)


def _copy_pipes(
    yes: bool = False,
    noask: bool = False,
    force: bool = False,
    debug: bool = False,
    **kw
) -> SuccessTuple:
    """
    Copy pipes' attributes and make new pipes.
    """
    from meerschaum import get_pipes, Pipe
    from meerschaum.connectors import instance_types
    from meerschaum.utils.prompt import prompt, yes_no, get_connectors_completer, choose
    from meerschaum.utils.warnings import warn, info
    from meerschaum.utils.formatting import print_tuple
    from meerschaum.utils.formatting._shell import clear_screen
    from meerschaum.utils.formatting._pipes import pprint_pipes
    from meerschaum.utils.pipes import pipes_dict_from_list
    pipes = get_pipes(as_list=True, cache=False, debug=debug, **kw)
    change_instance_only = False
    instance_keys = None
    if len(pipes) > 1:
        clear_screen(debug=debug)
        pprint_pipes(pipes_dict_from_list(pipes))
        if force or yes_no(
            f"Change only the instance keys for copies of the above {len(pipes)} pipes?",
            noask=noask,
            yes=yes,
            default='y',
        ):
            instance_keys = prompt(
                "Instance keys for the new pipes:",
                completer=get_connectors_completer(*instance_types),
            )
            change_instance_only = True
    batch_sync_choice = None
    batch_filter_kw = {}
    overwrite_existing = None
    if change_instance_only:
        prospective_pipes = [
            Pipe(p.connector_keys, p.metric_key, p.location_key, instance=instance_keys, cache=False)
            for p in pipes
        ]
        existing_new_pipes = [p for p in prospective_pipes if p.id is not None]
        new_only_pipes = [p for p in prospective_pipes if p.id is None]
        all_exist = not new_only_pipes

        if existing_new_pipes and not force:
            clear_screen(debug=debug)
            pprint_pipes(pipes_dict_from_list(existing_new_pipes))
            _n = len(existing_new_pipes)
            _total = len(pipes)
            _pipe_word = 'pipe' + ('s' if _n != 1 else '')

            if all_exist:
                overwrite_existing = choose(
                    f"All {_total} selected {_pipe_word} already exist on {instance_keys}."
                    "\n    What should be copied?",
                    [
                        ('skip', "Nothing — skip all pipes."),
                        ('data_only', "Data only (keep existing attributes)."),
                        ('attrs_only', "Attributes only (no data sync)."),
                        ('overwrite', "Attributes and data."),
                    ],
                    default='skip',
                    noask=noask,
                    as_indices=True,
                    numeric=True,
                )
                if overwrite_existing in ('skip', 'attrs_only'):
                    batch_sync_choice = 'nothing'
            else:
                overwrite_existing = choose(
                    f"{_n} of the {_total} selected {_pipe_word} already exist on {instance_keys}."
                    "\n    How should conflicts be resolved?",
                    [
                        ('skip', "Skip existing pipes — only register and sync the new ones."),
                        ('data_only', "Copy data into existing pipes without updating their attributes."),
                        ('overwrite', "Copy attributes and data into existing pipes."),
                    ],
                    default='skip',
                    noask=noask,
                    as_indices=True,
                    numeric=True,
                )
        else:
            overwrite_existing = 'overwrite'

        if batch_sync_choice is None:
            _data_committed = overwrite_existing in ('data_only', 'overwrite')
            _data_options = [
                ('backtrack', 'Recent data only (within the configured backtrack window).'),
                ('all', 'All available data from the source pipes.'),
                ('filter', 'Data matching specific filters (begin, end, params).'),
            ]
            if not _data_committed:
                _data_options.insert(0, ('nothing', 'No data.'))
            batch_sync_choice = choose(
                "What data should be copied?",
                _data_options,
                default='nothing' if not _data_committed else 'backtrack',
                noask=noask,
                as_indices=True,
                numeric=True,
            )
            if batch_sync_choice == 'filter':
                from meerschaum._internal.arguments._parse_arguments import parse_line
                filter_line = prompt(
                    "Enter filter flags"
                    " (e.g. --begin 2024-01-01 --end 2024-12-31 --params '{\"foo\": \"bar\"}'):",
                    noask=noask,
                )
                parsed = parse_line(filter_line)
                for key in ('begin', 'end', 'params'):
                    if key in parsed:
                        batch_filter_kw[key] = parsed[key]

    import time
    successes = 0
    sync_stats = []  # (new_pipe, elapsed_seconds, stats_dict, success)
    for i, pipe in enumerate(pipes):
        info(f"Copying {pipe} ({i+1} / {len(pipes)})...")
        ck = prompt(
            f"Connector keys for copy of {pipe}:",
            default=pipe.connector_keys,
            completer=get_connectors_completer(),
        ) if not change_instance_only else pipe.connector_keys
        mk = (
            prompt(f"Metric key for copy of {pipe}:", default=pipe.metric_key)
            if not change_instance_only else pipe.metric_key
        )
        lk = prompt(
            f"Location key for copy of {pipe} ('None' to omit):",
            default=str(pipe.location_key),
        ) if not change_instance_only else str(pipe.location_key)
        if lk in ('', 'None', '[None]'):
            lk = None

        instance_keys = prompt(
            f"Meerschaum instance for copy of {pipe}:",
            default=pipe.instance_keys,
            completer=get_connectors_completer(*instance_types),
        ) if not change_instance_only else instance_keys
        new_pipe = Pipe(
            ck, mk, lk,
            instance=instance_keys,
            parameters=pipe.get_parameters(apply_symlinks=False),
            cache=False,
        )

        if new_pipe.id is not None:
            if overwrite_existing is None:
                warn(f"{new_pipe} already exists on '{instance_keys}'.", stack=False)
            if overwrite_existing is not None:
                _existing_action = overwrite_existing
            else:
                _existing_action = choose(
                    f"How would you like to handle {new_pipe}?",
                    [
                        (
                            'skip',
                            "Skip this pipe entirely — do not update its attributes or copy any data into it.",
                        ),
                        (
                            'data_only',
                            (
                                "Copy data into this pipe without changing its stored"
                                " attributes (parameters, tags, target, etc.)."
                            ),
                        ),
                        (
                            'overwrite',
                            (
                                "Update this pipe — overwrite its stored attributes"
                                " (parameters, tags, target, etc.) with those from the source,"
                                " and copy data."
                            ),
                        ),
                    ],
                    default='skip',
                    noask=noask,
                    as_indices=True,
                    numeric=True,
                )
                if force:
                    _existing_action = 'overwrite'
            if _existing_action == 'skip':
                info(f"Skipping {new_pipe} — leaving destination pipe unchanged.")
                continue
            if _existing_action in ('overwrite', 'attrs_only'):
                info(f"Updating attributes of existing {new_pipe} from {pipe}...")
                edit_success, edit_msg = new_pipe.edit(debug=debug)
                if not edit_success:
                    warn(f"Failed to copy attributes to {new_pipe}:\n{edit_msg}", stack=False)
                    continue
            else:
                info(f"Keeping existing attributes of {new_pipe} — will copy data only.")
        else:
            register_success, register_msg = new_pipe.register(debug=debug)
            if not register_success:
                warn(f"Failed to register new {new_pipe}:\n{register_msg}", stack=False)
                continue

        successes += 1
        print_tuple(
            (True, f"Successfully copied {pipe}" + f" to {new_pipe}.")
        )
        if pipe.exists(debug=debug):
            if batch_sync_choice is not None:
                sync_choice = batch_sync_choice
                filter_kw = batch_filter_kw
            else:
                sync_choice = choose(
                    "What data should be copied?",
                    [
                        ('nothing', 'No data.'),
                        ('backtrack', 'Recent data only (within the configured backtrack window).'),
                        ('all', 'All available data from the source pipe.'),
                        ('filter', 'Data matching specific filters (begin, end, params).'),
                    ],
                    default='nothing',
                    noask=noask,
                    as_indices=True,
                    numeric=True,
                )
                filter_kw = {}
                if sync_choice == 'filter':
                    from meerschaum._internal.arguments._parse_arguments import parse_line
                    filter_line = prompt(
                        "Enter filter flags"
                        " (e.g. --begin 2024-01-01 --end 2024-12-31 --params '{\"foo\": \"bar\"}'):",
                        noask=noask,
                    )
                    parsed = parse_line(filter_line)
                    for key in ('begin', 'end', 'params'):
                        if key in parsed:
                            filter_kw[key] = parsed[key]

            if sync_choice != 'nothing':
                info(f"Fetching data from {pipe}...")
                _sync_start = time.monotonic()
                sync_data = (
                    pipe.get_backtrack_data(debug=debug)
                    if sync_choice == 'backtrack'
                    else pipe.get_data(debug=debug, as_iterator=True, **filter_kw)
                )
                info(f"Syncing into {new_pipe}...")
                sync_success, sync_msg = new_pipe.sync(sync_data, debug=debug, **kw)
                _sync_elapsed = time.monotonic() - _sync_start
                print_tuple((sync_success, sync_msg))
                from meerschaum.utils.formatting._pipes import extract_stats_from_message
                sync_stats.append((
                    new_pipe,
                    _sync_elapsed,
                    extract_stats_from_message(sync_msg),
                    sync_success,
                ))
                if not sync_success:
                    warn(f"Failed to copy data from {pipe} to {new_pipe}.", stack=False)
                    successes -= 1

    if sync_stats:
        from meerschaum.utils.packages import attempt_import, import_rich
        from meerschaum.utils.formatting import get_console, ANSI, UNICODE
        import_rich()
        rich_table = attempt_import('rich.table')
        rich_text = attempt_import('rich.text')
        rich_box = attempt_import('rich.box')
        Table = rich_table.Table
        Text = rich_text.Text

        def _fmt_dur(s: float) -> str:
            if s < 60:
                return f"{s:.1f}s"
            m, sec = divmod(int(s), 60)
            if m < 60:
                return f"{m}m{sec:02d}s"
            h, m = divmod(m, 60)
            return f"{h}h{m:02d}m{sec:02d}s"

        total_inserted = sum(r[2].get('inserted', 0) for r in sync_stats)
        total_updated = sum(r[2].get('updated', 0) for r in sync_stats)
        total_upserted = sum(r[2].get('upserted', 0) for r in sync_stats)
        total_elapsed = sum(r[1] for r in sync_stats)

        _style = lambda s: (s if ANSI else '')
        table = Table(
            box=(rich_box.MINIMAL if UNICODE else rich_box.ASCII),
            show_footer=True,
            title=Text("Copy Summary", style=_style("bold")),
        )
        table.add_column(
            "Pipe",
            footer=Text("Total", style=_style("bold")),
            no_wrap=True,
        )
        table.add_column(
            "Inserted",
            footer=Text(f"{total_inserted:,}", style=_style("bold")),
            justify="right",
        )
        table.add_column(
            "Updated",
            footer=Text(f"{total_updated:,}", style=_style("bold")),
            justify="right",
        )
        table.add_column(
            "Upserted",
            footer=Text(f"{total_upserted:,}", style=_style("bold")),
            justify="right",
        )
        table.add_column(
            "Duration",
            footer=Text(_fmt_dur(total_elapsed), style=_style("bold")),
            justify="right",
        )
        for new_pipe, elapsed, stats, success in sync_stats:
            _row_style = _style("" if success else "red")
            table.add_row(
                Text(str(new_pipe), style=_row_style),
                Text(f"{stats.get('inserted', 0):,}", style=_row_style),
                Text(f"{stats.get('updated', 0):,}", style=_row_style),
                Text(f"{stats.get('upserted', 0):,}", style=_row_style),
                Text(_fmt_dur(elapsed), style=_row_style),
            )
        get_console().print(table)

    msg = (
        "No pipes were copied." if successes == 0
        else (f"Copied {successes} pipe" + ("s" if successes != 1 else '') + '.')
    )

    return successes > 0, msg


def _copy_connectors(
    action: Optional[List[str]] = None,
    connector_keys: Optional[List[str]] = None,
    nopretty: bool = False,
    force: bool = False,
    debug: bool = False,
    **kwargs: Any
) -> SuccessTuple:
    """
    Create a new connector from an existing one.
    """
    from meerschaum.utils.prompt import yes_no, prompt
    from meerschaum.connectors.parse import parse_connector_keys
    from meerschaum.config import _config, get_config
    from meerschaum.utils.warnings import info
    from meerschaum.utils.formatting import pprint
    from meerschaum.actions import get_action
    _ = _config()
    if action is None:
        action = []
    if connector_keys is None:
        connector_keys = []

    _keys = (action or []) + connector_keys

    if not _keys:
        return False, "No connectors to copy."

    if len(_keys) < 1 or len(_keys) > 2:
        return False, "Provide one set of connector keys."

    ck = _keys[0]

    try:
        conn = parse_connector_keys(ck)
    except Exception:
        return False, f"Unable to parse connector '{ck}'."

    if len(_keys) == 2:
        new_ck = _keys[1] if ':' in _keys[1] else None
        new_label = _keys[1].split(':')[-1]
    else:
        new_ck = None
        new_label = None

    try:
        if new_label is None:
            new_label = prompt(f"Enter a label for the new '{conn.type}' connector:")
    except KeyboardInterrupt:
        return False, "Nothing was copied."

    if new_ck is None:
        new_ck = f"{conn.type}:{new_label}"

    info(f"Registering connector '{new_ck}' from '{ck}'...")

    attrs = get_config('meerschaum', 'connectors', conn.type, conn.label)
    pprint(attrs, nopretty=nopretty)
    if not force and not yes_no(
        f"Register connector '{new_ck}' with the above attributes?",
        default='n',
        **kwargs
    ):
        return False, "Nothing was copied."

    register_connector = get_action(['register', 'connector'])
    register_success, register_msg = register_connector(
        [new_ck],
        params=attrs,
        **kwargs
    )
    return register_success, register_msg


def _complete_copy_connectors(
    action: Optional[List[str]] = None,
    line: str = '',
    **kw: Any
) -> List[str]:
    from meerschaum.config import get_config
    from meerschaum.utils.misc import get_connector_labels
    types = list(get_config('meerschaum', 'connectors').keys())
    if line.split(' ')[-1] == '' or not action:
        search_term = ''
    else:
        search_term = action[-1]
    return get_connector_labels(*types, search_term=search_term)


### NOTE: This must be the final statement of the module.
###       Any subactions added below these lines will not
###       be added to the `help` docstring.
from meerschaum.utils.misc import choices_docstring as _choices_docstring
copy.__doc__ += _choices_docstring('copy')
