#! /usr/bin/env python
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

"""
Functions for bootstrapping elements
(pipes, configuration, etc)
"""

from __future__ import annotations

import meerschaum as mrsm
from meerschaum.utils.typing import Union, Any, Sequence, SuccessTuple, Optional, Tuple, List


def bootstrap(
    action: Optional[List[str]] = None,
    **kw: Any
) -> SuccessTuple:
    """
    Launch an interactive wizard to bootstrap pipes or connectors.

    Example:
        `bootstrap pipes`

    """
    from meerschaum.actions import choose_subaction
    options = {
        'pipes'      : _bootstrap_pipes,
        'connectors' : _bootstrap_connectors,
        'plugins'    : _bootstrap_plugins,
        'jobs'       : _bootstrap_jobs,
    }
    return choose_subaction(action, options, **kw)


def _bootstrap_pipes(
    action: Optional[List[str]] = None,
    connector_keys: Optional[List[str]] = None,
    metric_keys: Optional[List[str]] = None,
    location_keys: Optional[List[Optional[str]]] = None,
    yes: bool = False,
    force: bool = False,
    noask: bool = False,
    debug: bool = False,
    mrsm_instance: Optional[str] = None,
    shell: bool = False,
    **kw: Any
) -> SuccessTuple:
    """
    Create a new pipe.
    If no keys are provided, guide the user through the steps required.

    """
    from meerschaum import get_pipes
    from meerschaum.config import get_config
    from meerschaum.utils.warnings import info, warn
    from meerschaum.utils.prompt import yes_no, prompt, choose
    from meerschaum.connectors.parse import parse_instance_keys
    from meerschaum.utils.misc import get_connector_labels
    from meerschaum.utils.formatting._shell import clear_screen

    instance_connector = parse_instance_keys(mrsm_instance)

    if connector_keys is None:
        connector_keys = []
    if metric_keys is None:
        metric_keys = []
    if location_keys is None:
        location_keys = []

    _clear = get_config('shell', 'clear_screen', patch=True)
    abort_tuple = (False, "No pipes were bootstrapped.")

    if (
        len(connector_keys) > 0 and
        len(metric_keys) > 0
    ):
        info(
            "You've provided the following keys:\n" +
            "  - Connector Keys: " + str(connector_keys) + "\n" +
            "  - Metric Keys: " + str(metric_keys) + "\n" +
            (
                ("  - Location Keys: " + str(location_keys) + "\n")
                if len(location_keys) > 0 else ""
            )
        )
        if not force:
            try:
                if not yes_no(
                    ("Would you like to bootstrap pipes with these keys?\n"
                    + "Existing pipes will be deleted!"),
                    default = 'n',
                    yes = yes,
                    noask = noask
                ):
                    return abort_tuple
            except KeyboardInterrupt:
                return abort_tuple
    else:
        ### Get the connector.
        new_label = 'New'
        info(
            "To create a pipe without explicitly using a connector, "
            + "use the `register pipes` command.\n"
        )
        try:
            ck = choose(
                (
                    "Where are the data coming from?\n\n" +
                    f"    Please type the keys of a connector or enter '{new_label}'\n" +
                    "    to register a new connector.\n\n" +
                    f" {get_config('formatting', 'emoji', 'connector')} Connector:"
                ),
                get_connector_labels() + [new_label],
                numeric = False,
            )
        except KeyboardInterrupt:
            return abort_tuple

        if ck == new_label:
            if _clear:
                clear_screen(debug=debug)
            while True:
                tup = _bootstrap_connectors(
                    [], yes=yes, force=force, debug=debug, return_keys=True, **kw
                )
                if isinstance(tup[0], str):
                    if _clear:
                        clear_screen(debug=debug)
                    ck = tup[0] + ':' + tup[1]
                    break
                elif isinstance(tup[0], bool) and not tup[0]:
                    return abort_tuple
                warn("Please register a new connector or press CTRL+C to cancel.", stack=False)
        connector_keys = [ck]

        ### Get the metric.
        while True:
            if _clear:
                clear_screen(debug=debug)
            try:
                mk = prompt(
                    "What kind of data is this?\n\n" +
                    "    The metric is the label for the contents of the pipe.\n" +
                    "    For example, 'weather' might be a metric for weather station data.\n\n" +
                    f" {get_config('formatting', 'emoji', 'metric')} Metric:"
                )
            except KeyboardInterrupt:
                return abort_tuple
            if mk:
                break
            warn("Please enter a metric.", stack=False)
        metric_keys = [mk]

        ### Get the location
        if _clear:
            clear_screen(debug=debug)
        try:
            lk = prompt(
                "Where are the data located?\n\n"
                + "    You can build pipes that share a connector and metric\n"
                + "    but are in different locations.\n\n"
                + f"    For example, you could create Pipe('{ck}', '{mk}', 'home') and\n"
                + f"    Pipe('{ck}', '{mk}', 'work'), which would share a connector\n"
                + "    and metric, but may come from different tables.\n\n"
                + "    In most cases, you can omit the location.\n\n"
                + f" {get_config('formatting', 'emoji', 'location')} Location (Empty to omit):"
            )
        except KeyboardInterrupt:
            return abort_tuple
        lk = None if lk == '' else lk
        location_keys = [lk]

    if _clear:
        clear_screen(debug=debug)
    _pipes = get_pipes(
        connector_keys, metric_keys, location_keys,
        method = 'explicit',
        as_list = True,
        debug = debug,
        mrsm_instance = instance_connector,
        **kw
    )
    pipes = []
    for p in _pipes:
        if p.get_id(debug=debug) is not None and not force:
            try:
                if not yes_no(
                    f"{p} already exists.\n\n    Delete {p}?\n    Data will be lost!",
                    default='n'
                ):
                    info(f"Skipping bootstrapping {p}...")
                    continue
            except KeyboardInterrupt:
                return abort_tuple
        pipes.append(p)

    if len(pipes) == 0:
        return abort_tuple

    success_dict = {}
    successes, failures = 0, 0
    for p in pipes:
        try:
            tup = p.bootstrap(
                interactive=True, force=force, noask=noask, yes=yes, shell=shell, debug=debug
            )
        except Exception as e:
            import traceback
            traceback.print_exc()
            tup = False, f"Failed to bootstrap {p} with exception:\n" + str(e)
        success_dict[p] = tup
        if tup[0]:
            successes += 1
        else:
            failures += 1

    msg = (
        f"Finished bootstrapping {len(pipes)} pipe" + ("s" if len(pipes) != 1 else "") + "\n" +
        f"    ({successes} succeeded, {failures} failed)."
    )

    return (successes > 0), msg


def _bootstrap_connectors(
    action: Optional[List[str]] = None,
    connector_keys: Optional[List[str]] = None,
    yes: bool = False,
    force: bool = False,
    noask: bool = False,
    debug: bool = False,
    return_keys: bool = False,
    **kw: Any
) -> Union[SuccessTuple, Tuple[str, str]]:
    """
    Prompt the user for the details necessary to create a Connector.
    """
    from meerschaum.connectors import (
        connectors,
        get_connector,
        types,
        custom_types,
        _load_builtin_custom_connectors,
    )
    from meerschaum.utils.prompt import prompt, yes_no, choose
    from meerschaum.config import get_config
    from meerschaum.config._edit import write_config
    from meerschaum.utils.formatting import pprint
    from meerschaum.utils.formatting._shell import clear_screen
    from meerschaum.connectors import attributes as connector_attributes
    from meerschaum.utils.warnings import warn, info
    from meerschaum.utils.misc import is_int

    abort_tuple = False, "No connectors bootstrapped."
    _clear = get_config('shell', 'clear_screen', patch=True)
    _load_builtin_custom_connectors()

    if action is None:
        action = []
    if connector_keys is None:
        connector_keys = []

    if len(connector_keys) == 0:
        pass

    try:
        _type = choose(
            (
                'Please choose a connector type.\n'
                + '    See https://meerschaum.io/reference/connectors '
                + 'for documentation on connectors.\n'
            ),
            sorted([k for k in connectors if k != 'plugin']),
            default='sql',
        )
    except KeyboardInterrupt:
        return abort_tuple

    if _clear:
        clear_screen(debug=debug)

    existing_labels = get_config('meerschaum', 'connectors', _type, warn=False) or []
    _label_choices = sorted([
        label
        for label in existing_labels
        if label != 'default' and label is not None
    ])
    new_connector_label = 'New connector'
    _label_choices.append(new_connector_label)
    while True:
        try:
            _label = prompt(f"New label for '{_type}' connector:")
        except KeyboardInterrupt:
            return abort_tuple
        if _label in existing_labels:
            warn(f"Connector '{_type}:{_label}' already exists.", stack=False)
            overwrite = yes_no(
                f"Do you want to overwrite connector '{_type}:{_label}'?",
                default='n',
                yes=yes,
                noask=noask,
            )
            if not overwrite and not force:
                return False, "No changes made to connector configuration."
            break
        elif _label == "":
            warn("Please enter a label.", stack=False)
        else:
            break

    cls = types.get(_type)
    cls_required_attrs = getattr(cls, 'REQUIRED_ATTRIBUTES', [])
    cls_optional_attrs = getattr(cls, 'OPTIONAL_ATTRIBUTES', [])
    cls_default_attrs = getattr(cls, 'DEFAULT_ATTRIBUTES', {})
    type_attributes = connector_attributes.get(
        _type,
        {
            'required': cls_required_attrs,
            'optional': cls_optional_attrs,
            'default': cls_default_attrs,
        }
    )

    new_attributes = {}
    if 'flavors' in type_attributes:
        try:
            flavor = choose(
                f"Flavor for connector '{_type}:{_label}':",
                sorted(list(type_attributes['flavors'])),
                default = (
                    'timescaledb'
                    if 'timescaledb' in type_attributes['flavors']
                    else None
                )
            )
        except KeyboardInterrupt:
            return abort_tuple
        new_attributes['flavor'] = flavor
        required = sorted(list(connector_attributes[_type]['flavors'][flavor]['requirements']))
        optional = sorted(list(connector_attributes[_type]['flavors'][flavor].get('optional', {})))
        default = type_attributes['flavors'][flavor].get('defaults', {})
    else:
        required = sorted(list(type_attributes.get('required', {})))
        optional = sorted(list(type_attributes.get('optional', {})))
        default = type_attributes.get('default', {})
    info(
        f"Please answer the following questions to configure the new connector '{_type}:{_label}'."
        + '\n' + "Press [Ctrl + C] to skip."
    )
    for r in required:
        try:
            default_val = str(default.get(r)) if r in default else None
            val = prompt(f"Value for {r}:", default=default_val)
        except KeyboardInterrupt:
            continue
        if is_int(val):
            val = int(val)
        new_attributes[r] = val

    for o in optional:
        try:
            val = prompt(f"Value for {o} (optional; empty to omit):")
        except KeyboardInterrupt:
            continue
        if val:
            new_attributes[o] = val

    for k, v in default.items():
        ### skip already configured attributes, (e.g. flavor or from required)
        if k in new_attributes:
            continue
        try:
            val = prompt(f"Value for {k}:", default=str(v))
        except KeyboardInterrupt:
            continue
        if is_int(val):
            val = int(val)
        new_attributes[k] = val

    if _clear:
        clear_screen(debug=debug)
    try:
        conn = get_connector(_type, _label, **new_attributes)
    except Exception as e:
        return False, f"Failed to bootstrap connector '{_type}:{_label}' with exception:\n{e}"

    pprint(new_attributes)
    try:
        ok = (
            yes_no(
                f"Are you ok with these new attributes for connector '{conn}'?",
                default = 'y',
                noask = noask,
                yes = yes
            ) if not yes else yes
        )
    except KeyboardInterrupt:
        ok = False
    if not ok:
        return False, "No changes made to connectors configuration."

    meerschaum_config = get_config('meerschaum')

    if 'connectors' not in meerschaum_config:
        meerschaum_config['connectors'] = {}
    if _type not in meerschaum_config['connectors']:
        meerschaum_config['connectors'][_type] = {}
    meerschaum_config['connectors'][_type][_label] = new_attributes
    write_config({'meerschaum': meerschaum_config}, debug=debug)
    if return_keys:
        return _type, _label
    return True, "Success"


def _bootstrap_plugins(
    action: Optional[List[str]] = None,
    debug: bool = False,
    **kwargs: Any
) -> SuccessTuple:
    """
    Launch an interactive wizard to guide the user to creating a new plugin.
    """
    from meerschaum.utils.prompt import prompt
    from meerschaum.plugins.bootstrap import bootstrap_plugin

    if not action:
        action = [prompt("Enter the name of your new plugin:")]

    for plugin_name in action:
        bootstrap_success, bootstrap_msg = bootstrap_plugin(plugin_name)
        if not bootstrap_success:
            return bootstrap_success, bootstrap_msg

    return True, "Success"


def _bootstrap_jobs(
    action: Optional[List[str]] = None,
    executor_keys: Optional[str] = None,
    debug: bool = False,
    **kwargs: Any
) -> SuccessTuple:
    """
    Launch an interactive wizard to create new jobs.
    """
    import shlex
    from meerschaum.utils.prompt import prompt, yes_no
    from meerschaum.actions import actions
    from meerschaum.utils.formatting import print_options, make_header
    from meerschaum.utils.formatting._shell import clear_screen
    from meerschaum.utils.warnings import info
    from meerschaum._internal.arguments import (
        split_pipeline_sysargs,
        split_chained_sysargs,
    )
    from meerschaum.utils.misc import items_str
    from meerschaum._internal.shell.ShellCompleter import ShellCompleter

    if not action:
        action = [prompt("What is the name of the job you'd like to create?")]

    for name in action:
        clear_screen(debug=debug)
        job = mrsm.Job(name, executor_keys=executor_keys)
        if job.exists():
            edit_success, edit_msg = actions['edit'](['job', name], **kwargs)
            if not edit_success:
                return edit_success, edit_msg
            continue

        info(
            f"Editing arguments for job '{name}'.\n"
            "    Press [Esc + Enter] to submit, [CTRL + C] to exit.\n\n"
            "    Tip: join multiple actions with `+`, add pipeline arguments with `:`.\n"
            "    https://meerschaum.io/reference/actions/#chaining-actions\n"
        )
        try:
            new_sysargs_str = prompt(
                "",
                multiline=True,
                icon=False,
                completer=ShellCompleter(),
            )
        except KeyboardInterrupt:
            return True, "Nothing was changed."

        new_sysargs = shlex.split(new_sysargs_str)
        new_sysargs, pipeline_args = split_pipeline_sysargs(new_sysargs)
        chained_sysargs = split_chained_sysargs(new_sysargs)

        clear_screen(debug=debug)
        if len(chained_sysargs) > 1:
            print_options(
                [
                    shlex.join(step_sysargs)
                    for step_sysargs in chained_sysargs
                ],
                header=f"Steps in Job '{name}':",
                number_options=True,
                **kwargs
            )
        else:
            print('\n' + make_header(f"Action for Job '{name}':"))
            print(shlex.join(new_sysargs))

        if pipeline_args:
            print('\n' + make_header("Pipeline Arguments:"))
            print(shlex.join(pipeline_args))
            print()

        if not yes_no(
            (
                f"Are you sure you want to create job '{name}' with the above arguments?\n"
                + "    The job will be started if you continue."
            ),
            default='n',
            **kwargs
        ):
            return True, "Nothing was changed."

        new_job = mrsm.Job(name, new_sysargs_str, executor_keys=executor_keys)
        start_success, start_msg = new_job.start()
        if not start_success:
            return start_success, start_msg

    msg = (
        "Successfully bootstrapped job"
        + ('s' if len(action) != 1 else '')
        + ' '
        + items_str(action)
        + '.'
    )
    return True, msg


### NOTE: This must be the final statement of the module.
###       Any subactions added below these lines will not
###       be added to the `help` docstring.
from meerschaum.actions import choices_docstring as _choices_docstring
bootstrap.__doc__ += _choices_docstring('bootstrap')
