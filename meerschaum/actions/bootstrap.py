#! /usr/bin/env python
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

"""
Functions for bootstrapping elements
(pipes, configuration, etc)
"""

from __future__ import annotations
from meerschaum.utils.typing import Union, Any, Sequence, SuccessTuple, Optional, Tuple, List

def bootstrap(
        action : Optional[List[str]] = None,
        **kw : Any
    ) -> SuccessTuple:
    """
    Bootstrap an element (pipes, connectors, config).

    Command:
        `bootstrap {option}`

    Example:
        `bootstrap pipes`
    """
    from meerschaum.utils.misc import choose_subaction
    if action is None:
        action = []
    options = {
        'pipes'      : _bootstrap_pipes,
        'config'     : _bootstrap_config,
        'connectors' : _bootstrap_connectors,
    }
    return choose_subaction(action, options, **kw)

def _bootstrap_pipes(
        action : Optional[List[str]] = None,
        connector_keys : Optional[List[str]] = None,
        metric_keys : Optional[List[str]] = None,
        location_keys : Optional[List[Optional[str]]] = None,
        yes : bool = False,
        force : bool = False,
        noask : bool = False,
        debug : bool = False,
        **kw : Any
    ) -> SuccessTuple:
    """
    Create a new pipe.
    If no keys are provided, guide the user through the steps required.
    """
    from meerschaum import get_pipes
    from meerschaum.config import get_config
    from meerschaum.utils.warnings import info, warn, error
    from meerschaum.utils.debug import dprint
    from meerschaum.utils.prompt import yes_no, prompt, choose
    from meerschaum.connectors.parse import is_valid_connector_keys
    from meerschaum.utils.misc import get_connector_labels
    from meerschaum.utils.formatting._shell import clear_screen

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
                    "Would you like to bootstrap pipes with these keys?\nExisting pipes will be deleted!",
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
        info(f"To create a pipe without explicitly using a connector, use the `register pipes` command.\n")
        try:
            ck = choose(
                f"Where are the data coming from?\n\n" +
                f"Please type the keys of a connector from below,\n" +
                f"or enter '{new_label}' to register a new connector.\n\n" +
                f" {get_config('formatting', 'emoji', 'connector')} Connector:\n",
                get_connector_labels() + [new_label],
                numeric = False
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
                warn(f"Please register a new connector or press CTRL+C to cancel.", stack=False)
        connector_keys = [ck]

        ### Get the metric.
        while True:
            if _clear:
                clear_screen(debug=debug)
            try:
                mk = prompt(
                    f"What kind of data is this?\n\n" +
                    f"The metric is the label for the contents of the pipe.\n" +
                    f"For example, 'weather' might be a metric for weather station data.\n\n" +
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
                f"Where are the data located?\n\n" +
                f"You have the option to create multiple pipes with same connector and\n" +
                f"metric but different locations.\n\n" +
                f"For example, you could create the pipes 'sql_remote_energy_home' and\n" +
                f"'sql_remote_energy_work', which would share a connector ('sql:remote') and\n" +
                f"metric ('energy'), but may come from different tables.\n\n" +
                f"In most cases. you can omit the location.\n\n" +
                f" {get_config('formatting', 'emoji', 'location')} Location (Empty to omit):"
            )
        except KeyboardInterrupt:
            return abort_tuple
        lk = None if lk == '' else lk
        location_keys = [lk]

    if _clear:
        clear_screen(debug=debug)
    _pipes = get_pipes(
        connector_keys, metric_keys, location_keys,
        method = 'explicit', as_list = True,
        debug=debug, **kw
    )
    pipes = []
    for p in _pipes:
        if p.get_id() is not None and not force:
            try:
                if not yes_no(f"Pipe '{p}' already exists. Delete pipe '{p}'?\nData will be lost!", default='n'):
                    info(f"Skipping bootstrapping pipe '{p}'...")
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
            tup = p.bootstrap(interactive=True, force=force, noask=noask, yes=yes, debug=debug)
        except Exception as e:
            tup = False, f"Failed to bootstrap pipe '{p}' with exception:\n" + str(e)
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
        action : Optional[List[str]] = None,
        connector_keys : Optional[List[str]] = None,
        yes : bool = False,
        force : bool = False,
        noask : bool = False,
        debug : bool = False,
        return_keys : bool = False,
        **kw : Any
    ) -> Union[SuccessTuple, Tuple[str, str]]:
    """
    Prompt the user for the details necessary to create a Connector.
    """
    from meerschaum.connectors.parse import is_valid_connector_keys
    from meerschaum.connectors import connectors, get_connector
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

    if action is None:
        action = []
    if connector_keys is None:
        connector_keys = []

    if len(connector_keys) == 0:
        pass

    try:
        _type = choose(
            (
                'Please choose a connector type.\n' +
                'For more information on connectors, please visit https://meerschaum.io/reference/connectors'
            ),
            sorted(list(connectors)),
            default='sql'
        )
    except KeyboardInterrupt:
        return abort_tuple

    if _clear:
        clear_screen(debug=debug)

    _label_choices = sorted(
        [label for label in get_config('meerschaum', 'connectors', _type)
            if label != 'default']
    )
    new_connector_label = 'New connector'
    _label_choices.append(new_connector_label)
    while True:
        try:
            _label = prompt(f"New label for '{_type}' connector:")
        except KeyboardInterrupt:
            return abort_tuple
        if _label in get_config('meerschaum', 'connectors', _type):
            warn(f"Connector '{_type}:{_label}' already exists.", stack=False)
            overwrite = yes_no(f"Do you want to overwrite connector '{_type}:{_label}'?", default='n', yes=yes, noask=noask)
            if not overwrite and not force:
                return False, f"No changes made to connector configuration."
                break
        elif _label == "":
            warn(f"Please enter a label.", stack=False)
        else:
            break

    new_attributes = {}
    if 'flavors' in connector_attributes[_type]:
        try:
            flavor = choose(
                f"Flavor for connector '{_type}:{_label}':",
                sorted(list(connector_attributes[_type]['flavors'])),
                default = (
                    'timescaledb' if 'timescaledb' in connector_attributes[_type]['flavors']
                    else None
                )
            )
        except KeyboardInterrupt:
            return abort_tuple
        new_attributes['flavor'] = flavor
        required = sorted(list(connector_attributes[_type]['flavors'][flavor]['requirements']))
        default = connector_attributes[_type]['flavors'][flavor]['defaults']
    else:
        required = sorted(list(connector_attributes[_type]['required']))
        default = connector_attributes[_type]['default']
    info(
        f"Please answer the following questions to configure the new connector '{_type}:{_label}'." + '\n' +
        "Press Ctrl+C to skip."
    )
    for r in required:
        try:
            val = prompt(f"Value for {r}:")
        except KeyboardInterrupt:
            continue
        if is_int(val):
            val = int(val)
        new_attributes[r] = val

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
    write_config({'meerschaum' : meerschaum_config}, debug=debug)
    if return_keys:
        return _type, _label
    return True, "Success"

def _bootstrap_config(
        action : Optional[List[str]] = None,
        yes : bool = False,
        force : bool = False,
        debug : bool = False,
        **kw : Any
    ) -> SuccessTuple:
    """
    Delete and regenerate the default Meerschaum configuration.
    """
    from meerschaum.config._edit import write_default_config, write_config
    from meerschaum.config._default import default_config

    from meerschaum.actions import actions
    if not actions['delete'](['config'], debug=debug, yes=yes, force=force, **kw)[0]:
        return False, "Failed to delete configuration files."

    if not write_config(default_config, debug=debug, **kw):
        return (False, "Failed to write default configuration.")

    if not write_default_config(debug=debug, **kw):
        return (False, "Failed to write default configuration.")

    from meerschaum.config.stack import write_stack
    if not write_stack(debug=debug):
        return False, "Failed to write stack."

    return (True, "Successfully bootstrapped configuration files.")


### NOTE: This must be the final statement of the module.
###       Any subactions added below these lines will not
###       be added to the `help` docstring.
from meerschaum.utils.misc import choices_docstring as _choices_docstring
bootstrap.__doc__ += _choices_docstring('bootstrap')
