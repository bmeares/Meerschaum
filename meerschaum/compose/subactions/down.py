#! /usr/bin/env python3
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

"""
Entrypoint to the `compose down` command.
"""

from meerschaum.utils.warnings import info
from meerschaum.utils.typing import SuccessTuple, Any, Dict
from meerschaum.utils.misc import print_options, items_str
from meerschaum.utils.prompt import yes_no


def _compose_down(
    compose_config: Dict[str, Any],
    debug: bool = False,
    drop: bool = False,
    yes: bool = False,
    force: bool = False,
    **kw: Any
) -> SuccessTuple:
    """
    Bring up the configured Meerschaum stack.
    """
    from meerschaum.compose.utils import run_mrsm_command
    from meerschaum.compose.utils.pipes import (
        get_defined_pipes,
        build_custom_connectors,
        instance_pipes_from_pipes_list,
    )
    from meerschaum.compose.utils.stack import get_project_name

    project_name = get_project_name(compose_config)
    run_mrsm_command(
        ['delete', 'jobs', '-f'],
        compose_config,
        capture_output=False,
        debug=debug,
    )

    if not drop:
        return True, "Success"

    _ = build_custom_connectors(compose_config)
    pipes = [pipe for pipe in get_defined_pipes(compose_config) if pipe.id is not None]
    if not pipes:
        return False, "No pipes to delete."

    instance_pipes = instance_pipes_from_pipes_list(pipes)

    print_options(pipes, header="Pipes to be deleted:")
    question = (
        f"Are you sure you want to delete {len(pipes)} pipe" + ('s' if len(pipes) != 1 else '')
        + (
            (
                " on 1 instance"
                if len(instance_pipes) == 1
                else f" across {len(instance_pipes)} instances"
            ) if len(pipes) > 1
            else ''
        )
        + "?"
    )

    if not yes_no(question, yes=yes, force=force, default='n'):
        return True, "Nothing was deleted."

    for instance_keys in instance_pipes:
        info(f"Deleting pipes tagged as '{project_name}' on instance '{instance_keys}'.")
        run_mrsm_command(
            ['delete', 'pipes', '-t', project_name, '-i', instance_keys, '-f'],
            compose_config,
            capture_output=False,
            debug=debug,
        )

    return True, "Success"
