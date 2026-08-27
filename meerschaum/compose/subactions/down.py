#! /usr/bin/env python3
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

"""
Entrypoint to the `compose down` command.
"""

from meerschaum.utils.warnings import info
from meerschaum.utils.typing import SuccessTuple, Any, Dict
from meerschaum.utils.misc import print_options
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
    from meerschaum.compose.utils.jobs import get_project_job_names
    from meerschaum.jobs import get_jobs
    import copy

    project_name = get_project_name(compose_config)
    jobs = get_jobs(debug=debug)
    for job_name in get_project_job_names(compose_config, jobs):
        delete_success, delete_msg = run_mrsm_command(
            ['delete', 'job', job_name, '-f'],
            compose_config,
            capture_output=False,
            debug=debug,
        )
        if not delete_success:
            return False, f"Failed to delete job '{job_name}':\n{delete_msg}"

    if not drop:
        return True, "Success"

    _ = build_custom_connectors(compose_config)
    pipes = [pipe for pipe in get_defined_pipes(compose_config) if pipe.id is not None]
    if not pipes:
        return False, "No pipes to delete."

    instance_pipes = instance_pipes_from_pipes_list(pipes)

    print_options(pipes, header="Pipes to be removed from this project:")
    question = (
        f"Remove {len(pipes)} pipe" + ('s' if len(pipes) != 1 else '')
        + " from this project? Unshared pipes and their data will be deleted"
        + (
            (
                " on 1 instance"
                if len(instance_pipes) == 1
                else f" across {len(instance_pipes)} instances"
            ) if len(pipes) > 1
            else ''
        )
        + "."
    )

    if not yes_no(question, yes=yes, force=force, default='n'):
        return True, "Nothing was deleted."

    failures = []
    for pipe in pipes:
        try:
            remote_attributes = pipe.instance_connector.get_pipe_attributes(pipe) or {}
            remote_parameters = remote_attributes.get('parameters', None)
            remote_tags = remote_parameters.get('tags', None) if remote_parameters else None
        except Exception as e:
            failures.append(f"Could not verify ownership of {pipe}: {e}")
            continue

        if not isinstance(remote_parameters, dict) or not isinstance(remote_tags, list):
            failures.append(f"Could not verify ownership of {pipe}; it was preserved.")
            continue

        remaining_tags = [tag for tag in remote_tags if tag != project_name]
        if len(remaining_tags) == len(remote_tags):
            info(f"Preserving {pipe}; it is not tagged as '{project_name}'.")
            continue

        pipe.parameters = copy.deepcopy(remote_parameters)
        if remaining_tags:
            info(f"Preserving shared {pipe} and removing tag '{project_name}'.")
            pipe.tags = remaining_tags
            edit_success, edit_msg = pipe.edit(debug=debug)
            if not edit_success:
                failures.append(f"Failed to untag {pipe}: {edit_msg}")
            continue

        info(f"Deleting unshared {pipe}.")
        delete_success, delete_msg = pipe.delete(drop=True, debug=debug)
        if not delete_success:
            failures.append(f"Failed to delete {pipe}: {delete_msg}")

    if failures:
        return False, '\n'.join(failures)

    return True, "Success"
