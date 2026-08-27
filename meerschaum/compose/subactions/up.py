#! /usr/bin/env python3
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

"""
Entrypoint to the `compose up` command.
"""

import json

import meerschaum as mrsm
from meerschaum.utils.typing import SuccessTuple, Dict, Any, List, Optional
from meerschaum.utils.warnings import info, warn, dprint
from meerschaum.utils.misc import print_options


def _compose_up(
    compose_config: Dict[str, Any],
    dry: bool = False,
    force: bool = False,
    presync: bool = False,
    no_jobs: bool = False,
    sysargs: Optional[List[str]] = None,
    debug: bool = False,
    **kw
) -> SuccessTuple:
    """
    Bring up the configured Meerschaum stack.
    """
    from meerschaum.utils.pipes import is_pipe_registered
    from meerschaum.compose.utils import run_mrsm_command
    from meerschaum.compose.utils.stack import get_project_name
    from meerschaum.compose.utils.plugins import check_and_install_plugins
    from meerschaum.compose.utils.pipes import (
        build_custom_connectors,
        get_defined_pipes,
        instance_pipes_from_pipes_list,
    )
    from meerschaum.compose.utils.jobs import get_jobs_commands, get_project_job_names
    from meerschaum.compose.utils.config import config_has_changed
    from meerschaum.jobs import get_jobs
    no_daemon_flags = (
        ['--no-daemon']
        if compose_config.get('isolation', None) == 'subprocess'
        else []
    )

    success, msg = check_and_install_plugins(compose_config, debug=debug)
    if not success:
        return success, msg

    ### Initialize the custom connectors and build the in-memory pipes.
    custom_connectors = build_custom_connectors(compose_config)
    if debug:
        dprint("Compose: Built custom connectors:")
        mrsm.pprint(custom_connectors)

    pipes = get_defined_pipes(compose_config, debug=debug)
    instance_pipes = instance_pipes_from_pipes_list(pipes)
    project_name = get_project_name(compose_config)
    explicit_jobs = compose_config.get('jobs', {})

    remote_instance_pipes = {
        instance_keys: mrsm.get_pipes(
            tags=[project_name],
            instance=custom_connectors.get(instance_keys, instance_keys),
            debug=debug,
        )
        for instance_keys in instance_pipes
    }


    ### Update the parameters in case the remote has changed.
    updated_pipes = []
    updated_registration = False
    for pipe in pipes:
        if debug:
            dprint(f"Compose: Checking parameters for {pipe}...")
        updated_registration = False

        pipe_is_registered = is_pipe_registered(pipe, remote_instance_pipes.get(pipe.instance_keys, None))

        remote_pipe = (
            remote_instance_pipes[pipe.instance_keys][pipe.connector_keys][pipe.metric_key][pipe.location_key]
            if pipe_is_registered
            else mrsm.Pipe(**pipe.meta, **{'cache': False})
        )

        ### Some instance connectors pre-cache the parameters.
        remote_parameters = remote_pipe._attributes.get('parameters', None) or (
            remote_pipe.get_parameters(
                refresh=False,
                apply_symlinks=False,
                debug=debug,
            )
        )
        if debug:
            dprint(f"Remote parameters for {pipe}...")
            mrsm.pprint(remote_parameters)
        local_parameters = pipe._attributes['parameters']

        local_parameters_str = json.dumps(local_parameters, sort_keys=True, separators=(',', ':'))
        remote_parameters_str = json.dumps(remote_parameters, sort_keys=True, separators=(',', ':'))

        if pipe.temporary:
            info(f"{pipe} is temporary, will not modify registration.")
        elif not pipe_is_registered:
            ### Clear any stale local cache (e.g. pipe id) from a prior registration.
            try:
                pipe._invalidate_cache(hard=True, debug=debug)
            except Exception as e:
                if debug:
                    dprint(f"Failed to invalidate cache for {pipe}: {e}")

            ### The pipe may already exist under a different project's tag.
            ### In that case, merge tags + parameters instead of re-registering.
            existing_id = None
            try:
                existing_id = remote_pipe.get_id(debug=debug)
            except Exception as e:
                if debug:
                    dprint(f"Could not check remote id for {pipe}: {e}")

            if existing_id is not None:
                info(
                    f"{pipe} already exists on '{pipe.instance_keys}'; "
                    f"adding tag '{project_name}'..."
                )
                try:
                    fresh_remote_params = remote_pipe.get_parameters(refresh=True, debug=debug) or {}
                except Exception:
                    fresh_remote_params = remote_parameters or {}
                existing_tags = list((fresh_remote_params or {}).get('tags', []) or [])
                local_tags = list((pipe.parameters or {}).get('tags', []) or [])
                merged_tags = list(dict.fromkeys(existing_tags + local_tags))
                merged_params = dict(pipe.parameters or {})
                merged_params['tags'] = merged_tags
                remote_pipe.parameters = merged_params
                try:
                    success, msg = remote_pipe.edit(debug=debug)
                except Exception as e:
                    success, msg = False, str(e)
                if not success:
                    warn(f"Failed to add tag '{project_name}' to {pipe}:\n{msg}", stack=False)
            else:
                info(f"Registering {pipe}...")
                success, msg = run_mrsm_command(
                    [
                        'register', 'pipes',
                        '-c', str(pipe.connector_keys),
                        '-m', str(pipe.metric_key),
                        '-l', str(pipe.location_key),
                        '-i', str(pipe.instance_keys),
                        '--params', json.dumps(pipe.parameters, separators=(',', ':')),
                        '--noask',
                    ] + no_daemon_flags,
                    compose_config,
                    capture_output=False,
                    debug=debug,
                    _replace=False,
                    _subprocess=False,
                )
                if not success:
                    warn(f"Failed to register {pipe}.", stack=False)
            updated_registration = True

        ### Check the remote parameters against the specified parameters in the YAML.
        elif local_parameters_str != remote_parameters_str:
            if debug:
                dprint("Local parameters:")
                mrsm.pprint(local_parameters)
                dprint("Remote parameters:")
                mrsm.pprint(remote_parameters)

            ### Editing with `--params` in a subprocess only patches,
            ### so instead replace the parameters dictionary directly.
            info(f"Updating parameters for {pipe}...")
            try:
                pipe._invalidate_cache(hard=True, debug=debug)
            except Exception as e:
                if debug:
                    dprint(f"Failed to invalidate cache for {pipe}: {e}")
            success, msg = pipe.edit(debug=debug)
            if not success:
                warn(f"Failed to edit {pipe}.", stack=False)
            updated_registration = True

        if updated_registration or presync or pipe.temporary:
            updated_pipes.append(pipe)

    ### Untag pipes that are tagged but no longer defined in mrsm-config.yaml.
    if debug:
        dprint(f"Compose: Checking for stale pipes tagged as '{project_name}'...")
    tagged_instance_pipes = {
        instance_keys: mrsm.get_pipes(
            tags=[project_name],
            instance=custom_connectors.get(instance_keys, instance_keys),
            as_list=True,
            debug=debug,
        )
        for instance_keys in instance_pipes
    }
    for instance_connector, tagged_pipes in tagged_instance_pipes.items():
        for tagged_pipe in tagged_pipes:
            if tagged_pipe not in pipes:
                try:
                    tagged_pipe.tags = [
                        _tag
                        for _tag in tagged_pipe.tags
                        if _tag != project_name
                    ]
                except Exception:
                    warn(f"{tagged_pipe} was incorrectly tagged with '{project_name}'...")
                    continue
                info(f"Removing tag '{project_name}' from {tagged_pipe}...")
                tagged_pipe.edit(debug=debug)

    if dry:
        return True, (
            f"Updated parameters for {len(pipes)} pipe"
            + ("s" if len(pipes) != 1 else "")
            + (" across " if len(instance_pipes) != 1 else " on ")
            + f"{len(instance_pipes)} instance"
            + ("s" if len(instance_pipes) != 1 else "")
            + "."
        )

    ### If any changes have been made to the config file's values,
    ### trigger another verification pass before starting jobs.
    ### Explicitly configured jobs are the project's workload, so syncing the pipes
    ### is not implied by bringing them up (`--presync` still forces a pass).
    ran_verification_sync = False
    if presync or (not explicit_jobs and updated_pipes and config_has_changed(compose_config)):
        ran_verification_sync = True
        print_options(
            pipes,
            header = (
                f"Running initial syncs for {len(updated_pipes)} pipe"
                + ('s' if len(updated_pipes) != 1 else '')
                + ':'
            ),
        )
        success, msg = run_initial_syncs(
            updated_pipes,
            compose_config,
            sysargs,
            debug = debug,
            **kw
        )
        if not success:
            return success, msg

    if no_jobs:
        msg = (
            (
                f"Synced {len(updated_pipes)} pipe"
                + ("s" if len(updated_pipes) != 1 else "")
                + f" across {len(instance_pipes)} instance"
                + ("s" if len(instance_pipes) != 1 else "")
                + "."
            )
            if ran_verification_sync
            else (
                (
                    f"Updated {len(updated_pipes)} pipe"
                    + ("s" if len(updated_pipes) != 1 else "")
                    + f" across {len(instance_pipes)} instance"
                    + "."
                )
                if updated_pipes
                else "Nothing to do."
            )
        )
        return True, msg

    jobs_commands = get_jobs_commands(compose_config)
    existing_jobs = get_jobs(debug=debug)
    job_names_to_delete = get_project_job_names(compose_config, existing_jobs)
    unsafe_collisions = [
        job_name
        for job_name in jobs_commands
        if job_name in existing_jobs and job_name not in job_names_to_delete
    ]
    if unsafe_collisions:
        return False, (
            "Refusing to replace job"
            + ('s' if len(unsafe_collisions) != 1 else '')
            + " not owned by this Compose project: "
            + ', '.join(unsafe_collisions)
        )

    for job_name in job_names_to_delete:
        delete_success, delete_msg = run_mrsm_command(
            ['delete', 'job', job_name, '-f'],
            compose_config,
            capture_output=(not debug),
            debug=debug,
            _replace=False,
        )
        if not delete_success:
            return False, f"Failed to delete job '{job_name}':\n{delete_msg}"

    for job_name, job_command in jobs_commands.items():
        info(f"Starting job '{job_name}'...")
        start_success, start_msg = run_mrsm_command(
            job_command,
            compose_config,
            capture_output=False,
            debug=debug,
            _replace=False,
        )
        if not start_success:
            return False, f"Failed to start job '{job_name}':\n{start_msg}"

    if force:
        run_mrsm_command(
            ['show', 'logs'] + list(jobs_commands),
            compose_config,
            capture_output=False,
            debug=debug,
            _replace=False,
        )

    if explicit_jobs:
        msg = (
            f"Running {len(jobs_commands)} background job"
            + ('s' if len(jobs_commands) != 1 else '')
            + '.'
        )
    elif len(pipes) == 1:
        msg = f"Syncing {pipes[0]} in a background job."
    else:
        msg = (
            f"Syncing {len(pipes)} pipe" + ('s' if len(pipes) != 1 else '')
            + (" across " if len(jobs_commands) != 1 else " on ")
            + f"{len(jobs_commands)} instance"
            + ('s' if len(jobs_commands) != 1 else '')
            + "."
        )

    msg += (
        "\nRun `mrsm compose logs` or pass `-f` to follow logs output."
        if not force
        else ''
    )
    return True, msg


def run_initial_syncs(
    pipes: List[mrsm.Pipe],
    compose_config: Dict[str, Any],
    sysargs: Optional[List[str]] = None,
    debug: bool = False,
    **kw
) -> SuccessTuple:
    """
    Try two passes of syncing before starting the jobs.
    """
    from meerschaum.compose.utils import run_mrsm_command
    flags_to_remove = {
        '-c', '-C', '--connector-keys',
        '-m', '-M', '--metric-keys',
        '-l', '-L', '--location-keys',
        '-i', '-I', '--mrsm-instance', '--instance',
    }
    sysargs = sysargs or []
    indices_to_remove = {i for i, flag in enumerate(sysargs) if flag in flags_to_remove}
    flags = [
        flag
        for i, flag in enumerate(sysargs)
        if i not in indices_to_remove
            and (i - 1) not in indices_to_remove
    ]
    if '--no-daemon' not in flags:
        flags.append('--no-daemon')

    failed_pipes = []
    for pipe in pipes:
        info(f"Syncing {pipe}...")
        success, msg = (
            run_mrsm_command(
                [
                    'sync',
                    'pipes',
                    '-c', str(pipe.connector_keys),
                    '-m', str(pipe.metric_key),
                    '-l', str(pipe.location_key),
                    '-i', str(pipe.instance_keys),
                ] + flags,
                compose_config,
                capture_output=False,
                debug=debug,
                _replace=False,
            )
            if not pipe.temporary
            else pipe.sync(debug=debug, **kw)
        )

        if not success:
            warn(f"Failed to sync {pipe}:\n{msg}", stack=False)
            failed_pipes.append(pipe)

    if not failed_pipes:
        return True, "Success"

    ### Pipes may be interdependent, so try again if we encounter any errors.
    for pipe in failed_pipes:
        info(f"Retry syncing {pipe}...")
        success, msg = (
            run_mrsm_command(
                [
                    'sync',
                    'pipes',
                    '-c', str(pipe.connector_keys),
                    '-m', str(pipe.metric_key),
                    '-l', str(pipe.location_key),
                    '-i', str(pipe.instance_keys),
                ] + flags,
                compose_config,
                capture_output=False,
                debug=debug,
                _replace=False,
            )
            if not pipe.temporary
            else pipe.sync(debug=debug, **kw)
        )

        if not success:
            warn(f"Failed to sync {pipe}:\n{msg}", stack=False)
            return False, f"Unable to begin syncing {pipe}:\n{msg}"

    return True, "Success"
