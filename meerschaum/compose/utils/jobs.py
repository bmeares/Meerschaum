#! /usr/bin/env python3
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

"""
Utility functions for job management.
"""

import copy
import json
import shlex
from meerschaum.utils.typing import Dict, List, Any, Optional


def _get_explicit_job_name(command: List[str]):
    """Return a command's explicit job name, if provided."""
    for flag in ('--name', '--job-name'):
        if flag in command:
            name_ix = command.index(flag) + 1
            return command[name_ix] if name_ix < len(command) else None
    return None


### Flags which Compose adds when starting a job but which a running job's
### recorded sysargs may or may not carry (`-d` is stripped by `Job`, `--debug`
### is only present when the job was started from `compose up --debug`, and
### `--no-daemon` is appended only under `isolation: subprocess`).
NON_IDENTIFYING_FLAGS: List[str] = ['-d', '--daemon', '--no-daemon', '--debug']


def _command_signature(sysargs: List[str]) -> List[str]:
    """Return a job command without the flags that don't identify the workload."""
    return [arg for arg in sysargs if arg not in NON_IDENTIFYING_FLAGS]


def job_belongs_to_project(
    job: Any,
    project_name: str,
    project_command: Optional[List[str]] = None,
) -> bool:
    """Return whether a job's environment or command proves project ownership."""
    from meerschaum._internal.static import STATIC_CONFIG
    job_env = getattr(job, 'env', {}) or {}

    ### A job started inside a Compose project records the project's name, so this
    ### proof survives edits to the job's command. Only the name is persisted:
    ### `MRSM__COMPOSE_CONFIG` carries the project's `config:` block (i.e. connector
    ### credentials) and must never be written to a job's properties.
    if job_env.get(STATIC_CONFIG['environment']['compose_project'], None) == project_name:
        return True

    ### Jobs built explicitly with `Job(..., env=...)` may carry the whole config.
    try:
        compose_config = json.loads(job_env.get('MRSM__COMPOSE_CONFIG', '{}'))
        if compose_config.get('project_name', None) == project_name:
            return True
    except Exception:
        pass

    sysargs = list(getattr(job, 'sysargs', []) or [])
    for tag_ix, arg in enumerate(sysargs):
        if arg not in ('-t', '--tags'):
            continue
        for tag in sysargs[tag_ix + 1:]:
            if tag.startswith('-'):
                break
            if tag == project_name:
                return True

    ### A job whose command is byte-for-byte the command this project would run is
    ### taken to be this project's job. This adopts jobs started before Compose
    ### stamped the project name onto them, and jobs which set their own `-t` (e.g.
    ### to select pipes by tag), for which Compose appends no `-t <project_name>`.
    ### Two projects sharing a root directory and configuring the same job name with
    ### the same command are indistinguishable here; the stamp above is what
    ### separates them once each job has been started by its own project.
    if project_command is not None and _command_signature(sysargs) == _command_signature(project_command):
        return True

    return False

def get_jobs_commands(compose_config: Dict[str, Any]) -> Dict[str, List[str]]:
    """
    Return a mapping of jobs' names to their commands (sysargs) to run.
    """
    from meerschaum.compose.utils.stack import get_project_name
    from meerschaum.compose.utils.pipes import (
        build_custom_connectors, get_defined_pipes,
        instance_pipes_from_pipes_list,
    )
    project_name = get_project_name(compose_config)
    explicit_jobs = compose_config.get('jobs', {})
    if explicit_jobs:
        jobs = {}
        for job_name in explicit_jobs:
            command_str = explicit_jobs[job_name]
            command_list = shlex.split(command_str)

            if '-t' not in command_list and '--tags' not in command_list:
                command_list.extend(['-t', project_name])
            explicit_job_name = _get_explicit_job_name(command_list)
            project_job_name = explicit_job_name or job_name
            if explicit_job_name is None:
                command_list.extend(['--name', project_job_name])
            if '-d' not in command_list and '--daemon' not in command_list:
                command_list.append('-d')
            if '-f' not in command_list and '--force' not in command_list:
                command_list.append('-f')

            jobs[project_job_name] = command_list

        return jobs

    pipes = get_defined_pipes(compose_config)
    instance_pipes = instance_pipes_from_pipes_list(pipes)
    job_names = [
        project_name + f' sync ({instance_keys})'
        for instance_keys in instance_pipes
    ]
    schedule = compose_config.get('sync', {}).get('schedule', None)
    min_seconds = compose_config.get('sync', {}).get('min_seconds', None)
    timeout_seconds = compose_config.get('sync', {}).get('timeout_seconds', None)
    args = compose_config.get('sync', {}).get('args', [])
    if isinstance(args, str):
        args = shlex.split(args)

    additional_args = copy.deepcopy(args)
    if schedule:
        if (
            '--schedule' not in args
            and
            '-s' not in args
            and
            '--cron' not in args
        ):
            additional_args += ['--schedule', schedule]
    elif '--loop' not in args:
        additional_args.append('--loop')

    if min_seconds is not None:
        if '--min-seconds' not in args and '--cooldown' not in args:
            additional_args += ['--min-seconds', str(min_seconds)]

    if timeout_seconds is not None:
        if '--timeout-seconds' not in args and '--timeout' not in args:
            additional_args += ['--timeout-seconds', str(timeout_seconds)]

    commands_to_run = [
        (
            [
                'sync', 'pipes', '-i', instance_keys, '-t', project_name,
                '--name', job_name, '-f', '-d',
            ]
            + additional_args
        )
        for instance_keys, job_name in zip(instance_pipes, job_names)
    ]

    return dict(zip(job_names, commands_to_run))


def get_project_job_names(
    compose_config: Dict[str, Any],
    jobs: Dict[str, Any],
) -> List[str]:
    """Return exact job names which this Compose project may safely delete."""
    from meerschaum.compose.utils.stack import get_project_name

    project_name = get_project_name(compose_config)
    project_job_names = [
        job_name
        for job_name, job in jobs.items()
        if job_belongs_to_project(job, project_name)
    ]
    explicit_jobs = compose_config.get('jobs', {})
    if not explicit_jobs:
        return project_job_names

    for configured_job_name, command in get_jobs_commands(compose_config).items():
        explicit_job = jobs.get(configured_job_name, None)
        if explicit_job is None:
            continue
        if job_belongs_to_project(explicit_job, project_name, project_command=command):
            project_job_names.append(configured_job_name)

    return list(dict.fromkeys(project_job_names))
