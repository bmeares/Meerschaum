#! /usr/bin/env python3
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

"""
Utility functions for job management.
"""

import copy
import json
import shlex
from meerschaum.utils.typing import Dict, List, Any


def _get_explicit_job_name(command: List[str]):
    """Return a command's explicit job name, if provided."""
    for flag in ('--name', '--job-name'):
        if flag in command:
            name_ix = command.index(flag) + 1
            return command[name_ix] if name_ix < len(command) else None
    return None


def job_belongs_to_project(job: Any, project_name: str) -> bool:
    """Return whether a job's environment or command proves project ownership."""
    try:
        compose_config = json.loads((getattr(job, 'env', {}) or {}).get('MRSM__COMPOSE_CONFIG', '{}'))
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

    for legacy_job_name, command_str in explicit_jobs.items():
        command = shlex.split(command_str)
        explicit_job_name = _get_explicit_job_name(command)
        configured_job_name = explicit_job_name or legacy_job_name
        explicit_job = jobs.get(configured_job_name, None)
        if explicit_job is not None and job_belongs_to_project(explicit_job, project_name):
            project_job_names.append(configured_job_name)

    return list(dict.fromkeys(project_job_names))
