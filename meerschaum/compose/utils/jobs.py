#! /usr/bin/env python3
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

"""
Utility functions for job management.
"""

import copy
import shlex
from meerschaum.utils.typing import Dict, List, Any
from meerschaum.utils.daemon import Daemon

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
            if '--name' not in command_list:
                command_list.extend(['--name', job_name])
            if '-d' not in command_list and '--daemon' not in command_list:
                command_list.append('-d')
            if '-f' not in command_list and '--force' not in command_list:
                command_list.append('-f')

            jobs[job_name] = command_list

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
