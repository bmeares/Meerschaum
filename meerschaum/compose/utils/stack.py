#! /usr/bin/env python3
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

"""
Utility functions for the `stack` command.
"""

from meerschaum.utils.typing import Dict, Any
from meerschaum.utils.warnings import warn


def get_project_name(compose_config: Dict[str, Any]) -> str:
    """
    Determine the `docker-compose` project name.
    """
    root_project_name = compose_config.get('project_name', None)
    stack_project_name = compose_config.get('stack', {}).get('project_name', None)
    compose_file_path = compose_config.get('__file__', None)
    default_project_name = (
        compose_file_path.parent.stem
        if compose_file_path is not None
        else None
    )

    if root_project_name:
        project_name = root_project_name
    elif stack_project_name:
        project_name = stack_project_name
        warn(
            f"Detected a project_name '{stack_project_name}' under 'config:stack:project_name'.\n"
            + "    Will use this as the project name for this compose file.",
            stack = False,
        )
    elif default_project_name is None:
        raise ValueError("Could not determine the project name. Is `project_name` set?")
    else:
        project_name = default_project_name

    return project_name


def ensure_project_name(compose_config: Dict[str, Any]) -> None:
    """
    Ensure the project name is set in the compose configuration dictionary.
    """
    project_name = get_project_name(compose_config)
    compose_config['project_name'] = project_name
    if 'config' not in compose_config:
        compose_config['config'] = {}
    if 'stack' not in compose_config['config']:
        compose_config['config']['stack'] = {}
    compose_config['config']['stack']['project_name'] = project_name
