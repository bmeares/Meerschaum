#! /usr/bin/env python3
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

"""
Entrypoint to `mrsm compose init`.
"""

import os
import copy
import pathlib

import meerschaum as mrsm
from meerschaum.utils.typing import SuccessTuple, Any, Optional, List, Union
from meerschaum.utils.warnings import info


def _compose_init(
    _,
    debug: bool = False,
    file: Optional[pathlib.Path] = None,
    yes: bool = False,
    force: bool = False,
    connector_keys: Optional[List[str]] = None,
    metric_keys: Optional[List[str]] = None,
    location_keys: Optional[List[Union[str, None]]] = None,
    tags: Optional[List[str]] = None,
    mrsm_instance: Optional[str] = None,
    **kw: Any
) -> SuccessTuple:
    """
    Install the required dependencies for this compose project.
    This is useful for building Docker images.
    """
    from meerschaum.compose.utils import run_mrsm_command, init as _init
    from meerschaum.compose.utils.config import infer_compose_file_path, get_env_dict
    from meerschaum.compose.utils.plugins import (
        check_and_install_plugins,
        get_installed_plugins,
    )
    from meerschaum.compose.utils.stack import get_project_name
    import meerschaum.config.paths as paths
    from meerschaum.utils.prompt import yes_no
    from meerschaum.utils.formatting import pprint_pipes
    from meerschaum.config._default import default_config
    from meerschaum.config import replace_config
    from meerschaum.config.environment import replace_env

    compose_path = infer_compose_file_path(file)
    if compose_path is None:
        cwd_path = pathlib.Path(os.getcwd())
        compose_path = cwd_path / 'mrsm-compose.yaml'
        file = compose_path
        info("No compose project could be found.")
        try:
            create_new_compose_file = yes_no(
                f"Create new project '{cwd_path.name}'?\n    ({compose_path})",
                yes = yes,
                force = force,
            )
        except (Exception, KeyboardInterrupt):
            create_new_compose_file = False

        if not create_new_compose_file:
            return False, "No files were created."

        add_pipes = yes_no(
            "Add pipes from the given filters to the compose file?",
            yes=yes,
            force=force,
        ) if (
            connector_keys or metric_keys or location_keys or tags or mrsm_instance
        ) else (info("Run `compose init` with filters (-c, -m, -l, -t, -i) to add pipes."))
        if add_pipes:
            pipes = mrsm.get_pipes(
                connector_keys=connector_keys,
                metric_keys=metric_keys,
                location_keys=location_keys,
                tags=tags,
                mrsm_instance=mrsm_instance,
                **kw
            )
            pprint_pipes(pipes)
            add_pipes = yes_no(
                "Add these pipes to the compose file?",
                yes=yes,
                force=force,
            )
            if not add_pipes:
                info("No pipes will be added.")

        if not _write_new_compose_file(
            compose_path,
            add_pipes=add_pipes,
            connector_keys=connector_keys,
            metric_keys=metric_keys,
            location_keys=location_keys,
            tags=tags,
            mrsm_instance=mrsm_instance,
            **kw
        ):
            return False, f"Failed to create file '{compose_path}'."

    compose_config = _init(file=file, debug=debug, **kw)
    env = get_env_dict(compose_config)
    config = copy.deepcopy(compose_config.get('config', default_config))
    project_name = get_project_name(compose_config)
    existing_plugins = get_installed_plugins(compose_config, debug=debug)
    with replace_config(config):
        with replace_env(env):
            plugins_success, plugins_msg = check_and_install_plugins(
                compose_config,
                debug = debug,
                _existing_plugins = existing_plugins,
            )
    if not plugins_success:
        return plugins_success, plugins_msg

    if existing_plugins:
        with replace_config(config):
            with replace_env(env):
                ### Do NOT scope to `existing_plugins`: that list is discovered IN-PROCESS
                ### (get_installed_plugins), where the already-imported paths/plugins module
                ### doesn't pick up the compose MRSM_PLUGINS_DIR, so it only finds the
                ### internal `compose` plugin — scoping the install to it skips every
                ### project plugin (e.g. mqtt-connector → paho is never installed). Run
                ### `install required` / `setup plugins` UNSCOPED so the subprocess (which
                ### reads the absolute compose env at import) discovers all project plugins.
                install_required_success, install_required_msg = (
                    run_mrsm_command(
                        ['install', 'required'],
                        compose_config,
                        capture_output=False,
                        debug=debug,
                        _subprocess=True,
                    )
                )
                if not install_required_success:
                    return (
                        False,
                        f"Failed to install required packages for project '{project_name}':\n"
                        f"{install_required_msg}"
                    )

                setup_success, setup_msg = (
                    run_mrsm_command(
                        ['setup', 'plugins'],
                        compose_config,
                        capture_output=False,
                        debug=debug,
                        _subprocess=True,
                    )
                )
                if not setup_success:
                    return False, f"Failed to setup plugins for project '{project_name}':\n{setup_msg}"

    return True, f"Finished initializing project '{project_name}'."


def _write_new_compose_file(
    compose_path: pathlib.Path,
    add_pipes: bool = False,
    **kwargs: Any
) -> bool:
    """
    Write these the following text to a new project's compose file.
    """
    from meerschaum.config._edit import general_write_yaml_config

    if compose_path.exists():
        raise FileExistsError(f"File '{compose_path}' already exists.")

    pipes = [
        {
            'connector': pipe.connector_keys,
            'metric': pipe.metric_key,
            'location': pipe.location_key,
            'instance': pipe.instance_keys,
            'parameters': pipe.parameters,
        }
        for pipe in mrsm.get_pipes(as_list=True, **kwargs)
    ] if add_pipes else []

    return general_write_yaml_config({
        compose_path: (
            {
                'project_name': compose_path.parent.name,
                'root_dir': './root',
                'plugins_dir': ['./plugins'],
                'pipes': pipes,
                'plugins': [],
                'config': {
                    'meerschaum': {
                        'connectors': {
                            'sql': {
                                'main': 'MRSM{meerschaum:connectors:sql:main}',
                            },
                        },
                    },
                },
                'environment': {},
            },
            "### See https://meerschaum.io/reference/compose/ for the compose schema.\n\n",
        )
    })
