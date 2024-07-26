#! /usr/bin/env python
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

"""
Upgrade your current Meerschaum environment
"""

from __future__ import annotations
from meerschaum.utils.typing import SuccessTuple, Any, List, Optional, Union

def upgrade(
    action: Optional[List[str]] = None,
    **kw: Any
) -> SuccessTuple:
    """
    Upgrade Meerschaum, plugins, or packages.
    
    Command:
        `upgrade {option}`
    
    Example:
        `upgrade meerschaum`
    """

    from meerschaum.actions import choose_subaction
    options = {
        'plugins'    : _upgrade_plugins,
        'meerschaum' : _upgrade_meerschaum,
        'mrsm'       : _upgrade_meerschaum,
        'packages'   : _upgrade_packages,
    }
    return choose_subaction(action, options, **kw)


def _upgrade_meerschaum(
    action: Optional[List[str]] = None,
    yes: bool = False,
    force: bool = False,
    noask: bool = False,
    debug: bool = False,
    **kw: Any
) -> SuccessTuple:
    """
    Upgrade the current Meerschaum instance.
    Optionally specify dependency groups.
    
    Examples:
        - `upgrade meerschaum`
        - `upgrade meerschaum full`

    """
    import subprocess
    import json
    from meerschaum.utils.debug import dprint
    from meerschaum.utils.warnings import info
    from meerschaum.actions import actions
    from meerschaum.utils.prompt import yes_no
    from meerschaum.utils.packages import pip_install, attempt_import
    from meerschaum.utils.misc import is_docker_available

    if action is None:
        action = []

    is_stack_running = False
    try:
        success, msg = actions['stack'](
            ['ps', '-q'], _capture_output=True, debug=debug
        )
        if msg.endswith('\n'):
            msg = msg[:-1]
        containers = msg.split('\n')
    except Exception as e:
        containers = []

    if containers:
        if force:
            answer = True
        else:
            answer = yes_no(f"Take down the stack?", default='y', yes=yes, noask=noask)

        if answer:
            if debug:
                info("Taking stack down...")
            actions['stack'](['down'], debug=debug)

    dependencies = None
    if action != [''] and len(action) > 0:
        dependencies = action[0]

    install_name = 'meerschaum' + (
        ('[' + ','.join(dependencies.split(',')) + ']') if dependencies else ''
    )

    if debug:
        dprint('Upgrade meerschaum with dependencies: \"' + f'{dependencies}' + '\"')
    if not pip_install(install_name, venv=None, debug=debug):
        return False, f"Failed to upgrade Meerschaum via pip."

    if debug:
        dprint("Pulling new Docker images...")
    if is_docker_available():
        actions['stack'](['pull'], debug=debug)

    return True, "Success"


class NoVenv:
    pass

def _upgrade_packages(
    action: Optional[List[str]] = None,
    venv: Union[str, None, NoVenv] = NoVenv,
    yes: bool = False,
    force: bool = False,
    noask: bool = False,
    debug: bool = False,
    **kw: Any
) -> SuccessTuple:
    """
    Upgrade and install dependencies.
    If provided, upgrade only a dependency group, otherwise default to `full`.
    
    Examples:
        upgrade packages
        upgrade packages full
    """
    from meerschaum.utils.packages import packages, pip_install, get_prerelease_dependencies
    from meerschaum.utils.warnings import info, warn
    from meerschaum.utils.prompt import yes_no
    from meerschaum.utils.formatting import make_header, pprint
    from meerschaum.utils.misc import print_options
    if action is None:
        action = []
    if venv is NoVenv:
        venv = 'mrsm'
    if len(action) == 0:
        group = 'api'
    else:
        group = action[0]

    if group not in packages:
        invalid_msg = f"Invalid dependency group '{group}'."
        avail_msg = make_header("Available groups:")
        for k in packages:
            avail_msg += f"\n  - {k}"

        warn(invalid_msg + "\n\n" + avail_msg, stack=False)
        return False, invalid_msg

    print(make_header("Packages to Upgrade:"))
    pprint(packages[group])
    question = (
        f"Are you sure you want to upgrade {len(packages[group])} packages " +
        f"(dependency group '{group}')?"
    )
    to_install = [install_name for import_name, install_name in packages[group].items()]
    prereleases_to_install = get_prerelease_dependencies(to_install)
    to_install = [
        install_name
        for install_name in to_install
        if install_name not in prereleases_to_install
    ]

    success, msg = False, f"Nothing installed."
    if force or yes_no(question, noask=noask, yes=yes):
        success = pip_install(*to_install, debug=debug)
        if success and prereleases_to_install:
            success = pip_install(*prereleases_to_install, debug=debug)
        msg = (
            f"Successfully installed {len(packages[group])} packages." if success
            else f"Failed to install packages in dependency group '{group}'."
        )
    return success, msg


def _upgrade_plugins(
    action: Optional[List[str]] = None,
    yes: bool = False,
    force: bool = False,
    noask: bool = False,
    debug: bool = False,
    **kw: Any
) -> SuccessTuple:
    """
    Upgrade all installed plugins to the latest versions.
    If no plugins are specified, attempt to upgrade all,
    otherwise only upgrade the specified plugins.
    
    Examples:
    
    upgrade plugins
    upgrade plugins noaa
    """
    from meerschaum.actions import actions
    from meerschaum.plugins import get_plugins_names
    from meerschaum.utils.misc import print_options
    from meerschaum.utils.prompt import yes_no

    if action is None:
        action = []

    to_install = get_plugins_names() if len(action) == 0 else action
    if len(to_install) == 0:
        return False, "No plugins to upgrade."
    print_options(to_install, header="Plugins to Upgrade:")
    if force or yes_no(f"Upgrade {len(to_install)} plugins?", yes=yes, noask=noask):
        return actions['install'](
            action=['plugins'] + to_install, debug=debug, force=force, noask=noask, yes=yes, **kw
        )
    return False, "No plugins upgraded."

### NOTE: This must be the final statement of the module.
###       Any subactions added below these lines will not
###       be added to the `help` docstring.
from meerschaum.actions import choices_docstring as _choices_docstring
upgrade.__doc__ += _choices_docstring('upgrade')
