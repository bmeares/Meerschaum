#! /usr/bin/env python
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

"""
Upgrade your current Meerschaum environment
"""

from __future__ import annotations
from meerschaum.utils.typing import SuccessTuple, Any, List

def upgrade(
        action : Sequence[str] = [],
        **kw : Any
    ) -> SuccessTuple:
    """
    Upgrade Meerschaum or dependencies.

    Command:
        `upgrade {option}`

    Example:
        `upgrade meerschaum`
    """

    from meerschaum.utils.misc import choose_subaction
    options = {
        'plugins'    : _upgrade_plugins,
        'meerschaum' : _upgrade_meerschaum,
        'mrsm'       : _upgrade_meerschaum,
        'packages'   : _upgrade_packages,
    }
    return choose_subaction(action, options, **kw)

def _upgrade_meerschaum(
        action : List[str] = [],
        yes : bool = True,
        force : bool = False,
        debug : bool = False,
        **kw : Any
    ) -> SuccessTuple:
    """
    Upgrade the current Meerschaum instance.
    Optionally specify dependency versions.

    Examples:
        `upgrade meerschaum`
        `upgrade meerschaum full`
    """
    from meerschaum.utils.debug import dprint
    from meerschaum.actions import actions
    from meerschaum.utils.prompt import yes_no
    from meerschaum.utils.packages import pip_install, attempt_import

    is_stack_running = False
    client = None
    docker = attempt_import('docker', warn=False)
    if docker:
        try:
            client = docker.from_env()
            containers = client.containers.list()
            is_stack_running = len(containers) > 0
        except:
            pass

    if is_stack_running:
        answer = True or force
        if not yes and not force:
            answer = yes_no(f"Take down the stack?", default='y')

        if answer:
            if debug: dprint("Taking stack down...")
            actions['stack'](['down'], debug=debug)

    dependencies = None
    if action != [''] and len(action) > 0:
        dependencies = action[0]

    install_name = 'meerschaum' + (
        ('[' + ','.join(dependencies.split(',')) + ']') if dependencies else ''
    )

    if debug: dprint('Upgrade meerschaum with dependencies: \"' + f'{dependencies}' + '\"')
    if not pip_install(install_name, venv=None, debug=debug):
        return False, f"Failed to upgrade Meerschaum via pip."

    if debug: dprint("Pulling new Docker images...")
    if client: actions['stack'](['pull'], debug=debug)

    return True, "Success"

def _upgrade_packages(
        action : List[str] = [],
        yes : bool = False,
        debug : bool = False,
        **kw : Any
    ) -> SuccessTuple:
    """
    Upgrade and install dependencies.
    If provided, upgrade only a dependency group, otherwise default to `full`.

    Examples:
        ```
        upgrade packages
        ```

        ```
        upgrade packages docs
        ```
    """
    from meerschaum.utils.packages import packages, pip_install
    from meerschaum.utils.warnings import info, warn
    from meerschaum.utils.prompt import yes_no
    from meerschaum.utils.formatting import make_header, pprint
    from meerschaum.utils.misc import print_options
    if len(action) == 0:
        group = 'full'
    else:
        group = action[0]

    if group not in packages:
        invalid_msg = f"Invalid dependency group '{group}'."
        avail_msg = make_header("Available groups:")
        for k in packages:
            avail_msg += "\n  - {k}"

        warn(invalid_msg + "\n\n" + avail_msg, stack=False)
        return False, invalid_msg

    print(make_header("Packages to Upgrade:"))
    pprint(packages[group])
    #  print_options(packages[group], header="Packages to Upgrade:")
    question = f"Are you sure you want to upgrade {len(packages[group])} packages (dependency group '{group}')?"
    to_install = [install_name for import_name, install_name in packages[group].items()]

    success, msg = False, f"Nothing installed."
    if yes or yes_no(question):
        success = pip_install(*to_install, debug=debug)
        msg = (
            f"Successfully installed {len(packages[group])} packages" if success
            else f"Failed to install packages in dependency group '{group}'."
        )
    return success, msg

def _upgrade_plugins(
        action : List[str] = [],
        yes : bool = False,
        debug : bool = False,
        **kw : Any
    ) -> SuccessTuple:
    """
    Upgrade all installed plugins to the latest versions.
    If no plugins are specified, attempt to upgrade all, otherwise only upgrade the specified plugins.

    Examples:
    
    ```
    upgrade plugins
    ```
    ```
    upgrade plugins testing
    ```

    """
    from meerschaum.actions import actions
    from meerschaum.actions.plugins import get_plugins_names
    from meerschaum.utils.misc import print_options
    from meerschaum.utils.prompt import yes_no

    to_install = get_plugins_names() if len(action) == 0 else action
    if len(to_install) == 0:
        return False, "No plugins to upgrade."
    print_options(to_install, header="Plugins to Upgrade:")
    if yes or yes_no(f"Upgrade {len(to_install)} plugins?"):
        return actions['install'](action=['plugins'] + to_install, debug=debug, **kw)
    return False, "No plugins upgraded."

### NOTE: This must be the final statement of the module.
###       Any subactions added below these lines will not
###       be added to the `help` docstring.
from meerschaum.utils.misc import choices_docstring as _choices_docstring
upgrade.__doc__ += _choices_docstring('upgrade')
