#! /usr/bin/env python3
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

"""
Manage virtual environments.
"""

from __future__ import annotations

from meerschaum.utils.typing import Optional, Union, Dict, List, Tuple
from meerschaum.utils.threading import RLock

__all__ = sorted([
    'activate_venv', 'deactivate_venv', 'init_venv',
    'inside_venv', 'is_venv_active', 'venv_exec',
    'venv_executable', 'venv_exists', 'venv_target_path',
    'Venv', 'get_venvs', 'verify_venv',
])
__pdoc__ = {'Venv': True}

LOCKS = {
    'sys.path': RLock(),
    'active_venvs': RLock(),
}

active_venvs = set()


def activate_venv(
        venv: Optional[str] = 'mrsm',
        color : bool = True,
        debug: bool = False
    ) -> bool:
    """
    Create a virtual environment (if it doesn't exist) and add it to `sys.path` if necessary.

    Parameters
    ----------
    venv: Optional[str], default 'mrsm'
        The virtual environment to activate.

    color: bool, default True
        If `True`, include color in debug text.

    debug: bool, default False
        Verbosity toggle.

    Returns
    -------
    A bool indicating whether the virtual environment was successfully activated.

    """
    if venv in active_venvs:
        return True
    import sys, platform, os
    from meerschaum.config._paths import VIRTENV_RESOURCES_PATH
    if debug:
        from meerschaum.utils.debug import dprint
    if venv is not None:
        init_venv(venv=venv, debug=debug)
    with LOCKS['active_venvs']:
        if debug:
            dprint(f"Activating virtual environment '{venv}'...", color=color)
        active_venvs.add(venv)

    target = venv_target_path(venv, debug=debug)
    if str(target) not in sys.path:
        sys.path.insert(0, str(target))

    #  if debug:
        #  dprint(f"sys.path: {sys.path}", color=False)

    return True


def deactivate_venv(
        venv: str = 'mrsm',
        color : bool = True,
        debug: bool = False
    ) -> bool:
    """
    Remove a virtual environment from `sys.path` (if it's been activated).

    Parameters
    ----------
    venv: str, default 'mrsm'
        The virtual environment to deactivate.

    color: bool, default True
        If `True`, include color in debug text.

    debug: bool, default False
        Verbosity toggle.

    Returns
    -------
    Return a bool indicating whether the virtual environment was successfully deactivated.

    """
    import sys
    if venv is None:
        return True

    if debug:
        from meerschaum.utils.debug import dprint
        dprint(f"Deactivating virtual environment '{venv}'...", color=color)

    if venv in active_venvs:
        with LOCKS['active_venvs']:
            active_venvs.remove(venv)

    if sys.path is None:
        return False

    target = venv_target_path(venv, debug=debug)
    with LOCKS['sys.path']:
        if str(target) in sys.path:
            sys.path.remove(str(target))

    #  if debug:
        #  dprint(f"sys.path: {sys.path}", color=False)

    return True


def is_venv_active(
        venv: str = 'mrsm',
        color : bool = True,
        debug: bool = False
    ) -> bool:
    """
    Check if a virtual environment is active.

    Parameters
    ----------
    venv: str, default 'mrsm'
        The virtual environment to check.

    color: bool, default True
        If `True`, include color in debug text.

    debug: bool, default False
        Verbosity toggle.

    Returns
    -------
    A bool indicating whether the virtual environment `venv` is active.

    """
    if debug:
        from meerschaum.utils.debug import dprint
        dprint(f"Checking if virtual environment '{venv}' is active.", color=color)
    return venv in active_venvs

verified_venvs = set()
def verify_venv(
        venv: str,
        debug: bool = False,
    ) -> None:
    """
    Verify that the virtual environment matches the expected state.
    """
    import pathlib, platform, os, shutil, subprocess, sys
    from meerschaum.config._paths import VIRTENV_RESOURCES_PATH
    from meerschaum.utils.process import run_process
    venv_path = VIRTENV_RESOURCES_PATH / venv
    bin_path = venv_path / (
        'bin' if platform.system() != 'Windows' else "Scripts"
    )
    current_python_versioned_name = (
        'python' + str(sys.version_info.major) + '.' + str(sys.version_info.minor)
        + ('' if platform.system() != 'Windows' else '.exe')
    )

    if not bin_path.exists() or current_python_versioned_name not in os.listdir(bin_path):
        init_venv(venv, verify=False, force=True, debug=debug)
        current_python_in_venv_path = pathlib.Path(venv_executable(venv=venv))
        current_python_in_sys_path = pathlib.Path(venv_executable(venv=None))
        if not current_python_in_venv_path.exists():
            current_python_in_venv_path.symlink_to(current_python_in_sys_path)

    def get_python_version(python_path: pathlib.Path) -> Union[str, None]:
        """
        Return the version for the python binary at the given path.
        """
        try:
            ### It might be a broken symlink, so skip on errors.
            proc = run_process(
                [str(python_path), '-V'],
                as_proc = True,
                capture_output = True,
            )
            stdout, stderr = proc.communicate(timeout=0.1)
        except Exception as e:
            ### E.g. the symlink may be broken.
            return None
        return stdout.decode('utf-8').strip().replace('Python ', '')

    ### Ensure the versions are symlinked correctly.
    for filename in os.listdir(bin_path):
        if not filename.startswith('python'):
            continue
        python_path = bin_path / filename
        version = get_python_version(python_path)
        if version is None:
            continue
        major_version = version.split('.', maxsplit=1)[0]
        minor_version = version.split('.', maxsplit=2)[1]
        python_versioned_name = (
            'python' + major_version + '.' + minor_version
            + ('' if platform.system() != 'Windows' else '.exe')
        )

        ### E.g. python3.10 actually links to Python 3.10.
        if filename == python_versioned_name:
            real_path = pathlib.Path(os.path.realpath(python_path))
            if not real_path.exists():
                #  print(f"Does not exist:\n{python_path}\n->\n{real_path}")
                python_path.unlink()
                init_venv(venv, verify=False, force=True, debug=debug)
                if not python_path.exists():
                    raise FileNotFoundError(f"Unable to verify Python symlink:\n{python_path}")

            if python_path == real_path:
                continue

            python_path.unlink()
            python_path.symlink_to(real_path)
            continue

        python_versioned_path = bin_path / python_versioned_name
        if python_versioned_path.exists():
            ### Avoid circular symlinks.
            if get_python_version(python_versioned_path) == version:
                continue
            python_versioned_path.unlink()
        shutil.move(python_path, python_versioned_path)


tried_virtualenv = False
def init_venv(
        venv: str = 'mrsm',
        verify: bool = True,
        force: bool = False,
        debug: bool = False,
    ) -> bool:
    """
    Initialize the virtual environment.

    Parameters
    ----------
    venv: str, default 'mrsm'
        The name of the virtual environment to create.

    verify: bool, default True
        If `True`, verify that the virtual environment is in the expected state.

    force: bool, default False
        If `True`, recreate the virtual environment, even if already initalized.

    Returns
    -------
    A `bool` indicating success.
    """
    if not force and venv in verified_venvs:
        return True
    if not force and venv_exists(venv, debug=debug):
        if verify:
            verify_venv(venv, debug=debug)
            verified_venvs.add(venv)
        return True

    import sys, platform, os, pathlib
    from meerschaum.config._paths import VIRTENV_RESOURCES_PATH
    from meerschaum.utils.packages import run_python_package, attempt_import
    global tried_virtualenv
    try:
        import venv as _venv
        virtualenv = None
    except ImportError:
        _venv = None
        virtualenv = None
    
    venv_path = VIRTENV_RESOURCES_PATH / venv

    _venv_success = False
    if _venv is not None:
        import io
        from contextlib import redirect_stdout
        f = io.StringIO()
        with redirect_stdout(f):
            _venv_success = run_python_package(
                'venv',
                [str(venv_path)] + (
                    ['--symlinks'] if platform.system() != 'Windows' else []
                ),
                venv=None, debug=debug
            ) == 0
        if not _venv_success:
            print("Please install python3-venv! Falling back to virtualenv...")
        if not venv_exists(venv, debug=debug):
            _venv = None
    if not _venv_success:
        virtualenv = attempt_import(
            'virtualenv', venv=None, lazy=False, install=(not tried_virtualenv), warn=False,
            check_update=False, color=False, debug=debug,
        )
        if virtualenv is None:
            print(
                "Failed to import `venv` or `virtualenv`! "
                + "Please install `virtualenv` via pip then restart Meerschaum."
            )
            return False

        tried_virtualenv = True
        try:
            python_folder = (
                'python' + str(sys.version_info.major) + '.' + str(sys.version_info.minor)
            )
            dist_packages_path = (
                VIRTENV_RESOURCES_PATH /
                venv / 'local' / 'lib' / python_folder / 'dist-packages'
            )
            local_bin_path = VIRTENV_RESOURCES_PATH / venv / 'local' / 'bin'
            bin_path = VIRTENV_RESOURCES_PATH / venv / 'bin'
            vtp = venv_target_path(venv=venv, allow_nonexistent=True, debug=debug)
            virtualenv.cli_run([str(venv_path)])
            if dist_packages_path.exists():
                import shutil
                shutil.move(dist_packages_path, vtp)
                shutil.move(local_bin_path, bin_path)

        except Exception as e:
            import traceback
            traceback.print_exc()
            return False
    if verify:
        verify_venv(venv, debug=debug)
        verified_venvs.add(venv)
    return True


def venv_executable(venv: Optional[str] = 'mrsm') -> str:
    """
    The Python interpreter executable for a given virtual environment.
    """
    from meerschaum.config._paths import VIRTENV_RESOURCES_PATH
    import sys, platform, os
    return (
        sys.executable if venv is None
        else str(
            VIRTENV_RESOURCES_PATH
            / venv
            / (
                'bin' if platform.system() != 'Windows'
                else 'Scripts'
            ) / (
                'python'
                + str(sys.version_info.major)
                + '.'
                + str(sys.version_info.minor)
            )
        )
    )


def venv_exec(
        code: str,
        venv: Optional[str] = 'mrsm',
        with_extras: bool = False,
        as_proc: bool = False,
        capture_output: bool = True,
        debug: bool = False,
    ) -> Union[bool, Tuple[int, bytes, bytes]]:
    """
    Execute Python code in a subprocess via a virtual environment's interpeter.
    Return `True` if the code successfully executes, `False` on failure.

    Parameters
    ----------
    code: str
        The Python code to excecute.

    venv: str, default 'mrsm'
        The virtual environment to use to get the path for the Python executable.
        If `venv` is `None`, use the default `sys.executable` path.

    with_extras: bool, default False
        If `True`, return a tuple of the exit code, stdout bytes, and stderr bytes.

    as_proc: bool, default False
        If `True`, return the `subprocess.Popen` object instead of executing.

    Returns
    -------
    By default, return a bool indicating success.
    If `as_proc` is `True`, return a `subprocess.Popen` object.
    If `with_extras` is `True`, return a tuple of the exit code, stdout bytes, and stderr bytes.

    """
    import subprocess
    from meerschaum.utils.debug import dprint
    executable = venv_executable(venv=venv)
    cmd_list = [executable, '-c', code]
    if debug:
        dprint(str(cmd_list))
    if not with_extras and not as_proc:
        return subprocess.call(cmd_list) == 0

    stdout, stderr = (None, None) if not capture_output else (subprocess.PIPE, subprocess.PIPE)
    process = subprocess.Popen(cmd_list, stdout=stdout, stderr=stderr)
    if as_proc:
        return process
    stdout, stderr = process.communicate()
    exit_code = process.returncode
    return exit_code, stdout, stderr


def venv_exists(venv: Union[str, None], debug: bool = False) -> bool:
    """
    Determine whether a virtual environment has been created.
    """
    target_path = venv_target_path(venv, allow_nonexistent=True, debug=debug)
    return target_path.exists()


def venv_target_path(
        venv: Union[str, None],
        allow_nonexistent: bool = False,
        debug: bool = False,
    ) -> 'pathlib.Path':
    """
    Return a virtual environment's site-package path.

    Parameters
    ----------
    venv: Union[str, None]
        The virtual environment for which a path should be returned.

    allow_nonexistent: bool, default False
        If `True`, return a path even if it does not exist.

    Returns
    -------
    The `pathlib.Path` object for the virtual environment's path.

    """
    import os, sys, platform, pathlib, site
    from meerschaum.config._paths import VIRTENV_RESOURCES_PATH

    ### Check sys.path for a user-writable site-packages directory.
    if venv is None:
        if not inside_venv():
            site_path = pathlib.Path(site.getusersitepackages())
            ### Allow for dist-level paths (running as root).
            if not site_path.exists():
                if platform.system() == 'Windows' or os.geteuid() == 0:
                    for possible_dist in reversed(site.getsitepackages()):
                        dist_path = pathlib.Path(possible_dist)
                        if not dist_path.exists():
                            continue
                        return dist_path
                    
                    raise EnvironmentError("Could not determine the dist-packages directory.")

            return site_path

    venv_root_path = (
        (VIRTENV_RESOURCES_PATH / venv)
        if venv is not None else pathlib.Path(sys.prefix)
    )
    target_path = venv_root_path

    ### Ensure 'lib' or 'Lib' exists.
    lib = 'lib' if platform.system() != 'Windows' else 'Lib'
    if not allow_nonexistent:
        if not venv_root_path.exists() or lib not in os.listdir(venv_root_path):
            print(f"Failed to find lib directory for virtual environment '{venv}'.")
            import traceback
            traceback.print_stack()
            sys.exit(1)
    target_path = target_path / lib

    ### Check if a 'python3.x' folder exists.
    python_folder = 'python' + str(sys.version_info.major) + '.' + str(sys.version_info.minor)
    if target_path.exists():
        target_path = (
            (target_path / python_folder) if python_folder in os.listdir(target_path)
            else target_path
        )
    else:
        target_path = (
            (target_path / python_folder) if platform.system() != 'Windows'
            else target_path
        )

    ### Ensure 'site-packages' exists.
    if allow_nonexistent or 'site-packages' in os.listdir(target_path): ### Windows
        target_path = target_path / 'site-packages'
    else:
        import traceback
        traceback.print_stack()
        print(f"Failed to find site-packages directory for virtual environment '{venv}'.")
        print("This may be because you are using a different Python version.")
        print("Try deleting the following directory and restarting Meerschaum:")
        print(VIRTENV_RESOURCES_PATH)
        sys.exit(1)

    return target_path


def inside_venv() -> bool:
    """
    Determine whether current Python interpreter is running inside a virtual environment.
    """
    import sys
    return (
        hasattr(sys, 'real_prefix') or (
            hasattr(sys, 'base_prefix')
                and sys.base_prefix != sys.prefix
        )
    )


def get_venvs() -> List[str]:
    """
    Return a list of all the virtual environments.
    """
    import os
    from meerschaum.config._paths import VIRTENV_RESOURCES_PATH
    venvs = []
    for filename in os.listdir(VIRTENV_RESOURCES_PATH):
        path = VIRTENV_RESOURCES_PATH / filename
        if not path.is_dir():
            continue
        if not venv_exists(filename):
            continue
        venvs.append(filename)
    return venvs


from meerschaum.utils.venv._Venv import Venv
