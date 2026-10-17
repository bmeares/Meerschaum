#! /usr/bin/env python
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

"""
Functions for managing packages and virtual environments reside here.
"""

from __future__ import annotations

import importlib.util, os, pathlib, re
from meerschaum.utils.typing import Any, List, SuccessTuple, Optional, Union, Tuple, Dict, Iterable
from meerschaum.utils.threading import Lock, RLock
from meerschaum.utils.packages._packages import (
    packages,
    all_packages,
    get_install_names,
    _MRSM_PACKAGE_ARCHIVES_PREFIX,
)
from meerschaum.utils.venv import (
    activate_venv,
    deactivate_venv,
    venv_executable,
    venv_exec,
    venv_exists,
    venv_target_path,
    inside_venv,
    Venv,
    init_venv,
)

_import_module = importlib.import_module
_import_hook_venv = None
_locks = {
    '_pkg_resources_get_distribution': RLock(),
    'import_versions': RLock(),
    '_checked_for_updates': RLock(),
    '_is_installed_first_check': RLock(),
    'emitted_pandas_warning': RLock(),
    'emitted_auto_install_warning': RLock(),
    '_install_thread_locks': RLock(),
}
_checked_for_updates = set()
_is_installed_first_check: Dict[Tuple[str, Optional[str], bool, bool], bool] = {}
_install_thread_locks = {}
_install_lock_depths = {}
emitted_auto_install_warning = False


#: Environment variable that turns off every implicit package installation (V4-002).
NO_AUTO_INSTALL_ENV_VAR = 'MRSM_NO_AUTO_INSTALL'


#: Packages that only style output (`rich`, ...): `attempt_import()` never installs them. The base
#: wheel has no dependencies, and the formatting helpers fall back to plain text without them.
_NEVER_AUTO_INSTALLED = frozenset({
    'rich', 'pygments', 'typing_extensions', 'colorama', 'more_termcolor',
})


def _auto_install_allowed() -> bool:
    """
    Return whether Meerschaum may install packages on its own.

    This is the single guard behind `MRSM_NO_AUTO_INSTALL`. An install is *implicit* when it is
    reached from an import or a dependency precheck (`attempt_import()`, `is_installed()`,
    `need_update()`, bootstrapping `uv`, `pip`, `wheel`, ...) and *explicit* when the
    caller asked for it (`pip_install()` from user code, `mrsm install packages`,
    `install required`). Implicit installs call this; explicit ones are always allowed, but do not
    bootstrap installers of their own while the variable is set.
    """
    return os.environ.get(NO_AUTO_INSTALL_ENV_VAR, '').lower() not in ('1', 'true', 'yes')


def _get_uv_bin() -> Optional[str]:
    """Find uv without importing or installing any bootstrap dependencies."""
    import shutil
    try:
        import uv
        return str(uv.find_uv_bin())
    except (ImportError, FileNotFoundError):
        return shutil.which('uv')


def _get_pip_install_target_path(venv: Optional[str] = 'mrsm') -> pathlib.Path:
    """Resolve an installation target without creating it."""
    if venv is not None:
        return venv_target_path(venv, allow_nonexistent=True)

    import sys
    return pathlib.Path(sys.prefix)


def get_pip_install_lock_path(venv: Optional[str] = 'mrsm') -> pathlib.Path:
    """Return the cross-process package installation lock path for an environment."""
    import hashlib
    import tempfile
    target_path = _get_pip_install_target_path(venv).resolve()
    target_key = os.path.normcase(str(target_path))
    target_hash = hashlib.sha256(target_key.encode('utf-8')).hexdigest()[:16]
    return (
        pathlib.Path(tempfile.gettempdir())
        / 'meerschaum-package-installs'
        / (target_hash + '.lock')
    )


def _pip_install_lock(venv: Optional[str] = 'mrsm'):
    """Serialize package mutations across threads and processes for an environment."""
    from contextlib import contextmanager

    @contextmanager
    def _locked():
        from threading import get_ident
        from meerschaum.utils.locks import InterProcessLock

        lock_path = get_pip_install_lock_path(venv)
        lock_key = str(lock_path)
        with _locks['_install_thread_locks']:
            thread_lock = _install_thread_locks.setdefault(lock_key, RLock())

        with thread_lock:
            depth_key = (get_ident(), lock_key)
            depth = _install_lock_depths.get(depth_key, 0)
            _install_lock_depths[depth_key] = depth + 1
            try:
                if depth:
                    yield
                    return
                lock_path.parent.mkdir(parents=True, exist_ok=True)
                with InterProcessLock(lock_path):
                    yield
            finally:
                if depth:
                    _install_lock_depths[depth_key] -= 1
                else:
                    _install_lock_depths.pop(depth_key, None)

    return _locked()


def _locked_pip_install(func):
    """Lock package mutations while preserving `pip_install()`'s public signature."""
    import functools

    @functools.wraps(func)
    def _wrapped(*args, **kw):
        if kw.get('dry_run', False):
            return func(*args, **kw)
        if kw.get('_implicit', False) and not _auto_install_allowed():
            return False
        with _pip_install_lock(kw.get('venv', 'mrsm')):
            return func(*args, **kw)

    return _wrapped


def get_module_path(
    import_name: str,
    venv: Optional[str] = 'mrsm',
    debug: bool = False,
    _try_install_name_on_fail: bool = True,
) -> Union[pathlib.Path, None]:
    """
    Get a module's path without importing.
    """
    import site
    if debug:
        from meerschaum.utils.debug import dprint
    if not _try_install_name_on_fail:
        install_name = _import_to_install_name(import_name, with_version=False)
        install_name_lower = install_name.lower().replace('-', '_')
        import_name_lower = install_name_lower
    else:
        import_name_lower = import_name.lower().replace('-', '_')

    vtp = venv_target_path(venv, allow_nonexistent=True, debug=debug)
    if not vtp.exists():
        if debug:
            dprint(
                (
                    "Venv '{venv}' does not exist, cannot import "
                    + f"'{import_name}'."
                ),
                color = False,
            )
        return None

    venv_target_candidate_paths = [vtp]
    if venv is None:
        site_user_packages_dirs = [
            pathlib.Path(site.getusersitepackages())
        ] if not inside_venv() else []
        site_packages_dirs = [pathlib.Path(path) for path in site.getsitepackages()]

        paths_to_add = [
            path
            for path in site_user_packages_dirs + site_packages_dirs
            if path not in venv_target_candidate_paths
        ]
        venv_target_candidate_paths += paths_to_add

    candidates = []
    for venv_target_candidate in venv_target_candidate_paths:
        try:
            file_names = os.listdir(venv_target_candidate)
        except FileNotFoundError:
            continue
        for file_name in file_names:
            file_name_lower = file_name.lower().replace('-', '_')
            if not file_name_lower.startswith(import_name_lower):
                continue
            if file_name.endswith('dist_info'):
                continue
            file_path = venv_target_candidate / file_name

            ### Most likely: Is a directory with __init__.py
            if file_name_lower == import_name_lower and file_path.is_dir():
                init_path = file_path / '__init__.py'
                if init_path.exists():
                    candidates.append(init_path)

            ### May be a standalone .py file.
            elif file_name_lower == import_name_lower + '.py':
                candidates.append(file_path)

            ### Compiled wheels (e.g. pyodbc)
            elif file_name_lower.startswith(import_name_lower + '.'):
                candidates.append(file_path)

    if len(candidates) == 1:
        return candidates[0]

    if not candidates:
        if _try_install_name_on_fail:
            return get_module_path(
                import_name, venv=venv, debug=debug,
                _try_install_name_on_fail=False
            )
        return None

    specs_paths = []
    for candidate_path in candidates:
        spec = importlib.util.spec_from_file_location(import_name, str(candidate_path))
        if spec is not None:
            return candidate_path
    
    return None


def manually_import_module(
    import_name: str,
    venv: Optional[str] = 'mrsm',
    check_update: bool = True,
    check_pypi: bool = False,
    install: bool = True,
    split: bool = True,
    warn: bool = True,
    color: bool = True,
    debug: bool = False,
    use_sys_modules: bool = True,
) -> Union['ModuleType', None]:
    """
    Manually import a module from a virtual environment (or the base environment).

    Parameters
    ----------
    import_name: str
        The name of the module.
        
    venv: Optional[str], default 'mrsm'
        The virtual environment to read from.

    check_update: bool, default True
        If `True`, examine whether the available version of the package meets the required version.

    check_pypi: bool, default False
        If `True`, check PyPI for updates before importing.

    install: bool, default True
        If `True`, install the package if it's not installed or needs an update.

    split: bool, default True
        If `True`, split `import_name` on periods to get the package name.

    warn: bool, default True
        If `True`, raise a warning if the package cannot be imported.

    color: bool, default True
        If `True`, use color output for debug and warning text.

    debug: bool, default False
        Verbosity toggle.

    use_sys_modules: bool, default True
        If `True`, return the module in `sys.modules` if it exists.
        Otherwise continue with manually importing.

    Returns
    -------
    The specified module or `None` if it can't be imported.

    """
    import sys
    _previously_imported = import_name in sys.modules
    if _previously_imported and use_sys_modules:
        return sys.modules[import_name]

    from meerschaum.utils.warnings import warn as warn_function
    import warnings
    root_name = import_name.split('.')[0] if split else import_name
    install_name = _import_to_install_name(root_name)

    root_path = get_module_path(root_name, venv=venv)
    if root_path is None:
        return None

    mod_path = root_path
    if mod_path.is_dir():
        for _dir in import_name.split('.')[:-1]:
            mod_path = mod_path / _dir
            possible_end_module_filename = import_name.split('.')[-1] + '.py'
            try:
                mod_path = (
                    (mod_path / possible_end_module_filename)
                    if possible_end_module_filename in os.listdir(mod_path)
                    else (
                        mod_path / import_name.split('.')[-1] / '__init__.py'
                    )
                )
            except Exception:
                mod_path = None

    spec = (
        importlib.util.find_spec(import_name)
        if mod_path is None or not mod_path.exists()
        else importlib.util.spec_from_file_location(import_name, str(mod_path))
    )
    root_spec = (
        importlib.util.find_spec(root_name)
        if not root_path.exists()
        else importlib.util.spec_from_file_location(root_name, str(root_path))
    )

    ### Check for updates before importing.
    _version = (
        determine_version(
            pathlib.Path(root_spec.origin),
            import_name=root_name, venv=venv, debug=debug
        ) if root_spec is not None and root_spec.origin is not None else None
    )

    if _version is not None:
        if check_update:
            if need_update(
                None,
                import_name=root_name,
                version=_version,
                check_pypi=check_pypi,
                debug=debug,
            ):
                ### An installed package that is too old (or has a newer release) is never
                ### replaced behind the user's back: say how to upgrade it.
                if warn:
                    warn_function(
                        f"The installed '{root_name}' ({_version}) is older than what is "
                        f"required or available. Upgrade it with `mrsm upgrade packages "
                        f"{root_name}`.",
                        stack=False,
                        color=False,
                    )
                spec = (
                    importlib.util.find_spec(import_name)
                    if mod_path is None or not mod_path.exists()
                    else importlib.util.spec_from_file_location(import_name, str(mod_path))
                )

    if spec is None:
        try:
            mod = _import_module(import_name)
        except Exception:
            mod = None
        return mod

    with Venv(venv, debug=debug):
        mod = importlib.util.module_from_spec(spec)
        old_sys_mod = sys.modules.get(import_name, None)
        sys.modules[import_name] = mod

        try:
            with warnings.catch_warnings():
                warnings.filterwarnings('ignore', 'The NumPy')
                spec.loader.exec_module(mod)
        except Exception:
            pass
        mod = _import_module(import_name)
        if old_sys_mod is not None:
            sys.modules[import_name] = old_sys_mod
        else:
            del sys.modules[import_name]

    return mod


def _import_to_install_name(import_name: str, with_version: bool = True) -> str:
    """
    Try to translate an import name to an installation name.
    """
    install_name = all_packages.get(import_name, import_name)
    if with_version:
        return install_name
    return get_install_no_version(install_name)


def _import_to_dir_name(import_name: str) -> str:
    """
    Translate an import name to the package name in the sites-packages directory.
    """
    import re
    return re.split(
        r'[<>=\[]', all_packages.get(import_name, import_name)
    )[0].replace('-', '_').lower() 


def _install_to_import_name(install_name: str) -> str:
    """
    Translate an installation name to a package's import name.
    """
    _install_no_version = get_install_no_version(install_name)
    return get_install_names().get(_install_no_version, _install_no_version)


def get_install_no_version(install_name: str) -> str:
    """
    Strip the version information from the install name.
    """
    import re
    return re.split(r'[\[=<>,! \]]', install_name)[0]


import_versions = {}
def determine_version(
    path: pathlib.Path,
    import_name: Optional[str] = None,
    venv: Optional[str] = 'mrsm',
    search_for_metadata: bool = True,
    split: bool = True,
    warn: bool = False,
    debug: bool = False,
) -> Union[str, None]:
    """
    Determine a module's `__version__` string from its filepath.
    
    First it searches for pip metadata, then it attempts to import the module in a subprocess.

    Parameters
    ----------
    path: pathlib.Path
        The file path of the module.

    import_name: Optional[str], default None
        The name of the module. If omitted, it will be determined from the file path.
        Defaults to `None`.

    venv: Optional[str], default 'mrsm'
        The virtual environment of the Python interpreter to use if importing is necessary.

    search_for_metadata: bool, default True
        If `True`, search the pip site_packages directory (assumed to be the parent)
        for the corresponding dist-info directory.

    warn: bool, default True
        If `True`, raise a warning if the module fails to import in the subprocess.

    split: bool, default True
        If `True`, split the determined import name by periods to get the room name.

    Returns
    -------
    The package's version string if available or `None`.
    If multiple versions are found, it will trigger an import in a subprocess.

    """
    with _locks['import_versions']:
        if venv not in import_versions:
            import_versions[venv] = {}
    import os
    old_cwd = os.getcwd()
    from meerschaum.utils.warnings import warn as warn_function
    if import_name is None:
        import_name = path.parent.stem if path.stem == '__init__' else path.stem
        import_name = import_name.split('.')[0] if split else import_name
    if import_name in import_versions[venv]:
        return import_versions[venv][import_name]
    _version = None
    module_parent_dir = (
        path.parent.parent if path.stem == '__init__' else path.parent
    ) if path is not None else venv_target_path(venv, allow_nonexistent=True, debug=debug)

    if not module_parent_dir.exists():
        return None

    installed_dir_name = _import_to_dir_name(import_name)
    clean_installed_dir_name = installed_dir_name.lower().replace('-', '_')

    ### First, check if a dist-info directory exists.
    _found_versions = []
    if search_for_metadata:
        try:
            filenames = os.listdir(module_parent_dir)
        except FileNotFoundError:
            filenames = []
        for filename in filenames:
            if not filename.endswith('.dist-info'):
                continue
            filename_lower = filename.lower()
            if not filename_lower.startswith(clean_installed_dir_name + '-'):
                continue
            _v = filename.replace('.dist-info', '').split("-")[-1]
            _found_versions.append(_v)

    if len(_found_versions) == 1:
        _version = _found_versions[0]
        with _locks['import_versions']:
            import_versions[venv][import_name] = _version
        return _found_versions[0]

    if not _found_versions:
        try:
            import importlib.metadata as importlib_metadata
        except ImportError:
            importlib_metadata = attempt_import(
                'importlib_metadata',
                debug=debug, check_update=False, precheck=False,
                color=False, check_is_installed=False, lazy=False,
            )
        try:
            os.chdir(module_parent_dir)
            _version = importlib_metadata.metadata(import_name)['Version']
        except Exception:
            _version = None
        finally:
            os.chdir(old_cwd)

        if _version is not None:
            with _locks['import_versions']:
                import_versions[venv][import_name] = _version
            return _version

    if debug:
        print(f'Found multiple versions for {import_name}: {_found_versions}')

    module_parent_dir_str = module_parent_dir.as_posix()

    ### Not a pip package, so let's try importing the module directly (in a subprocess).
    _no_version_str = 'no-version'
    code = (
        f"import sys, importlib; sys.path.insert(0, '{module_parent_dir_str}');\n"
        + f"module = importlib.import_module('{import_name}');\n"
        + "try:\n"
        + "  print(module.__version__ , end='')\n"
        + "except:\n"
        + f"  print('{_no_version_str}', end='')"
    )
    exit_code, stdout_bytes, stderr_bytes = venv_exec(
        code, venv=venv, with_extras=True, debug=debug
    )
    stdout, stderr = stdout_bytes.decode('utf-8'), stderr_bytes.decode('utf-8')
    _version = stdout.split('\n')[-1] if exit_code == 0 else None
    _version = _version if _version != _no_version_str else None

    if _version is None:
        _version = _get_package_metadata(import_name, venv).get('version', None)
    if _version is None and warn:
        warn_function(
            f"Failed to determine a version for '{import_name}':\n{stderr}",
            stack = False
        )

    ### If `__version__` doesn't exist, return `None`.
    import_versions[venv][import_name] = _version
    return _version


def _get_package_metadata(import_name: str, venv: Optional[str]) -> Dict[str, str]:
    """
    Read installed distribution metadata with the standard library.
    This is useful for getting a version when no `__version__` is defined
    and multiple versions are installed.

    Parameters
    ----------
    import_name: str
        The package's import or installation name.

    venv: Optional[str]
        The virtual environment which contains the package.

    Returns
    -------
    A dictionary of distribution metadata with lowercase keys.
    """
    from importlib import metadata
    install_name = _import_to_install_name(import_name, with_version=False)
    if install_name.startswith(_MRSM_PACKAGE_ARCHIVES_PREFIX):
        return {}
    if venv is None:
        try:
            distributions = [metadata.distribution(install_name)]
        except metadata.PackageNotFoundError:
            return {}
    else:
        target = venv_target_path(venv, allow_nonexistent=True)
        if not target.exists():
            return {}
        distributions = metadata.distributions(path=[str(target)])

    normalize = lambda name: re.sub(r'[-_.]+', '-', name).lower()
    for distribution in distributions:
        meta = distribution.metadata
        if normalize(meta.get('Name', '')) != normalize(install_name):
            continue
        return {k.lower(): v for k, v in meta.items() if v and 'UNKNOWN' not in v}
    return {}


def _version_satisfies(
    version: str,
    required_version: str,
    debug: bool = False,
) -> Optional[bool]:
    """
    Match an installed version against a PEP 440 requirement without installing anything.

    Uses the Rust comparison (`meerschaum._core.version_satisfies`), then `packaging` if it is
    importable. Returns `None` when neither can answer.
    """
    if debug:
        from meerschaum.utils.debug import dprint
    required_version = required_version.strip()
    if not required_version:
        return True

    try:
        from meerschaum import _core
        satisfied = _core.version_satisfies(version, required_version)
    except (ImportError, AttributeError):
        satisfied = None
    if satisfied is not None:
        return satisfied

    specifiers, packaging_version = attempt_import(
        'packaging.specifiers',
        'packaging.version',
        install=False,
        warn=False,
        lazy=False,
        check_update=False,
    )
    if specifiers is not None and packaging_version is not None:
        try:
            return specifiers.SpecifierSet(required_version, prereleases=True).contains(
                packaging_version.Version(version)
            )
        except Exception as e:
            if debug:
                dprint(f"`packaging` could not match '{version}' to '{required_version}': {e}")

    if debug:
        dprint(
            f"No version parser is available to match '{version}' to '{required_version}'; "
            + "assuming the requirement is satisfied."
        )
    return None


def need_update(
    package: Optional['ModuleType'] = None,
    install_name: Optional[str] = None,
    import_name: Optional[str] = None,
    version: Optional[str] = None,
    check_pypi: bool = False,
    split: bool = True,
    color: bool = True,
    debug: bool = False,
    _run_determine_version: bool = True,
) -> bool:
    """
    Check if a Meerschaum dependency needs an update.
    Returns a bool for whether or not a package needs to be updated.

    Parameters
    ----------
    package: 'ModuleType'
        The module of the package to be updated.

    install_name: Optional[str], default None
        If provided, use this string to determine the required version.
        Otherwise use the install name defined in `meerschaum.utils.packages._packages`.

    import_name:
        If provided, override the package's `__name__` string.

    version: Optional[str], default None
        If specified, override the package's `__version__` string.

    check_pypi: bool, default False
        If `True`, check pypi.org for updates.
        Defaults to `False`.

    split: bool, default True
        If `True`, split the module's name on periods to detrive the root name.
        Defaults to `True`.

    color: bool, default True
        If `True`, format debug output.
        Defaults to `True`.

    debug: bool, default True
        Verbosity toggle.

    Returns
    -------
    A bool indicating whether the package requires an update.

    """
    if debug:
        from meerschaum.utils.debug import dprint
    from meerschaum.utils.warnings import warn as warn_function
    import re
    root_name = (
        package.__name__.split('.')[0] if split else package.__name__
    ) if import_name is None else (
        import_name.split('.')[0] if split else import_name
    )
    install_name = install_name or _import_to_install_name(root_name)
    auto_install_allowed = _auto_install_allowed()
    ### Asking PyPI for the latest release is an implicit network request.
    check_pypi = check_pypi and auto_install_allowed
    with _locks['_checked_for_updates']:
        if install_name in _checked_for_updates:
            return False
        _checked_for_updates.add(install_name)

    _install_no_version = get_install_no_version(install_name)
    required_version = (
        install_name
        .replace(_install_no_version, '')
    )
    if ']' in required_version:
        required_version = required_version.split(']')[1]

    ### No minimum version was specified, and we're not going to check PyPI.
    if not required_version and not check_pypi:
        return False

    ### NOTE: Sometimes (rarely), we depend on a development build of a package.
    if '.dev' in required_version:
        required_version = required_version.split('.dev')[0]
    if version and '.dev' in version:
        version = version.split('.dev')[0]

    try:
        if not version:
            if not _run_determine_version:
                version = determine_version(
                    pathlib.Path(package.__file__),
                    import_name=root_name, warn=False, debug=debug
                )
        if version is None:
            return False
    except Exception as e:
        if debug:
            dprint(str(e), color=color)
            dprint("No version could be determined from the installed package.", color=color)
        return False
    raw_version = version
    split_version = version.split('.')
    last_part = split_version[-1]
    if len(split_version) == 2:
        version = '.'.join(split_version) + '.0'
    elif 'dev' in last_part or 'rc' in last_part:
        tag = 'dev' if 'dev' in last_part else 'rc'
        last_sep = '-'
        if not last_part.startswith(tag):
            last_part = f'-{tag}'.join(last_part.split(tag))
            last_sep = '.'
        version = '.'.join(split_version[:-1]) + last_sep + last_part
    elif len(split_version) > 3:
        version = '.'.join(split_version[:3])

    if check_pypi:
        ### Check PyPI for updates
        update_checker, packaging_version = attempt_import(
            'update_checker', 'packaging.version', lazy=False, check_update=False, debug=debug
        )
        checker = update_checker.UpdateChecker()
        result = checker.check(_install_no_version, version)
    else:
        ### Skip PyPI and assume we can't be sure.
        result = None

    ### Compare PyPI's version with our own.
    if result is not None:
        ### We have a result from PyPI and a stated required version.
        if required_version:
            satisfied = _version_satisfies(result.available_version, required_version, debug=debug)
            return (not satisfied) if satisfied is not None else False

        ### If `check_pypi` and we don't have a required version, check if PyPI's version
        ### is newer than the installed version.
        else:
            return (
                packaging_version.parse(result.available_version) > 
                packaging_version.parse(version)
            )

    ### We might be depending on a prerelease.
    ### Sanity check that the required version is not greater than the installed version. 
    required_version = (
        required_version.replace(_MRSM_PACKAGE_ARCHIVES_PREFIX, '')
        .replace(' @ ', '').replace('wheels', '').replace('+mrsm', '').replace('/-', '')
        .replace('-py3-none-any.whl', '')
    )

    ### An unknown answer (neither side parses as PEP 440) must not trigger an install.
    satisfied = _version_satisfies(raw_version, required_version, debug=debug)
    return (not satisfied) if satisfied is not None else False


def get_pip(
    venv: Optional[str] = 'mrsm',
    color: bool = True,
    debug: bool = False,
) -> bool:
    """
    Download and run the get-pip.py script.

    Parameters
    ----------
    venv: Optional[str], default 'mrsm'
        The virtual environment into which to install `pip`.

    color: bool, default True
        If `True`, force color output.

    debug: bool, default False
        Verbosity toggle.

    Returns
    -------
    A bool indicating success.

    """
    import sys
    import subprocess
    import meerschaum.config.paths as paths
    from meerschaum.utils.misc import wget
    from meerschaum._internal.static import STATIC_CONFIG
    url = STATIC_CONFIG['system']['urls']['get-pip.py']
    dest = paths.CACHE_RESOURCES_PATH / 'get-pip.py'
    try:
        wget(url, dest, color=False, debug=debug)
    except Exception:
        print(f"Failed to fetch pip from '{url}'. Please install pip and restart Meerschaum.") 
        sys.exit(1)
    if venv is not None:
        init_venv(venv=venv, debug=debug)
    cmd_list = [venv_executable(venv=venv), dest.as_posix()] 
    return subprocess.call(cmd_list, env=_get_pip_os_env(color=color)) == 0


def get_pip_install_plan(
    *install_names: str,
    args: Optional[List[str]] = None,
    requirements_file_path: Union[pathlib.Path, str, None] = None,
    venv: Optional[str] = 'mrsm',
    _uninstall: bool = False,
    _install_uv_pip: bool = True,
    _use_uv_pip: bool = True,
    debug: bool = False,
) -> Dict[str, Any]:
    """Return a read-only summary of a package installation request."""
    try:
        import pip  # noqa: F401
        have_pip = True
    except ImportError:
        have_pip = False

    uv_bin = _get_uv_bin()

    use_uv_pip = bool(
        _use_uv_pip
        and uv_bin
        and is_uv_enabled()
    )
    requested_packages = [
        (
            get_install_no_version(install_name)
            if _uninstall or install_name.startswith(_MRSM_PACKAGE_ARCHIVES_PREFIX)
            else install_name
        )
        for install_name in install_names
    ]
    auto_install_enabled = _auto_install_allowed()
    return {
        'operation': 'uninstall' if _uninstall else 'install',
        'environment': venv,
        'target': str(_get_pip_install_target_path(venv)),
        'packages': requested_packages,
        'requirements_file': (
            str(pathlib.Path(requirements_file_path).resolve())
            if requirements_file_path is not None
            else None
        ),
        'args': list(args if args is not None else ([] if _uninstall else ['--upgrade'])),
        'installer': 'uv' if use_uv_pip else 'pip',
        'installer_available': bool(use_uv_pip or have_pip),
        'pip_fallback': bool(use_uv_pip and have_pip),
        'auto_install_enabled': auto_install_enabled,
        'would_bootstrap_uv': bool(
            not use_uv_pip
            and not uv_bin
            and have_pip
            and _install_uv_pip
            and is_uv_enabled()
        ),
        'lock_path': str(get_pip_install_lock_path(venv)),
    }


@_locked_pip_install
def pip_install(
    *install_names: str,
    args: Optional[List[str]] = None,
    requirements_file_path: Union[pathlib.Path, str, None] = None,
    venv: Optional[str] = 'mrsm',
    split: bool = False,
    check_update: bool = True,
    check_pypi: bool = True,
    check_wheel: bool = True,
    _uninstall: bool = False,
    _from_completely_uninstall: bool = False,
    _install_uv_pip: bool = True,
    _use_uv_pip: bool = True,
    _implicit: bool = False,
    color: bool = True,
    silent: bool = False,
    dry_run: bool = False,
    debug: bool = False,
) -> bool:
    """
    Install packages from PyPI with `pip`.

    When `MRSM_NO_AUTO_INSTALL` is set, calling this function directly still installs the
    requested packages, but never bootstraps `uv`, `pip`, `wheel` or `setuptools` on its own
    and never checks PyPI for a newer `pip`. Calls made on behalf of an import or a dependency
    precheck (`_implicit=True`) install nothing and return `False`.

    Parameters
    ----------
    *install_names: str
        The installation names of packages to be installed.
        This includes version restrictions.
        Use `_import_to_install_name()` to get the predefined `install_name` for a package
        from its import name.

    args: Optional[List[str]], default None
        A list of command line arguments to pass to `pip`.
        If not provided, default to `['--upgrade']` if `_uninstall` is `False`, else `[]`.

    requirements_file_path: Optional[pathlib.Path, str], default None
        If provided, append `['-r', '/path/to/requirements.txt']` to `args`.

    venv: str, default 'mrsm'
        The virtual environment to install into.

    split: bool, default False
        If `True`, split on periods and only install the root package name.

    check_update: bool, default True
        If `True`, check if the package requires an update.

    check_pypi: bool, default True
        If `True` and `check_update` is `True`, check PyPI for the latest version.

    check_wheel: bool, default True
        If `True`, check if `wheel` is available.

    _uninstall: bool, default False
        If `True`, uninstall packages instead.

    color: bool, default True
        If `True`, include color in debug text.

    silent: bool, default False
        If `True`, skip printing messages.

    dry_run: bool, default False
        If `True`, print the read-only installation plan without changing the environment.

    debug: bool, default False
        Verbosity toggle.

    Returns
    -------
    A bool indicating success.

    """
    import meerschaum.config.paths as paths
    from meerschaum._internal.static import STATIC_CONFIG
    from meerschaum.utils.warnings import warn
    auto_install_allowed = _auto_install_allowed()
    if dry_run:
        import json
        plan = get_pip_install_plan(
            *install_names,
            args=args,
            requirements_file_path=requirements_file_path,
            venv=venv,
            _uninstall=_uninstall,
            _install_uv_pip=_install_uv_pip,
            _use_uv_pip=_use_uv_pip,
            debug=debug,
        )
        if not silent:
            print(json.dumps(plan, indent=2))
        return True

    if args is None:
        args = ['--upgrade'] if not _uninstall else []
    ANSI = True if color else False
    daemon_env_var = STATIC_CONFIG['environment']['daemon_id']
    inside_daemon = daemon_env_var in os.environ
    if inside_daemon:
        silent = True

    _args = list(args)
    pip_venv = None
    try:
        import pip
        have_pip = True
    except ImportError:
        have_pip = False
    uv_bin = _get_uv_bin()
    have_uv_pip = uv_bin is not None

    if (
        auto_install_allowed
        and have_pip
        and not have_uv_pip
        and _install_uv_pip
        and is_uv_enabled()
    ):
        if not pip_install(
            'uv',
            venv=None,
            debug=debug,
            _install_uv_pip=False,
            check_update=False,
            check_pypi=False,
            check_wheel=False,
        ) and not silent:
            warn(
                f"Failed to install `uv` for virtual environment '{venv}'.",
                color=False,
            )
        # The bootstrap may have made uv available in this interpreter or on PATH.
        uv_bin = _get_uv_bin()

    use_uv_pip = (
        _use_uv_pip
        and uv_bin is not None
        and is_uv_enabled()
    )
    # uv does not need wheel, setuptools, or pip in either interpreter. Checking their
    # versions first recursively bootstraps the very installer we are trying to select.
    have_wheel = (
        venv_contains_package('wheel', venv=venv, debug=debug)
        if check_wheel and not use_uv_pip else False
    )

    import sys
    if not have_pip and not use_uv_pip:
        have_mrsm_pip = venv_contains_package('pip', venv='mrsm')
        if not have_mrsm_pip and not auto_install_allowed:
            if not silent:
                warn(
                    "Neither `uv` nor `pip` is available to install packages, "
                    + f"and {NO_AUTO_INSTALL_ENV_VAR} forbids downloading them.",
                    stack=False,
                    color=False,
                )
            return False
        if not have_mrsm_pip and not get_pip(venv=venv, color=color, debug=debug):
            import sys
            minor = sys.version_info.minor
            print(
                "\nFailed to import `pip` and `ensurepip`.\n"
                + "If you are running Ubuntu/Debian, "
                + f"you might need to install `python3.{minor}-distutils`:\n\n"
                + f"    sudo apt install python3.{minor}-pip python3.{minor}-venv\n\n"
                + "Please install pip and restart Meerschaum.\n\n"
                + "You can find instructions on installing `pip` here:\n"
                + "https://pip.pypa.io/en/stable/installing/"
            )
            sys.exit(1)

        pip = attempt_import('pip', lazy=False)
        pip_venv = 'mrsm'

    with Venv(venv, debug=debug):
        if venv is not None:
            if (
                '--ignore-installed' not in args
                and '-I' not in _args
                and not _uninstall
                and not use_uv_pip
            ):
                _args += ['--ignore-installed']
            if '--cache-dir' not in args and not _uninstall:
                cache_dir_path = paths.VIRTENV_RESOURCES_PATH / venv / 'cache'
                _args += ['--cache-dir', str(cache_dir_path)]

        if 'pip' not in ' '.join(_args) and not use_uv_pip:
            if check_update and not _uninstall and auto_install_allowed:
                pip = attempt_import('pip', venv=venv, install=False, debug=debug, lazy=False)
                if need_update(pip, check_pypi=check_pypi, debug=debug):
                    _args.append(all_packages['pip'])

        _args = (['install'] if not _uninstall else ['uninstall']) + _args

        if check_wheel and not _uninstall and not use_uv_pip and auto_install_allowed:
            if not have_wheel:
                setup_packages_to_install = (
                    ['setuptools', 'wheel', 'PyYAML']
                    + (['uv'] if is_uv_enabled() else [])
                )
                if not pip_install(
                    *setup_packages_to_install,
                    venv=venv,
                    check_update=False,
                    check_pypi=False,
                    check_wheel=False,
                    debug=debug,
                    _install_uv_pip=False,
                ) and not silent:
                    from meerschaum.utils.misc import items_str
                    warn(
                        (
                            f"Failed to install {items_str(setup_packages_to_install)} for virtual "
                            + f"environment '{venv}'."
                        ),
                        color=False,
                    )

        if requirements_file_path is not None:
            _args.append('-r')
            _args.append(pathlib.Path(requirements_file_path).resolve().as_posix())

        if not ANSI and '--no-color' not in _args:
            _args.append('--no-color')

        if '--no-input' not in _args and not use_uv_pip:
            _args.append('--no-input')

        if _uninstall and '-y' not in _args and not use_uv_pip:
            _args.append('-y')

        ### `uv pip uninstall` has no `-y` (it never prompts) and rejects it.
        if _uninstall and use_uv_pip:
            _args = [arg for arg in _args if arg not in ('-y', '--yes')]

        if '--no-warn-conflicts' not in _args and not _uninstall and not use_uv_pip:
            _args.append('--no-warn-conflicts')

        if '--disable-pip-version-check' not in _args and not use_uv_pip:
            _args.append('--disable-pip-version-check')

        if '--target' not in _args and '-t' not in _args and not (not use_uv_pip and _uninstall):
            if venv is not None:
                vtp = venv_target_path(venv, allow_nonexistent=True, debug=debug)
                if not vtp.exists():
                    if not init_venv(venv, force=True):
                        vtp.mkdir(parents=True, exist_ok=True)
                _args += ['--target', venv_target_path(venv, debug=debug).as_posix()]
        elif (
            '--target' not in _args
                and '-t' not in _args
                and not inside_venv()
                and not _uninstall
                and not use_uv_pip
        ):
            _args.append('--user')

        if venv is None and '--break-system-packages' not in _args:
            _args.append('--break-system-packages')

        if debug:
            if '-v' not in _args or '-vv' not in _args or '-vvv' not in _args:
                if use_uv_pip:
                    _args.append('--verbose')
        else:
            if '-q' not in _args or '-qq' not in _args or '-qqq' not in _args:
                pass

        _packages = [
            (
                get_install_no_version(install_name)
                if _uninstall or install_name.startswith(_MRSM_PACKAGE_ARCHIVES_PREFIX)
                else install_name
            )
            for install_name in install_names
        ]
        msg = "Installing packages:" if not _uninstall else "Uninstalling packages:"
        for p in _packages:
            msg += f'\n  - {p}'
        if not silent:
            print(msg)

        if _uninstall and not _from_completely_uninstall and not use_uv_pip:
            for install_name in _packages:
                _install_no_version = get_install_no_version(install_name)
                if _install_no_version in ('pip', 'wheel', 'uv'):
                    continue
                if not completely_uninstall_package(
                    _install_no_version,
                    venv=venv,
                    debug=debug,
                ) and not silent:
                    warn(
                        f"Failed to clean up package '{_install_no_version}'.",
                    )

        ### NOTE: Only append the `--prerelease=allow` flag if we explicitly depend on a prerelease.
        if use_uv_pip:
            _args.insert(0, 'pip')
            if not _uninstall and get_prerelease_dependencies(_packages):
                _args.append('--prerelease=allow')

        rc = run_python_package(
            ('pip' if not use_uv_pip else 'uv'),
            _args + _packages,
            venv=pip_venv,
            env=_get_pip_os_env(color=color),
            debug=debug,
        )
        if debug:
            print(f"{rc=}")
        success = rc == 0

    if success:
        with _locks['_is_installed_first_check']:
            _is_installed_first_check.clear()
        _update_request_ledger(_packages, venv, _uninstall)

    msg = (
        "Successfully " + ('un' if _uninstall else '') + "installed packages." if success 
        else "Failed to " + ('un' if _uninstall else '') + "install packages."
    )
    if not silent:
        print(msg)
    if debug and not silent:
        print('pip ' + ('un' if _uninstall else '') + 'install returned:', success)
    return success


def get_prerelease_dependencies(_packages: Optional[List[str]] = None):
    """
    Return a list of explicitly prerelease dependencies from a list of packages.
    """
    if _packages is None:
        _packages = list(all_packages.keys())
    prelrease_strings = ['dev', 'rc', 'a']
    prerelease_packages = []
    for install_name in _packages:
        _install_no_version = get_install_no_version(install_name)
        import_name = _install_to_import_name(install_name)
        install_with_version = _import_to_install_name(import_name)
        version_only = (
            install_with_version.lower().replace(_install_no_version.lower(), '')
            .split(']')[-1]
        )

        is_prerelease = False
        for prelrease_string in prelrease_strings:
            if prelrease_string in version_only:
                is_prerelease = True

        if is_prerelease:
            prerelease_packages.append(install_name)
    return prerelease_packages


def completely_uninstall_package(
    install_name: str,
    venv: str = 'mrsm',
    debug: bool = False,
) -> bool:
    """
    Continue calling `pip uninstall` until a package is completely
    removed from a virtual environment. 
    This is useful for dealing with multiple installed versions of a package.
    """
    attempts = 0
    _install_no_version = get_install_no_version(install_name)
    clean_install_no_version = _install_no_version.lower().replace('-', '_')
    installed_versions = []
    vtp = venv_target_path(venv, allow_nonexistent=True, debug=debug)
    if not vtp.exists():
        return True

    for file_name in os.listdir(vtp):
        if not file_name.endswith('.dist-info'):
            continue
        clean_dist_info = file_name.replace('-', '_').lower()
        if not clean_dist_info.startswith(clean_install_no_version):
            continue
        installed_versions.append(file_name)

    max_attempts = len(installed_versions)
    while attempts < max_attempts:
        if not venv_contains_package(
            _install_to_import_name(_install_no_version),
            venv=venv, debug=debug,
        ):
            return True
        if not pip_uninstall(
            _install_no_version,
            venv = venv,
            silent = (not debug),
            _from_completely_uninstall = True,
            debug = debug,
        ):
            return False
        attempts += 1
    return False


def pip_uninstall(
    *args, **kw
) -> bool:
    """
    Uninstall Python packages.
    This function is a wrapper around `pip_install()` but with `_uninstall` enforced as `True`.
    """
    return pip_install(*args, _uninstall=True, **{k: v for k, v in kw.items() if k != '_uninstall'})


def run_python_package(
    package_name: str,
    args: Optional[List[str]] = None,
    venv: Optional[str] = 'mrsm',
    cwd: Optional[str] = None,
    env: Optional[Dict[str, str]] = None,
    foreground: bool = False,
    as_proc: bool = False,
    capture_output: bool = False,
    debug: bool = False,
    **kw: Any,
) -> Union[int, subprocess.Popen, None]:
    """
    Runs an installed python package.
    E.g. Translates to `/usr/bin/python -m [package]`

    Parameters
    ----------
    package_name: str
        The Python module to be executed.

    args: Optional[List[str]], default None
        Additional command line arguments to be appended after `-m [package]`.

    venv: Optional[str], default 'mrsm'
        If specified, execute the Python interpreter from a virtual environment.

    cwd: Optional[str], default None
        If specified, change directories before starting the process.
        Defaults to `None`.

    env: Optional[Dict[str, str]], default None
        If specified, only use the provided dictionary for the environment variables.
        Defaults to `os.environ`.

    as_proc: bool, default False
        If `True`, return a `subprocess.Popen` object.

    capture_output: bool, default False
        If `as_proc` is `True`, capture stdout and stderr.

    foreground: bool, default False
        If `True`, start the subprocess as a foreground process.
        Defaults to `False`.

    kw: Any
        Additional keyword arguments to pass to `meerschaum.utils.process.run_process()`
        and by extension `subprocess.Popen()`.

    Returns
    -------
    Either a return code integer or a `subprocess.Popen` object
    (or `None` if a `KeyboardInterrupt` occurs and as_proc is `True`).
    """
    import sys
    import subprocess
    import traceback
    import meerschaum.config.paths as paths
    from meerschaum.utils.process import run_process
    from meerschaum.utils.warnings import warn
    if args is None:
        args = []
    old_cwd = os.getcwd()
    if cwd is not None:
        os.chdir(cwd)
    executable = venv_executable(venv=venv)
    venv_path = (paths.VIRTENV_RESOURCES_PATH / venv) if venv is not None else None
    env_dict = (env if isinstance(env, dict) else (os.environ or {})).copy()
    if venv_path is not None:
        env_dict.update({'VIRTUAL_ENV': venv_path.as_posix()})
    uv_bin = _get_uv_bin() if package_name == 'uv' else None
    command = (
        [uv_bin] if uv_bin is not None else [executable, '-m', str(package_name)]
    ) + [str(a) for a in args]
    if debug:
        print(command, file=sys.stderr)
    try:
        to_return = run_process(
            command,
            foreground=foreground,
            as_proc=as_proc,
            capture_output=capture_output,
            env=env_dict,
            **kw
        )
    except Exception:
        msg = f"Failed to execute {command}, will try again:\n{traceback.format_exc()}"
        warn(msg, color=False)
        stdout, stderr = (
            (None, None)
            if not capture_output
            else (subprocess.PIPE, subprocess.PIPE)
        )
        proc = subprocess.Popen(
            command,
            stdout=stdout,
            stderr=stderr,
            stdin=sys.stdin,
            env=env_dict,
        )
        to_return = proc if as_proc else proc.wait()
    except KeyboardInterrupt:
        to_return = 1 if not as_proc else None
    os.chdir(old_cwd)
    return to_return


def attempt_import(
    *names: str,
    lazy: bool = True,
    warn: bool = True,
    install: bool = True,
    venv: Optional[str] = 'mrsm',
    precheck: bool = True,
    split: bool = True,
    check_update: bool = False,
    check_pypi: bool = False,
    check_is_installed: bool = True,
    allow_outside_venv: bool = True,
    color: bool = True,
    debug: bool = False
) -> Any:
    """
    Raise a warning if packages are not installed; otherwise import and return modules.
    If `lazy` is `True`, return lazy-imported modules.
    
    Returns tuple of modules if multiple names are provided, else returns one module.
    
    Parameters
    ----------
    names: List[str]
        The packages to be imported.

    lazy: bool, default True
        If `True`, lazily load packages.

    warn: bool, default True
        If `True`, raise a warning if a package cannot be imported.

    install: bool, default True
        If `True`, attempt to install a missing package into the designated virtual environment.
        If `check_update` is True, install updates if available.

    venv: Optional[str], default 'mrsm'
        The virtual environment in which to search for packages and to install packages into.

    precheck: bool, default True
        If `True`, attempt to find module before importing (necessary for checking if modules exist
        and retaining lazy imports), otherwise assume lazy is `False`.

    split: bool, default True
        If `True`, split packages' names on `'.'`.

    check_update: bool, default False
        If `True` and `install` is `True`, install updates if the required minimum version
        does not match.

    check_pypi: bool, default False
        If `True` and `check_update` is `True`, check PyPI when determining whether
        an update is required.

    check_is_installed: bool, default True
        If `True`, check if the package is contained in the virtual environment.

    allow_outside_venv: bool, default True
        If `True`, search outside of the specified virtual environment
        if the package cannot be found.
        Setting to `False` will reinstall the package into a virtual environment, even if it
        is installed outside.

    color: bool, default True
        If `False`, do not print ANSI colors.

    Returns
    -------
    The specified modules. If they're not available and `install` is `True`, it will first
    download them into a virtual environment and return the modules.

    Examples
    --------
    >>> pandas, sqlalchemy = attempt_import('pandas', 'sqlalchemy')
    >>> pandas = attempt_import('pandas')

    """

    import importlib.util

    global emitted_auto_install_warning
    no_auto_install_env_var = NO_AUTO_INSTALL_ENV_VAR
    if not _auto_install_allowed():
        install = False

    ### Cosmetic packages are never installed on import: without them output is plain text.
    if any(name.split('.')[0] in _NEVER_AUTO_INSTALLED for name in names):
        install = False
        warn = False

    ### to prevent recursion, check if parent Meerschaum package is being imported
    if names == ('meerschaum',):
        return _import_module('meerschaum')

    if venv == 'mrsm' and _import_hook_venv is not None:
        if debug:
            print(f"Import hook for virtual environment '{_import_hook_venv}' is active.")
        venv = _import_hook_venv

    _warnings = _import_module('meerschaum.utils.warnings')
    warn_function = _warnings.warn

    def do_import(_name: str, **kw) -> Union['ModuleType', None]:
        ### A fully-imported module resolves from `sys.modules` regardless of the active venv,
        ### so skip activating the venv (which resolves and creates its directories).
        import sys
        loaded_module = sys.modules.get(_name, None)
        if (
            loaded_module is not None
            and not getattr(getattr(loaded_module, '__spec__', None), '_initializing', False)
        ):
            return loaded_module

        with Venv(venv=venv, debug=debug):
            ### determine the import method (lazy vs normal)
            from meerschaum.utils.misc import filter_keywords
            import_method = (
                _import_module if not lazy
                else lazy_import
            )
            try:
                mod = import_method(_name, **(filter_keywords(import_method, **kw)))
            except Exception as e:
                if warn:
                    import traceback
                    traceback.print_exception(type(e), e, e.__traceback__)
                    warn_function(
                        f"Failed to import module '{_name}'.\nException:\n{e}",
                        ImportWarning,
                        stacklevel = (5 if lazy else 4),
                        color = False,
                    )
                mod = None
        return mod

    modules = []
    for name in names:
        ### Check if package is a declared dependency.
        root_name = name.split('.')[0] if split else name
        install_name = _import_to_install_name(root_name)

        if install_name is None:
            install_name = root_name
            if warn and root_name != 'plugins':
                warn_function(
                    f"Package '{root_name}' is not declared in meerschaum.utils.packages.",
                    ImportWarning,
                    stacklevel = 3,
                    color = False
                )

        ### Determine if the package exists.
        if precheck is False:
            found_module = (
                do_import(
                    name, debug=debug, warn=False, venv=venv, color=color,
                    check_update=False, check_pypi=False, split=split,
                ) is not None
            )
        else:
            installed_cache_key = (name, venv, split, allow_outside_venv)
            if check_is_installed:
                with _locks['_is_installed_first_check']:
                    if not _is_installed_first_check.get(installed_cache_key, False):
                        package_is_installed = is_installed(
                            name,
                            venv = venv,
                            split = split,
                            allow_outside_venv = allow_outside_venv,
                            debug = debug,
                        )
                        if package_is_installed:
                            _is_installed_first_check[installed_cache_key] = True
                    else:
                        package_is_installed = True
            else:
                package_is_installed = venv_contains_package(
                    name,
                    venv=venv,
                    split=split,
                    debug=debug,
                )
            found_module = package_is_installed

        if not found_module:
            if install:
                with _locks['emitted_auto_install_warning']:
                    if warn and not emitted_auto_install_warning:
                        emitted_auto_install_warning = True
                        warn_function(
                            "Meerschaum is installing a missing runtime dependency. "
                            + f"Set {no_auto_install_env_var}=1 to disable automatic downloads.",
                            ImportWarning,
                            stacklevel=3,
                            color=False,
                        )
                if not pip_install(
                    install_name,
                    venv = venv,
                    split = False,
                    check_update = check_update,
                    color = color,
                    debug = debug,
                    _implicit = True,
                ):
                    if warn:
                        warn_function(
                            f"Failed to install '{install_name}'.",
                            ImportWarning,
                            stacklevel = 3,
                            color = False,
                        )
                else:
                    _record_import_request(install_name, venv)
            elif warn:
                ### Raise a warning if we can't find the package and install = False.
                warn_function(
                    (f"\n\nMissing package '{name}' from virtual environment '{venv}'; "
                     + "some features will not work correctly."
                     + (
                         f"\n\nUnset {no_auto_install_env_var} to allow package installation.\n"
                         if no_auto_install_env_var in os.environ
                         else "\n\nSet install=True when calling attempt_import.\n"
                     )),
                    ImportWarning,
                    stacklevel = 3,
                    color = False,
                )

        elif found_module:
            _record_import_request(install_name, venv, only_plugins=True)

        ### Do the import. Will be lazy if lazy=True.
        m = do_import(
            name, debug=debug, warn=warn, venv=venv, color=color,
            check_update=check_update, check_pypi=check_pypi, install=install, split=split,
        )
        modules.append(m)

    modules = tuple(modules)
    if len(modules) == 1:
        return modules[0]
    return modules


def _update_request_ledger(install_names: List[str], venv: Optional[str], uninstall: bool) -> None:
    """
    Keep `venvs/<venv>/.mrsm_requests.json` in step with a successful `pip_install()` (D20):
    record the caller (a plugin, else `meerschaum`) for installs, forget uninstalled packages.
    """
    if venv is None:
        return
    try:
        from meerschaum.utils.venv import _requests
        if uninstall:
            _requests.forget_requests(venv, install_names)
            return
        requester = _requests.get_requester()
        for install_name in install_names:
            if install_name.startswith(('-', '/', '.')) or '://' in install_name:
                continue
            _requests.record_request(venv, install_name, requester)
    except Exception:
        pass


def _record_import_request(
    install_name: str,
    venv: Optional[str],
    only_plugins: bool = False,
) -> None:
    """
    Record in `venvs/<venv>/.mrsm_requests.json` who asked for `install_name` (D20).
    With `only_plugins`, requests by Meerschaum itself are not recorded
    (used for packages that are already installed).
    """
    try:
        from meerschaum.utils.venv._requests import record_request, get_requester, CORE_REQUESTER
        requester = get_requester()
        if only_plugins and requester == CORE_REQUESTER:
            return
        record_request(venv, install_name, requester)
    except Exception:
        pass


def lazy_import(
    name: str,
    local_name: str = None,
    **kw
) -> meerschaum.utils.packages.lazy_loader.LazyLoader:
    """
    Lazily import a package.
    """
    from meerschaum.utils.packages.lazy_loader import LazyLoader
    if local_name is None:
        local_name = name
    return LazyLoader(
        local_name,
        globals(),
        name,
        **kw
    )


def pandas_name() -> str:
    """
    Return the configured name for `pandas`.
    
    Below are the expected possible values:

    - 'pandas'
    - 'modin.pandas'
    - 'dask.dataframe'

    """
    from meerschaum.config import get_config
    pandas_module_name = get_config('system', 'connectors', 'all', 'pandas', patch=True)
    if pandas_module_name == 'modin':
        pandas_module_name = 'modin.pandas'
    elif pandas_module_name == 'dask':
        pandas_module_name = 'dask.dataframe'

    return pandas_module_name


emitted_pandas_warning: bool = False
def import_pandas(
    debug: bool = False,
    lazy: bool = False,
    **kw
) -> 'ModuleType':
    """
    Quality-of-life function to attempt to import the configured version of `pandas`.
    """
    pandas_module_name = pandas_name()
    global emitted_pandas_warning

    if pandas_module_name != 'pandas':
        with _locks['emitted_pandas_warning']:
            if not emitted_pandas_warning:
                from meerschaum.utils.warnings import warn
                emitted_pandas_warning = True
                warn(
                    (
                        "You are using an alternative Pandas implementation "
                        + f"'{pandas_module_name}'"
                        + "\n   Features may not work as expected."
                    ),
                    stack=False,
                )

    pandas, pyarrow = attempt_import('pandas', 'pyarrow', debug=debug, lazy=False, **kw)
    pd = attempt_import(pandas_module_name, debug=debug, lazy=lazy, **kw)
    _warn_if_pandas_is_too_old(pandas)
    return pd


_emitted_old_pandas_warning = False


def _warn_if_pandas_is_too_old(pandas: Any) -> None:
    """Warn once per process that the installed pandas is older than the `pandas` extra's."""
    global _emitted_old_pandas_warning
    version = getattr(pandas, '__version__', None)
    if _emitted_old_pandas_warning or not version:
        return
    from meerschaum.utils.packages._packages import all_packages
    required = (all_packages.get('pandas', 'pandas>=3.0.0').split(']')[-1]).strip()
    if _version_satisfies(version, required) is not False:
        return
    _emitted_old_pandas_warning = True
    from meerschaum.utils.warnings import warn
    warn(
        f"The installed pandas {version} is older than the {required} Meerschaum requires: "
        "results (dtypes, datetime units) may differ. Upgrade it with "
        "`mrsm upgrade packages pandas`.",
        stack=False,
    )


def import_rich(
    lazy: bool = True,
    debug: bool = False,
    **kw: Any
) -> 'ModuleType':
    """
    Quality of life function for importing `rich`.
    """
    from meerschaum.utils.formatting import ANSI, UNICODE
    ## need typing_extensions for `from rich import box`
    typing_extensions = attempt_import(
        'typing_extensions', lazy=False, debug=debug
    )
    pygments = attempt_import(
        'pygments', lazy=False,
    )
    rich = attempt_import(
        'rich', lazy=lazy,
        **kw
    )
    return rich


def _dash_less_than_2(**kw) -> bool:
    dash = attempt_import('dash', **kw)
    if dash is None:
        return None
    packaging_version = attempt_import('packaging.version', **kw)
    return (
        packaging_version.parse(dash.__version__) < 
        packaging_version.parse('2.0.0')
    )


def import_dcc(warn=False, **kw) -> 'ModuleType':
    """
    Import Dash Core Components (`dcc`).
    """
    return (
        attempt_import('dash_core_components', warn=warn, **kw)
        if _dash_less_than_2(warn=warn, **kw) else attempt_import('dash.dcc', warn=warn, **kw)
    )


def import_html(warn=False, **kw) -> 'ModuleType':
    """
    Import Dash HTML Components (`html`).
    """
    return (
        attempt_import('dash_html_components', warn=warn, **kw)
        if _dash_less_than_2(warn=warn, **kw)
        else attempt_import('dash.html', warn=warn, **kw)
    )


def get_modules_from_package(
    package: Any,
    names: bool = False,
    recursive: bool = False,
    lazy: bool = False,
    modules_venvs: bool = False,
    debug: bool = False
):
    """
    Find and import all modules in a package.
    
    Returns
    -------
    Either list of modules or tuple of lists.
    """
    from os.path import dirname, join, isfile, isdir, basename
    import glob

    pattern = '*' if recursive else '*.py'
    package_path = dirname(package.__file__ or package.__path__[0])
    module_names = glob.glob(join(package_path, pattern), recursive=recursive)
    _all = [
        basename(f)[:-3] if isfile(f) else basename(f)
        for f in module_names
            if ((isfile(f) and f.endswith('.py')) or isdir(f))
               and not f.endswith('__init__.py')
               and not f.endswith('__pycache__')
    ]

    if debug:
        from meerschaum.utils.debug import dprint
        dprint(str(_all))
    modules = []
    for module_name in [package.__name__ + "." + mod_name for mod_name in _all]:
        ### there's probably a better way than a try: catch but it'll do for now
        try:
            ### if specified, activate the module's virtual environment before importing.
            ### NOTE: this only considers the filename, so two modules from different packages
            ### may end up sharing virtual environments.
            if modules_venvs:
                activate_venv(module_name.split('.')[-1], debug=debug)
            m = lazy_import(module_name, debug=debug) if lazy else _import_module(module_name)
            modules.append(m)
        except Exception:
            import traceback
            traceback.print_exc()
        finally:
            if modules_venvs:
                deactivate_venv(module_name.split('.')[-1], debug=debug)
    if names:
        return _all, modules

    return modules


def import_children(
    package: Optional['ModuleType'] = None,
    package_name: Optional[str] = None,
    types : Optional[List[str]] = None,
    lazy: bool = True,
    recursive: bool = False,
    debug: bool = False
) -> List['ModuleType']:
    """
    Import all functions in a package to its `__init__`.

    Parameters
    ----------
    package: Optional[ModuleType], default None
        Package to import its functions into.
        If `None` (default), use parent.

    package_name: Optional[str], default None
        Name of package to import its functions into
        If None (default), use parent.

    types: Optional[List[str]], default None
        Types of members to return.
        Defaults are `['method', 'builtin', 'class', 'function', 'package', 'module']`

    Returns
    -------
    A list of modules.
    """
    import sys, inspect

    if types is None:
        types = ['method', 'builtin', 'function', 'class', 'module']

    ### if package_name and package are None, use parent
    if package is None and package_name is None:
        package_name = inspect.stack()[1][0].f_globals['__name__']

    ### populate package or package_name from other other
    if package is None:
        package = sys.modules[package_name]
    elif package_name is None:
        package_name = package.__name__

    ### Set attributes in sys module version of package.
    ### Kinda like setting a dictionary
    ###   functions[name] = func
    modules = get_modules_from_package(package, recursive=recursive, lazy=lazy, debug=debug)
    _all, members = [], []
    objects = []
    for module in modules:
        _objects = []
        for ob in inspect.getmembers(module):
            for t in types:
                ### ob is a tuple of (name, object)
                if getattr(inspect, 'is' + t)(ob[1]):
                    _objects.append(ob)

        if 'module' in types:
            _objects.append((module.__name__.split('.')[0], module))
        objects += _objects
    for ob in objects:
        setattr(sys.modules[package_name], ob[0], ob[1])
        _all.append(ob[0])
        members.append(ob[1])

    if debug:
        from meerschaum.utils.debug import dprint
        dprint(str(_all))
    ### set __all__ for import *
    setattr(sys.modules[package_name], '__all__', _all)
    return members


_reload_module_cache = {}
def reload_package(
    package: str,
    skip_submodules: Optional[List[str]] = None,
    lazy: bool = False,
    debug: bool = False,
    **kw: Any
):
    """
    Recursively load a package's subpackages, even if they were not previously loaded.
    """
    import sys
    if isinstance(package, str):
        package_name = package
    else:
        try:
            package_name = package.__name__
        except Exception as e:
            package_name = str(package)

    skip_submodules = skip_submodules or []
    if 'meerschaum.utils.packages' not in skip_submodules:
        skip_submodules.append('meerschaum.utils.packages')
    def safeimport():
        subs = [
            m for m in sys.modules
            if m.startswith(package_name + '.')
        ]
        subs_to_skip = []
        for skip_mod in skip_submodules:
            for mod in subs:
                if mod.startswith(skip_mod):
                    subs_to_skip.append(mod)
                    continue

        subs = [m for m in subs if m not in subs_to_skip]
        for module_name in subs:
            _reload_module_cache[module_name] = sys.modules.pop(module_name, None)
        if not subs_to_skip:
            _reload_module_cache[package_name] = sys.modules.pop(package_name, None)

        return _import_module(package_name)

    return safeimport()


def reload_meerschaum(debug: bool = False) -> SuccessTuple:
    """
    Reload the currently loaded Meercshaum modules, refreshing plugins.
    """
    reload_package(
        'meerschaum',
        skip_submodules = [
            'meerschaum.utils.pool',
        ]
    )

    from meerschaum.plugins import reload_plugins
    reload_plugins(debug=debug)
    return True, "Success"


def is_installed(
    import_name: str,
    venv: Optional[str] = 'mrsm',
    split: bool = True,
    allow_outside_venv: bool = True,
    debug: bool = False,
) -> bool:
    """
    Check whether a package is installed.

    Parameters
    ----------
    import_name: str
        The import name of the module.

    venv: Optional[str], default 'mrsm'
        The venv in which to search for the module.

    split: bool, default True
        If `True`, split on periods to determine the root module name.

    allow_outside_venv: bool, default True
        If `True`, search outside of the specified virtual environment
        if the package cannot be found.

    Returns
    -------
    A bool indicating whether a package may be imported.
    """
    if debug:
        from meerschaum.utils.debug import dprint
    root_name = import_name.split('.')[0] if split else import_name
    import importlib.util
    with Venv(venv, debug=debug):
        try:
            spec_path = pathlib.Path(
                get_module_path(root_name, venv=venv, debug=debug)
                or
                (
                    importlib.util.find_spec(root_name).origin 
                    if venv is not None and allow_outside_venv
                    else None
                )
            )
        except (ModuleNotFoundError, ValueError, AttributeError, TypeError) as e:
            spec_path = None

        ### Whether the package is present, not whether it meets Meerschaum's minimum version:
        ### a too-old package is reported (`import_pandas()`, `attempt_import(check_update=True)`),
        ### never replaced by an implicit install.
        found = spec_path is not None

    return found


def venv_contains_package(
    import_name: str,
    venv: Optional[str] = 'mrsm',
    split: bool = True,
    debug: bool = False,
) -> bool:
    """
    Search the contents of a virtual environment for a package.
    """
    import site
    import pathlib
    root_name = import_name.split('.')[0] if split else import_name
    return get_module_path(root_name, venv=venv, debug=debug) is not None


def package_venv(package: 'ModuleType') -> Union[str, None]:
    """
    Inspect a package and return the virtual environment in which it presides.
    """
    import os
    import meerschaum.config.paths as paths
    if str(paths.VIRTENV_RESOURCES_PATH) not in package.__file__:
        return None
    return package.__file__.split(str(paths.VIRTENV_RESOURCES_PATH))[1].split(os.path.sep)[1]


def ensure_readline() -> 'ModuleType':
    """Make sure that the `readline` package is able to be imported."""
    import sys
    try:
        import readline
    except ImportError:
        readline = None

    if readline is None:
        import platform
        rl_name = "gnureadline" if platform.system() != 'Windows' else "pyreadline3"
        try:
            rl = attempt_import(
                rl_name,
                lazy=False,
                install=True,
                venv=None,
                warn=False,
            )
        except (ImportError, ModuleNotFoundError):
            if not pip_install(
                rl_name,
                args=['--upgrade', '--ignore-installed'],
                venv=None,
                _implicit=True,
            ):
                print(f"Unable to import {rl_name}!", file=sys.stderr)
                sys.exit(1)

    sys.modules['readline'] = readline
    return readline


def _get_pip_os_env(color: bool = True):
    """
    Return the environment variables context in which `pip` should be run.
    See PEP 668 for why we are overriding the environment.
    """
    import os, sys, platform
    python_bin_path = pathlib.Path(sys.executable)
    pip_os_env = os.environ.copy()
    path_str = pip_os_env.get('PATH', '') or ''
    path_sep = ':' if platform.system() != 'Windows' else ';'
    pip_os_env.update({
        'PIP_BREAK_SYSTEM_PACKAGES': 'true',
        'UV_BREAK_SYSTEM_PACKAGES': 'true',
        # Venv targets contain packages imported by this Python process. Resolve wheels
        # for its ABI even when uv discovers a project venv or inherits UV_PYTHON.
        'UV_PYTHON': sys.executable,
        ('FORCE_COLOR' if color else 'NO_COLOR'): '1',
    })
    if str(python_bin_path) not in path_str:
        pip_os_env['PATH'] = str(python_bin_path.parent) + path_sep + path_str

    return pip_os_env


def use_uv() -> bool:
    """
    Return whether `uv` is available and enabled.
    """
    from meerschaum.utils.misc import is_android
    if is_android():
        return False

    if not is_uv_enabled():
        return False

    return _get_uv_bin() is not None


def is_uv_enabled() -> bool:
    """
    Return whether the user has disabled `uv`.
    """
    from meerschaum.utils.misc import is_android
    if is_android():
        return False

    from meerschaum.config import get_config
    enabled = get_config('system', 'experimental', 'uv_pip')
    return enabled
