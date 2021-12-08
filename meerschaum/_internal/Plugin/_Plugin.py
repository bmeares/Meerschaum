#! /usr/bin/env python
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

"""
Plugin metadata class
"""

from __future__ import annotations
from meerschaum.utils.typing import (
    Dict,
    List,
    Optional,
    Any,
    SuccessTuple,
    Union,
)
from meerschaum.config import get_config
from meerschaum.config._paths import (
    PLUGINS_RESOURCES_PATH,
    PLUGINS_ARCHIVES_RESOURCES_PATH,
    PLUGINS_TEMP_RESOURCES_PATH
)
import os, pathlib
_tmpversion = None

class Plugin:
    """
    Handle packaging of Meerschaum plugins.
    """
    def __init__(
        self,
        name: str,
        version: Optional[str] = None,
        user_id: Optional[int] = None,
        attributes: Optional[Dict[str, Any]] = None,
        archive_path: Optional[pathlib.Path] = None,
        repo_connector: Optional['meerschaum.connectors.api.APIConnector'] = None,
    ):
        if attributes is None:
            attributes = {}
        self.name = name
        self.attributes = attributes
        self.user_id = user_id
        self._version = version

        if archive_path is None:
            self.archive_path = pathlib.Path(
                os.path.join(PLUGINS_ARCHIVES_RESOURCES_PATH, f'{self.name}.tar.gz')
            )
        else:
            self.archive_path = archive_path

        self.repo_connector = repo_connector
        if repo_connector is None:
            from meerschaum.connectors.parse import parse_repo_keys
            self.repo_connector = parse_repo_keys(get_config('meerschaum', 'default_repository'))

    @property
    def version(self):
        """
        Return the plugin's version string.
        """
        if self._version is None:
            try:
                self._version = self.module.__version__
            except Exception as e:
                self._version = None
        return self._version

    @property
    def module(self):
        """
        Return a Plugin's Python module.
        """
        if '_module' not in self.__dict__ or self._module is None:
            from meerschaum.plugins import import_plugins
            self._module = import_plugins(str(self), warn=False)
        return self._module

    @property
    def __file__(self) -> Union[str, None]:
        if '_module' not in self.__dict__:
            return None
        if self.module is None:
            return None
        return self.module.__file__

    def make_tar(self, debug : bool = False) -> pathlib.Path:
        """
        Compress the plugin's source files into a `.tar.gz` archive and return the archive's path.
        """
        import tarfile
        from meerschaum.utils.debug import dprint

        old_cwd = os.getcwd()
        os.chdir(PLUGINS_RESOURCES_PATH)

        if self.module is None and self.__file__ is None:
            from meerschaum.utils.warnings import error
            error(f"Could not find file for plugin '{self}'.")
        if '__init__.py' in self.__file__:
            path = self.__file__.replace('__init__.py', '')
            is_dir = True
        else:
            path = self.__file__
            is_dir = False

        tarf = tarfile.open(
            self.archive_path,
            'w:gz'
        )

        patterns_to_ignore = {
            '.pyc',
            '__pycache__/',
            'eggs/',
            '__pypackages__/',
            '.git',
        }

        if not is_dir:
            tarf.add(f"{self.name}.py")
        else:
            for root, dirs, files in os.walk(self.name):
                for f in files:
                    good_file = True
                    fp = os.path.join(root, f)
                    for pattern in patterns_to_ignore:
                        if pattern in str(fp) or f.startswith('.'):
                            good_file = False
                            break
                    if good_file:
                        if debug:
                            dprint(f"Adding '{fp}'...")
                        tarf.add(fp)

        ### clean up and change back to old directory
        tarf.close()
        os.chdir(old_cwd)

        ### change to 775 to avoid permissions issues with the API in a Docker container
        self.archive_path.chmod(0o775)

        if debug:
            dprint(f"Created archive '{self.archive_path}'.")
        return self.archive_path

    def remove(
            self,        
            force : bool = False,
            debug : bool = False
        ) -> SuccessTuple:
        """
        Remove a plugin's archive file.
        """
        if not self.archive_path.exists():
            return True, f"Archive file for plugin '{self}' does not exist."
        try:
            self.archive_path.unlink()
        except Exception as e:
            return False, f"Failed to remove plugin '{self}'\n{e}"
        return True, "Success"

    def install(
            self,
            force: bool = False,
            debug: bool = False
        ) -> SuccessTuple:
        """
        Extract a plugin's tar archive to the plugins directory.
        This function checks if the plugin is already installed and if the version is equal or
        greater than the existing installation.
        """
        from meerschaum.utils.warnings import warn, error
        if debug:
            from meerschaum.utils.debug import dprint
        import tarfile, pathlib, shutil
        from meerschaum.plugins import reload_plugins
        from meerschaum.utils.packages import attempt_import, determine_version
        old_cwd = os.getcwd()
        old_version = ''
        new_version = ''
        if not self.archive_path.exists():
            return False, f"Missing archive file for plugin '{self}'."
        is_installed = None
        if self.version is not None:
            is_installed = True
            old_version = self.version
            if debug:
                dprint(f"Found existing version '{old_version}' for plugin '{self}'.")
        try:
            tarf = tarfile.open(
                self.archive_path,
                'r:gz'
            )
        except Exception as e:
            warn(e)
            return False, f"Plugin '{self.name}' could not be downloaded."

        temp_dir = pathlib.Path(os.path.join(PLUGINS_TEMP_RESOURCES_PATH, self.name))
        temp_dir.mkdir(exist_ok=True)

        if debug:
            dprint(f"Extracting '{self.archive_path}' to '{temp_dir}'...")
        try:
            tarf.extractall(temp_dir)
        except Exception as e:
            success, msg = False, f"Failed to extract plugin '{self}'."

        ### search for version information
        files = os.listdir(temp_dir)
        
        if str(files[0]) == self.name:
            is_dir = True
        elif str(files[0]) == self.name + '.py':
            is_dir = False
        else:
            error(f"Unknown format encountered for plugin '{self}'.")

        fpath = pathlib.Path(os.path.join(temp_dir, files[0]))
        if is_dir:
            fpath = pathlib.Path(os.path.join(fpath, '__init__.py'))

        new_version = determine_version(fpath, name=self.name, search_for_metadata=False, warn=True)

        packaging_version = attempt_import('packaging.version')
        try:
            is_new_version = (
                packaging_version.parse(old_version) < packaging_version.parse(new_version)
            )
            is_same_version = (
                packaging_version.parse(old_version) == packaging_version.parse(new_version)
            )
        except Exception as e:
            is_new_version, is_same_version = True, False
        success_msg = f"Successfully installed plugin '{self}'."
        success, abort = None, None
        if is_same_version and not force:
            success, msg = True, (
                f"Plugin '{self}' is up-to-date (version {old_version}).\n" +
                "    Install again with `-f` or `--force` to reinstall."
            )
            abort = True
        elif is_new_version or force:
            for src_dir, dirs, files in os.walk(temp_dir):
                if success is not None:
                    break
                dst_dir = str(src_dir).replace(str(temp_dir), str(PLUGINS_RESOURCES_PATH))
                if not os.path.exists(dst_dir):
                    os.mkdir(dst_dir)
                for f in files:
                    src_file = os.path.join(src_dir, f)
                    dst_file = os.path.join(dst_dir, f)
                    if os.path.exists(dst_file):
                        os.remove(dst_file)

                    if debug:
                        dprint(f"Moving '{src_file}' to '{dst_dir}'...")
                    try:
                        shutil.move(src_file, dst_dir)
                    except Exception as e:
                        success, msg = False, (
                            f"Failed to install plugin '{self}': " +
                            f"Could not move file '{src_file}' to '{dst_dir}'"
                        )
                        print(msg)
                        break
            if success is None:
                success, msg = True, success_msg
        else:
            success, msg = False, (
                f"Your installed version of plugin '{self}' ({old_version}) is higher than "
                + f"attempted version {new_version}."
            )

        shutil.rmtree(temp_dir)
        tarf.close()
        os.chdir(old_cwd)

        ### Reload the plugin's module.
        if '_module' in self.__dict__:
            del self.__dict__['_module']
        reload_plugins([self.name], debug=debug)

        ### if we've already failed, return here
        if not success or abort:
            return success, msg

        ### attempt to install dependencies
        if not self.install_dependencies(force=force, debug=debug):
            return False, f"Failed to install dependencies for plugin '{self}'."

        ### handling success tuple, bool, or other (typically None)
        setup_tuple = self.setup(debug=debug)
        if isinstance(setup_tuple, tuple):
            if not setup_tuple[0]:
                success, msg = setup_tuple
        elif isinstance(setup_tuple, bool):
            if not setup_tuple:
                success, msg = False, (
                    f"Failed to run post-install setup for plugin '{self}'." + '\n' +
                    f"Check `setup()` in '{self.__file__}' for more information " +
                    f"(no error message provided)."
                )
            else:
                success, msg = True, success_msg
        elif setup_tuple is None:
            success = True
            msg = (
                f"Post-install for plugin '{self}' returned None. " +
                f"Assuming plugin successfully installed."
            )
            warn(msg)
        else:
            success = False
            msg = (
                f"Post-install for plugin '{self}' returned unexpected value " +
                f"of type '{type(setup_tuple)}': {setup_tuple}"
            )

        return success, msg

    def setup(self, *args : str, debug : bool = False, **kw : Any) -> SuccessTuple:
        """
        If exists, run the plugin's `setup()` function.
        """
        from meerschaum.utils.packages import activate_venv, deactivate_venv
        from meerschaum.utils.debug import dprint
        import inspect
        _setup = None
        for name, fp in inspect.getmembers(self.module):
            if name == 'setup' and inspect.isfunction(fp):
                _setup = fp
                break

        ### assume success if no setup() is found (not necessary)
        if _setup is None:
            return True

        sig = inspect.signature(_setup)
        has_debug, has_kw = ('debug' in sig.parameters), False
        for k, v in sig.parameters.items():
            if '**' in str(v):
                has_kw = True
                break

        _kw = {}
        if has_kw:
            _kw.update(kw)
        if has_debug:
            _kw['debug'] = debug

        if debug:
            dprint(f"Running setup for plugin '{self}'...")
        try:
            activate_venv(venv=self.name, debug=debug)
            return_tuple = _setup(*args, **_kw)
            deactivate_venv(venv=self.name, debug=debug)
        except Exception as e:
            return False, str(e)

        if isinstance(return_tuple, tuple):
            return return_tuple
        if isinstance(return_tuple, bool):
            return return_tuple, f"Setup for Plugin '{self.name}' did not return a message."
        if return_tuple is None:
            return False, f"Setup for Plugin '{self.name}' returned None."
        return False, f"Unknown return value from setup for Plugin '{self.name}': {return_tuple}"

    def get_dependencies(
            self,
            debug: bool = False,
        ) -> List[str]:
        """
        If the Plugin has specified dependencies in a list called `required`, return the list.

        NOTE: Dependecies which start with 'plugin:' are Meerschaum plugins, not pip packages. 
        """
        from meerschaum.utils.packages import activate_venv, deactivate_venv
        import inspect
        activate_venv(venv=self.name, debug=debug)
        required = []
        for name, val in inspect.getmembers(self.module):
            if name == 'required':
                required = val
                break
        deactivate_venv(venv=self.name, debug=debug)
        return required

    def install_dependencies(
            self,
            force: bool = False,
            debug: bool = False,
        ) -> bool:
        """
        If specified, install dependencies.

        NOTE: Dependencies that start with 'plugin:' will be installed as Meerschaum plugins
        from the same repository as this Plugin.
        """
        from meerschaum.utils.packages import pip_install
        from meerschaum.utils.debug import dprint
        from meerschaum.utils.warnings import warn, info
        _deps = self.get_dependencies(debug=debug)
        if not _deps:
            return True

        packages, plugins = [], []
        for _d in _deps:
            if _d.startswith('plugin:'):
                plugins.append(_d[len('plugin:'):])
            else:
                packages.append(_d)

        ### Attempt pip packages installation.
        if not pip_install(*packages, venv=self.name, debug=debug):
            return False

        ### Attempt plugins installation.
        ### NOTE: Currently there is no logic to relove circular dependencies!
        for _plugin in plugins:
            if _plugin == self.name:
                warn(f"Plugin '{self.name}' cannot depend on itself! Skipping...")
                continue
            _success, _msg = self.repo_connector.install_plugin(_plugin, debug=debug, force=force)
            if not _success:
                warn(
                    f"Failed to install required plugin '{_plugin}' for plugin '{self.name}':\n"
                    + _msg,
                    stack = False,
                )
                if not force:
                    warn(
                        "Try installing with the `--force` flag to continue anyway",
                        stack = False,
                    )
                    return False
                info(
                    "Continuing with installation despite the failure "
                    + "(careful, things might be broken!)",
                    icon = False
                )

        return True

    def __str__(self):
        return self.name

    def __repr__(self):
        return str(self)

    def __del__(self):
        pass
