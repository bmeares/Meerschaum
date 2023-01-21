#! /usr/bin/env python
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

"""
Plugin metadata class
"""

from __future__ import annotations
import os, pathlib, shutil
from site import venv
from meerschaum.utils.typing import (
    Dict,
    List,
    Optional,
    Any,
    SuccessTuple,
    Union,
)
from meerschaum.utils.warnings import error, warn
from meerschaum.config import get_config
from meerschaum.config._paths import (
    PLUGINS_RESOURCES_PATH,
    PLUGINS_ARCHIVES_RESOURCES_PATH,
    PLUGINS_TEMP_RESOURCES_PATH,
    VIRTENV_RESOURCES_PATH,
    PLUGINS_DIR_PATHS,
)
_tmpversion = None
_ongoing_installations = set()

class Plugin:
    """Handle packaging of Meerschaum plugins."""
    def __init__(
        self,
        name: str,
        version: Optional[str] = None,
        user_id: Optional[int] = None,
        required: Optional[List[str]] = None,
        attributes: Optional[Dict[str, Any]] = None,
        archive_path: Optional[pathlib.Path] = None,
        venv_path: Optional[pathlib.Path] = None,
        repo_connector: Optional['meerschaum.connectors.api.APIConnector'] = None,
        repo: Union['meerschaum.connectors.api.APIConnector', str, None] = None,
    ):
        from meerschaum.config.static import STATIC_CONFIG
        sep = STATIC_CONFIG['plugins']['repo_separator']
        _repo = None
        if sep in name:
            try:
                name, _repo = name.split(sep)
            except Exception as e:
                error(f"Invalid plugin name: '{name}'")
        self._repo_in_name = _repo

        if attributes is None:
            attributes = {}
        self.name = name
        self.attributes = attributes
        self.user_id = user_id
        self._version = version
        if required:
            self._required = required
        self.archive_path = (
            archive_path if archive_path is not None
            else PLUGINS_ARCHIVES_RESOURCES_PATH / f"{self.name}.tar.gz"
        )
        self.venv_path = (
            venv_path if venv_path is not None
            else VIRTENV_RESOURCES_PATH / self.name
        )
        self._repo_connector = repo_connector
        self._repo_keys = repo


    @property
    def repo_connector(self):
        """
        Return the repository connector for this plugin.
        NOTE: This imports the `connectors` module, which imports certain plugin modules.
        """
        if self._repo_connector is None:
            from meerschaum.connectors.parse import parse_repo_keys

            repo_keys = self._repo_keys or self._repo_in_name
            if self._repo_in_name and self._repo_keys and self._repo_keys != self._repo_in_name:
                error(
                    f"Received inconsistent repos: '{self._repo_in_name}' and '{self._repo_keys}'."
                )
            repo_connector = parse_repo_keys(repo_keys)
            self._repo_connector = repo_connector
        return self._repo_connector


    @property
    def version(self):
        """
        Return the plugin's module version is defined (`__version__`) if it's defined.
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
        Return the Python module of the underlying plugin.
        """
        if '_module' not in self.__dict__ or self.__dict__.get('_module', None) is None:
            if self.__file__ is None:
                return None
            from meerschaum.plugins import import_plugins
            self._module = import_plugins(str(self), warn=False)
        return self._module


    @property
    def __file__(self) -> Union[str, None]:
        """
        Return the file path (str) of the plugin if it exists, otherwise `None`.
        """
        if self.__dict__.get('_module', None) is not None:
            return self.module.__file__

        potential_dir = PLUGINS_RESOURCES_PATH / self.name
        if (
            potential_dir.exists()
            and potential_dir.is_dir()
            and (potential_dir / '__init__.py').exists()
        ):
            return str((potential_dir / '__init__.py').as_posix())

        potential_file = PLUGINS_RESOURCES_PATH / (self.name + '.py')
        if potential_file.exists() and not potential_file.is_dir():
            return str(potential_file.as_posix())

        return None


    @property
    def requirements_file_path(self) -> Union[pathlib.Path, None]:
        """
        If a file named `requirements.txt` exists, return its path.
        """
        if self.__file__ is None:
            return None
        path = pathlib.Path(self.__file__).parent / 'requirements.txt'
        if not path.exists():
            return None
        return path


    def is_installed(self, **kw) -> bool:
        """
        Check whether a plugin is correctly installed.

        Returns
        -------
        A `bool` indicating whether a plugin exists and is successfully imported.
        """
        return self.__file__ is not None


    def make_tar(self, debug: bool = False) -> pathlib.Path:
        """
        Compress the plugin's source files into a `.tar.gz` archive and return the archive's path.

        Parameters
        ----------
        debug: bool, default False
            Verbosity toggle.

        Returns
        -------
        A `pathlib.Path` to the archive file's path.

        """
        import tarfile, pathlib, subprocess, fnmatch
        from meerschaum.utils.debug import dprint
        from meerschaum.utils.packages import attempt_import
        pathspec = attempt_import('pathspec', debug=debug)

        if not self.__file__:
            from meerschaum.utils.warnings import error
            error(f"Could not find file for plugin '{self}'.")
        if '__init__.py' in self.__file__ or os.path.isdir(self.__file__):
            path = self.__file__.replace('__init__.py', '')
            is_dir = True
        else:
            path = self.__file__
            is_dir = False

        old_cwd = os.getcwd()
        real_parent_path = pathlib.Path(os.path.realpath(path)).parent
        os.chdir(real_parent_path)

        default_patterns_to_ignore = [
            '.pyc',
            '__pycache__/',
            'eggs/',
            '__pypackages__/',
            '.git',
        ]

        def parse_gitignore() -> 'Set[str]':
            gitignore_path = pathlib.Path(path) / '.gitignore'
            if not gitignore_path.exists():
                return set()
            with open(gitignore_path, 'r', encoding='utf-8') as f:
                gitignore_text = f.read()
            return set(pathspec.PathSpec.from_lines(
                pathspec.patterns.GitWildMatchPattern,
                default_patterns_to_ignore + gitignore_text.splitlines()
            ).match_tree(path))

        patterns_to_ignore = parse_gitignore() if is_dir else set()

        if debug:
            dprint(f"Patterns to ignore:\n{patterns_to_ignore}")

        with tarfile.open(self.archive_path, 'w:gz') as tarf:
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
        os.chdir(old_cwd)

        ### change to 775 to avoid permissions issues with the API in a Docker container
        self.archive_path.chmod(0o775)

        if debug:
            dprint(f"Created archive '{self.archive_path}'.")
        return self.archive_path


    def install(
            self,
            force: bool = False,
            debug: bool = False,
        ) -> SuccessTuple:
        """
        Extract a plugin's tar archive to the plugins directory.
        
        This function checks if the plugin is already installed and if the version is equal or
        greater than the existing installation.

        Parameters
        ----------
        force: bool, default False
            If `True`, continue with installation, even if required packages fail to install.

        debug: bool, default False
            Verbosity toggle.

        Returns
        -------
        A `SuccessTuple` of success (bool) and a message (str).

        """
        if self.full_name in _ongoing_installations:
            return True, f"Already installing plugin '{self}'."
        _ongoing_installations.add(self.full_name)
        from meerschaum.utils.warnings import warn, error
        if debug:
            from meerschaum.utils.debug import dprint
        import tarfile
        import re
        import ast
        from meerschaum.plugins import reload_plugins, sync_plugins_symlinks
        from meerschaum.utils.packages import attempt_import, determine_version, reload_package
        from meerschaum.utils.venv import init_venv
        from meerschaum.utils.misc import safely_extract_tar
        old_cwd = os.getcwd()
        old_version = ''
        new_version = ''
        temp_dir = PLUGINS_TEMP_RESOURCES_PATH / self.name
        temp_dir.mkdir(exist_ok=True)

        if not self.archive_path.exists():
            return False, f"Missing archive file for plugin '{self}'."
        if self.version is not None:
            old_version = self.version
            if debug:
                dprint(f"Found existing version '{old_version}' for plugin '{self}'.")

        if debug:
            dprint(f"Extracting '{self.archive_path}' to '{temp_dir}'...")

        try:
            with tarfile.open(self.archive_path, 'r:gz') as tarf:
                safely_extract_tar(tarf, temp_dir)
        except Exception as e:
            warn(e)
            return False, f"Failed to extract plugin '{self.name}'."

        ### search for version information
        files = os.listdir(temp_dir)
        
        if str(files[0]) == self.name:
            is_dir = True
        elif str(files[0]) == self.name + '.py':
            is_dir = False
        else:
            error(f"Unknown format encountered for plugin '{self}'.")

        fpath = temp_dir / files[0]
        if is_dir:
            fpath = fpath / '__init__.py'

        init_venv(self.name, debug=debug)
        with open(fpath, 'r', encoding='utf-8') as f:
            init_lines = f.readlines()
        new_version = None
        for line in init_lines:
            if '__version__' not in line:
                continue
            version_match = re.search(r'__version__(\s?)=', line.lstrip().rstrip())
            if not version_match:
                continue
            new_version = ast.literal_eval(line.split('=')[1].lstrip().rstrip())
            break
        if not new_version:
            warn(
                f"No `__version__` defined for plugin '{self}'. "
                + "Assuming new version...",
                stack = False,
            )

        packaging_version = attempt_import('packaging.version')
        try:
            is_new_version = (not new_version and not old_version) or (
                packaging_version.parse(old_version) < packaging_version.parse(new_version)
            )
            is_same_version = new_version and old_version and (
                packaging_version.parse(old_version) == packaging_version.parse(new_version)
            )
        except Exception as e:
            is_new_version, is_same_version = True, False

        ### Determine where to permanently store the new plugin.
        plugin_installation_dir_path = PLUGINS_DIR_PATHS[0]
        for path in PLUGINS_DIR_PATHS:
            files_in_plugins_dir = os.listdir(path)
            if (
                self.name in files_in_plugins_dir
                or
                (self.name + '.py') in files_in_plugins_dir
            ):
                plugin_installation_dir_path = path
                break

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
                dst_dir = str(src_dir).replace(str(temp_dir), str(plugin_installation_dir_path))
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
        os.chdir(old_cwd)

        ### Reload the plugin's module.
        sync_plugins_symlinks(debug=debug)
        if '_module' in self.__dict__:
            del self.__dict__['_module']
        init_venv(venv=self.name, force=True, debug=debug)
        reload_package('meerschaum')
        reload_plugins([self.name], debug=debug)

        ### if we've already failed, return here
        if not success or abort:
            _ongoing_installations.remove(self.full_name)
            return success, msg

        ### attempt to install dependencies
        if not self.install_dependencies(force=force, debug=debug):
            _ongoing_installations.remove(self.full_name)
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

        _ongoing_installations.remove(self.full_name)
        module = self.module
        return success, msg


    def remove_archive(
            self,        
            debug: bool = False
        ) -> SuccessTuple:
        """Remove a plugin's archive file."""
        if not self.archive_path.exists():
            return True, f"Archive file for plugin '{self}' does not exist."
        try:
            self.archive_path.unlink()
        except Exception as e:
            return False, f"Failed to remove archive for plugin '{self}':\n{e}"
        return True, "Success"


    def remove_venv(
            self,        
            debug: bool = False
        ) -> SuccessTuple:
        """Remove a plugin's virtual environment."""
        if not self.venv_path.exists():
            return True, f"Virtual environment for plugin '{self}' does not exist."
        try:
            shutil.rmtree(self.venv_path)
        except Exception as e:
            return False, f"Failed to remove virtual environment for plugin '{self}':\n{e}"
        return True, "Success"


    def uninstall(self, debug: bool = False) -> SuccessTuple:
        """
        Remove a plugin, its virtual environment, and archive file.
        """
        from meerschaum.utils.packages import reload_package
        from meerschaum.plugins import reload_plugins, sync_plugins_symlinks
        from meerschaum.utils.warnings import warn, info
        warnings_thrown_count: int = 0
        max_warnings: int = 3

        if not self.is_installed():
            info(
                f"Plugin '{self.name}' doesn't seem to be installed.\n    "
                + "Checking for artifacts...",
                stack = False,
            )
        else:
            real_path = pathlib.Path(os.path.realpath(self.__file__))
            try:
                if real_path.name == '__init__.py':
                    shutil.rmtree(real_path.parent)
                else:
                    real_path.unlink()
            except Exception as e:
                warn(f"Could not remove source files for plugin '{self.name}':\n{e}", stack=False)
                warnings_thrown_count += 1
            else:
                info(f"Removed source files for plugin '{self.name}'.")

        if self.venv_path.exists():
            success, msg = self.remove_venv(debug=debug)
            if not success:
                warn(msg, stack=False)
                warnings_thrown_count += 1
            else:
                info(f"Removed virtual environment from plugin '{self.name}'.")

        success = warnings_thrown_count < max_warnings
        sync_plugins_symlinks(debug=debug)
        self.deactivate_venv(force=True, debug=debug)
        reload_package('meerschaum')
        reload_plugins(debug=debug)
        return success, (
            f"Successfully uninstalled plugin '{self}'." if success
            else f"Failed to uninstall plugin '{self}'."
        )


    def setup(self, *args: str, debug: bool = False, **kw: Any) -> Union[SuccessTuple, bool]:
        """
        If exists, run the plugin's `setup()` function.

        Parameters
        ----------
        *args: str
            The positional arguments passed to the `setup()` function.
            
        debug: bool, default False
            Verbosity toggle.

        **kw: Any
            The keyword arguments passed to the `setup()` function.

        Returns
        -------
        A `SuccessTuple` or `bool` indicating success.

        """
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
            self.activate_venv(debug=debug)
            return_tuple = _setup(*args, **_kw)
            self.deactivate_venv(debug=debug)
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
        
        **NOTE:** Dependecies which start with `'plugin:'` are Meerschaum plugins, not pip packages.
        Meerschaum plugins may also specify connector keys for a repo after `'@'`.

        Parameters
        ----------
        debug: bool, default False
            Verbosity toggle.

        Returns
        -------
        A list of required packages and plugins (str).

        """
        if '_required' in self.__dict__:
            return self._required

        ### If the plugin has not yet been imported,
        ### infer the dependencies from the source text.
        ### This is not super robust, and it doesn't feel right
        ### having multiple versions of the logic.
        ### This is necessary when determining the activation order
        ### without having import the module.
        ### For consistency's sake, the module-less method does not cache the requirements.
        if self.__dict__.get('_module', None) is None:
            file_path = self.__file__
            if file_path is None:
                return []
            with open(file_path, 'r', encoding='utf-8') as f:
                text = f.read()

            if 'required' not in text:
                return []

            ### This has some limitations:
            ### It relies on `required` being manually declared.
            ### We lose the ability to dynamically alter the `required` list,
            ### which is why we've kept the module-reliant method below.
            import ast, re
            ### NOTE: This technically would break 
            ### if `required` was the very first line of the file.
            req_start_match = re.search(r'\nrequired(\s?)=', text)
            if not req_start_match:
                return []
            req_start = req_start_match.start()

            ### Dependencies may have brackets within the strings, so push back the index.
            first_opening_brace = req_start + 1 + text[req_start:].find('[')
            if first_opening_brace == -1:
                return []

            next_closing_brace = req_start + 1 + text[req_start:].find(']')
            if next_closing_brace == -1:
                return []

            start_ix = first_opening_brace + 1
            end_ix = next_closing_brace

            num_braces = 0
            while True:
                if '[' not in text[start_ix:end_ix]:
                    break
                num_braces += 1
                start_ix = end_ix
                end_ix += text[end_ix + 1:].find(']') + 1

            req_end = end_ix + 1
            req_text = (
                text[req_start:req_end]
                .lstrip()
                .replace('required', '', 1)
                .lstrip()
                .replace('=', '', 1)
                .lstrip()
            )
            try:
                required = ast.literal_eval(req_text)
            except Exception as e:
                warn(
                    f"Unable to determine requirements for plugin '{self.name}' "
                    + "without importing the module.\n"
                    + "    This may be due to dynamically setting the global `required` list.\n"
                    + f"    {e}"
                )
                return []
            return required

        import inspect
        self.activate_venv(dependencies=False, debug=debug)
        required = []
        for name, val in inspect.getmembers(self.module):
            if name == 'required':
                required = val
                break
        self._required = required
        self.deactivate_venv(dependencies=False, debug=debug)
        return required


    def get_required_plugins(self, debug: bool=False) -> List[meerschaum.plugins.Plugin]:
        """
        Return a list of required Plugin objects.
        """
        from meerschaum.utils.warnings import warn
        from meerschaum.config import get_config
        from meerschaum.config.static import STATIC_CONFIG
        plugins = []
        _deps = self.get_dependencies(debug=debug)
        sep = STATIC_CONFIG['plugins']['repo_separator']
        plugin_names = [
            _d[len('plugin:'):] for _d in _deps
            if _d.startswith('plugin:') and len(_d) > len('plugin:')
        ]
        default_repo_keys = get_config('meerschaum', 'default_repository')
        for _plugin_name in plugin_names:
            if sep in _plugin_name:
                try:
                    _plugin_name, _repo_keys = _plugin_name.split(sep)
                except Exception as e:
                    _repo_keys = default_repo_keys
                    warn(
                        f"Invalid repo keys for required plugin '{_plugin_name}'.\n    "
                        + f"Will try to use '{_repo_keys}' instead.",
                        stack = False,
                    )
            else:
                _repo_keys = default_repo_keys
            plugins.append(Plugin(_plugin_name, repo=_repo_keys))
        return plugins


    def get_required_packages(self, debug: bool=False) -> List[str]:
        """
        Return the required package names (excluding plugins).
        """
        _deps = self.get_dependencies(debug=debug)
        return [_d for _d in _deps if not _d.startswith('plugin:')]


    def activate_venv(self, dependencies: bool=True, debug: bool=False, **kw) -> bool:
        """
        Activate the virtual environments for the plugin and its dependencies.

        Parameters
        ----------
        dependencies: bool, default True
            If `True`, activate the virtual environments for required plugins.

        Returns
        -------
        A bool indicating success.
        """
        from meerschaum.utils.venv import venv_target_path
        from meerschaum.utils.packages import activate_venv
        from meerschaum.utils.misc import make_symlink, is_symlink
        from meerschaum.config._paths import PACKAGE_ROOT_PATH

        if dependencies:
            for plugin in self.get_required_plugins(debug=debug):
                plugin.activate_venv(debug=debug, **kw)

        vtp = venv_target_path(self.name, debug=debug, allow_nonexistent=True)
        venv_meerschaum_path = vtp / 'meerschaum'

        try:
            success, msg = True, "Success"
            if is_symlink(venv_meerschaum_path):
                if pathlib.Path(os.path.realpath(venv_meerschaum_path)) != PACKAGE_ROOT_PATH:
                    venv_meerschaum_path.unlink()
                    success, msg = make_symlink(venv_meerschaum_path, PACKAGE_ROOT_PATH)
        except Exception as e:
            success, msg = False, str(e)
        if not success:
            warn(f"Unable to create symlink {venv_meerschaum_path} to {PACKAGE_ROOT_PATH}:\n{msg}")

        return activate_venv(self.name, debug=debug, **kw)


    def deactivate_venv(self, dependencies: bool=True, debug: bool = False, **kw) -> bool:
        """
        Deactivate the virtual environments for the plugin and its dependencies.

        Parameters
        ----------
        dependencies: bool, default True
            If `True`, deactivate the virtual environments for required plugins.

        Returns
        -------
        A bool indicating success.
        """
        from meerschaum.utils.packages import deactivate_venv
        success = deactivate_venv(self.name, debug=debug, **kw)
        if dependencies:
            for plugin in self.get_required_plugins(debug=debug):
                plugin.deactivate_venv(debug=debug, **kw)
        return success


    def install_dependencies(
            self,
            force: bool = False,
            debug: bool = False,
        ) -> bool:
        """
        If specified, install dependencies.
        
        **NOTE:** Dependencies that start with `'plugin:'` will be installed as
        Meerschaum plugins from the same repository as this Plugin.
        To install from a different repository, add the repo keys after `'@'`
        (e.g. `'plugin:foo@api:bar'`).

        Parameters
        ----------
        force: bool, default False
            If `True`, continue with the installation, even if some
            required packages fail to install.

        debug: bool, default False
            Verbosity toggle.

        Returns
        -------
        A bool indicating success.

        """
        from meerschaum.utils.packages import pip_install, venv_contains_package
        from meerschaum.utils.debug import dprint
        from meerschaum.utils.warnings import warn, info
        from meerschaum.connectors.parse import parse_repo_keys
        _deps = self.get_dependencies(debug=debug)
        if not _deps and self.requirements_file_path is None:
            return True

        plugins = self.get_required_plugins(debug=debug)
        for _plugin in plugins:
            if _plugin.name == self.name:
                warn(f"Plugin '{self.name}' cannot depend on itself! Skipping...", stack=False)
                continue
            _success, _msg = _plugin.repo_connector.install_plugin(
                _plugin.name, debug=debug, force=force
            )
            if not _success:
                warn(
                    f"Failed to install required plugin '{_plugin}' from '{_plugin.repo_connector}'"
                    + f" for plugin '{self.name}':\n" + _msg,
                    stack = False,
                )
                if not force:
                    warn(
                        "Try installing with the `--force` flag to continue anyway.",
                        stack = False,
                    )
                    return False
                info(
                    "Continuing with installation despite the failure "
                    + "(careful, things might be broken!)...",
                    icon = False
                )


        ### First step: parse `requirements.txt` if it exists.
        if self.requirements_file_path is not None:
            if not pip_install(
                requirements_file_path=self.requirements_file_path,
                venv=self.name, debug=debug
            ):
                warn(
                    f"Failed to resolve 'requirements.txt' for plugin '{self.name}'.",
                    stack = False,
                )
                if not force:
                    warn(
                        "Try installing with `--force` to continue anyway.",
                        stack = False,
                    )
                    return False
                info(
                    "Continuing with installation despite the failure "
                    + "(careful, things might be broken!)...",
                    icon = False
                )


        ### Don't reinstall packages that are already included in required plugins.
        packages = []
        _packages = self.get_required_packages(debug=debug)
        accounted_for_packages = set()
        for package_name in _packages:
            for plugin in plugins:
                if venv_contains_package(package_name, plugin.name):
                    accounted_for_packages.add(package_name)
                    break
        packages = [pkg for pkg in _packages if pkg not in accounted_for_packages]

        ### Attempt pip packages installation.
        if packages:
            for package in packages:
                if not pip_install(package, venv=self.name, debug=debug):
                    warn(
                        f"Failed to install required package '{package}'"
                        + f" for plugin '{self.name}'.",
                        stack = False,
                    )
                    if not force:
                        warn(
                            "Try installing with `--force` to continue anyway.",
                            stack = False,
                        )
                        return False
                    info(
                        "Continuing with installation despite the failure "
                        + "(careful, things might be broken!)...",
                        icon = False
                    )
        return True


    @property
    def full_name(self) -> str:
        """
        Include the repo keys with the plugin's name.
        """
        from meerschaum.config.static import STATIC_CONFIG
        sep = STATIC_CONFIG['plugins']['repo_separator']
        return self.name + sep + str(self.repo_connector)


    def __str__(self):
        return self.name


    def __repr__(self):
        return f"Plugin('{self.name}', repo='{self.repo_connector}')"


    def __del__(self):
        pass
