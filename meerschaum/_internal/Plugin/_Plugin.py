#! /usr/bin/env python
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

"""
Plugin metadata class
"""
from meerschaum.config._paths import PLUGINS_RESOURCES_PATH, PLUGINS_ARCHIVES_RESOURCES_PATH
import os, pathlib
_tmpversion = None

class Plugin:
    def __init__(
        self,
        name : str,
        version : str = None,
        user_id : int = None,
        attributes : dict = {},
        archive_path : pathlib.Path = None
    ):
        self.name = name
        self.attributes = attributes
        self.user_id = user_id
        if version is None:
            try:
                self.version = self.module.__version__
            except:
                self.version = None
        else: self.version = version

        if archive_path is None:
            self.archive_path = pathlib.Path(
                os.path.join(PLUGINS_ARCHIVES_RESOURCES_PATH, f'{self.name}.tar.gz')
            )
        else: self.archive_path = archive_path

    @property
    def module(self):
        if '_module' not in self.__dict__:
            from meerschaum.actions.plugins import get_plugins_modules
            for m in get_plugins_modules():
                if self.name == m.__name__.split('.')[-1]:
                    self._module = m
                    break
        if '_module' not in self.__dict__: return None
        return self._module

    @property
    def __file__(self):
        if self.module is None: return None
        return self.module.__file__

    def make_tar(self, debug : bool = False) -> str:
        """
        Compress the plugin's source files into a .tar.gz archive and return the archive's file path.
        """
        import tarfile, os
        from meerschaum.config._paths import PLUGINS_RESOURCES_PATH, PLUGINS_ARCHIVES_RESOURCES_PATH
        from meerschaum.utils.debug import dprint

        old_cwd = os.getcwd()
        os.chdir(PLUGINS_RESOURCES_PATH)

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
                        if debug: dprint(f"Adding '{fp}'...")
                        tarf.add(fp)

        ### clean up and change back to old directory
        tarf.close()
        os.chdir(old_cwd)

        ### change to 775 to avoid permissions issues with the API in a Docker container
        self.archive_path.chmod(0o775)

        if debug: dprint(f"Created archive '{self.archive_path}'")
        return self.archive_path

    def install(
            self,
            debug : bool = False
        ) -> tuple:
        """
        Extract a plugin's tar archive to the plugins directory.
        This function checks if the plugin is already installed and if the version is equal or
        greater than the existing installation.
        """
        from meerschaum.config._paths import (
            PLUGINS_RESOURCES_PATH,
            PLUGINS_ARCHIVES_RESOURCES_PATH,
            PLUGINS_TEMP_RESOURCES_PATH
        )
        from meerschaum.utils.warnings import warn, error
        from meerschaum.utils.debug import dprint
        import tarfile, os, pathlib, shutil
        from meerschaum.utils.misc import reload_plugins
        old_cwd = os.getcwd()
        old_version = ''
        new_version = ''
        if not self.archive_path.exists(): return False, f"Missing archive file for plugin '{self}'"
        is_installed = None
        if self.__file__ is not None:
            is_installed = True
            try:
                old_version = self.module.__version__
            except:
                old_version = ''
            if debug: dprint(f"Found existing version '{old_version}' for plugin '{self}'")
        try:
            tarf = tarfile.open(
                self.archive_path,
                'r:gz'
            )
        except:
            return False, f"Plugin '{self.name}' could not be downloaded"

        temp_dir = pathlib.Path(os.path.join(PLUGINS_TEMP_RESOURCES_PATH, self.name))
        temp_dir.mkdir(exist_ok=True)

        if debug: dprint(f"Extracting '{self.archive_path}' to '{temp_dir}'...")
        try:
            tarf.extractall(temp_dir)
        except:
            success, msg = False, f"Failed to extract plugin '{self}'"

        ### search for version information
        files = os.listdir(temp_dir)
        
        if str(files[0]) == self.name: is_dir = True
        elif str(files[0]) == self.name + '.py': is_dir = False
        else: error(f"Unknown format encountered for plugin {self}")

        fpath = pathlib.Path(os.path.join(temp_dir, files[0]))
        if is_dir: fpath = pathlib.Path(os.path.join(fpath, '__init__.py'))
        fpointer = open(fpath, 'r')
        lines = fpointer.readlines()
        fpointer.close()
        global _tmpversion
        for l in lines:
            if '__version__' in l:
                _l = l.replace('__version__', '_tmpversion')
                exec(_l, globals())
                new_version = _tmpversion
                if debug: dprint(f"Attempting to install plugin '{self}' version '{new_version}'...")
                break

        from meerschaum.utils.packages import attempt_import
        packaging_version = attempt_import('packaging.version')
        is_new_version = (
            packaging_version.parse(old_version) <= packaging_version.parse(new_version)
        )
        
        success_msg = f"Successfully installed plugin '{self}'"
        success = None
        if is_new_version:
            for src_dir, dirs, files in os.walk(temp_dir):
                if success is not None: break
                dst_dir = str(src_dir).replace(str(temp_dir), str(PLUGINS_RESOURCES_PATH))
                if not os.path.exists(dst_dir):
                    os.mkdir(dst_dir)
                for f in files:
                    src_file = os.path.join(src_dir, f)
                    dst_file = os.path.join(dst_dir, f)
                    if os.path.exists(dst_file):
                        os.remove(dst_file)

                    if debug: dprint(f"Moving '{src_file}' to '{dst_dir}'...")
                    try:
                        shutil.move(src_file, dst_dir)
                    except:
                        success, msg = False, (
                            f"Failed to install plugin '{self}': " +
                            f"Could not move file '{src_file}' to '{dst_dir}'"
                        )
                        print(msg)
                        break
            if success is None: success, msg = True, success_msg
        else:
            success, msg = False, (
                f"Failed to install plugin '{self}': " +
                f"Existing version '{old_version}' is higher than attempted version '{new_version}'"
            )

        shutil.rmtree(temp_dir)
        tarf.close()
        os.chdir(old_cwd)

        reload_plugins()

        ### if we've already failed, return here
        if not success: return success, msg

        ### attempt to install dependencies
        if not self.install_dependencies(debug=debug):
            return False, f"Failed to install dependencies for plugin '{self}'"

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
            msg = f"Post-install for plugin '{self}' returned None. Assuming plugin successfully installed."
            warn(msg)
        else:
            success = False
            msg = f"Post-install for plugin '{self}' returned unexpected value of type '{type(setup_tuple)}': {setup_tuple}"

        return success, msg

    def setup(self, *args, debug : bool = False, **kw) -> tuple:
        """
        If exists, run the plugin's setup() function
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
        if _setup is None: return True

        sig = inspect.signature(_setup)
        has_debug, has_kw = ('debug' in sig.parameters), False
        for k, v in sig.parameters.items():
            if '**' in str(v):
                has_kw = True
                break

        _kw = {}
        if has_kw: _kw.update(kw)
        if has_debug: _kw['debug'] = debug

        if debug: dprint(f"Running setup for plugin '{self}'...")
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

    @property
    def dependencies(self):
        from meerschaum.utils.packages import activate_venv, deactivate_venv
        import inspect
        activate_venv(venv=self.name)
        required = []
        for name, val in inspect.getmembers(self.module):
            if name == 'required':
                required = val
                break
        deactivate_venv(venv=self.name)
        return required

    def install_dependencies(self, debug : bool = False) -> bool:
        """
        If specified, install dependencies
        """
        from meerschaum.utils.packages import pip_install
        from meerschaum.utils.debug import dprint
        if self.dependencies:
            if debug: dprint(f"Installing dependencies: {self.dependencies}")
            return pip_install(*self.dependencies, venv=self.name, debug=debug)
        return True

    def __str__(self):
        return self.name

    def __repr__(self):
        return str(self)

    def __del__(self):
        pass


