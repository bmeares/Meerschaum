#! /usr/bin/env python
# PYTHON_ARGCOMPLETE_OK
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

"""
Copyright 2021 Bennett Meares

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

   http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
"""

import sys, os

def main(sysargs: list = None) -> None:
    """Main CLI entry point.

    Parameters
    ----------
    sysargs: list :
         (Default value = None)

    Returns
    -------

    """
    if sysargs is None:
        sysargs = sys.argv[1:]
    old_cwd = os.getcwd()

    ### Check for a custom configuration directory.
    if '--root-dir' in sysargs:
        from meerschaum.actions.arguments._parse_arguments import parse_arguments
        from meerschaum.config._paths import set_root
        import pathlib
        from meerschaum.config.static import _static_config
        env_var = _static_config()['environment']['root']
        if env_var in os.environ:
            print(f"WARNING: '{env_var}' is set, so --root-dir will be ignored.")
        else:
            args = parse_arguments(sysargs)
            config_dir_path = pathlib.Path(args['root_dir']).absolute()
            if not config_dir_path.exists():
                print(
                    f"Invalid config directory '{str(config_dir_path)}'.\n" +
                    "Please enter a valid path for `--config-dir`."
                )
                sys.exit(1)
            set_root(config_dir_path)

    ### Catch help flags.
    if '--help' in sysargs or '-h' in sysargs:
        from meerschaum.actions.arguments._parser import parse_help
        parse_help(sysargs)
        return _exit(old_cwd=old_cwd)

    ### Catch version flags.
    if '--version' in sysargs or '-V' in sysargs:
        from meerschaum.actions.arguments._parser import parse_version
        parse_version(sysargs)
        return _exit(old_cwd=old_cwd)

    if ('-d' in sysargs or '--daemon' in sysargs) and ('stack' not in sysargs):
        from meerschaum.utils.daemon import daemon_entry
        daemon_entry(sysargs)
        return _exit(old_cwd=old_cwd)

    from meerschaum.actions import entry, get_shell

    ### Try to launch a shell if --shell is provided.
    if len(sysargs) == 0 or '--shell' in sysargs:
        if '--shell' in sysargs:
            sysargs.remove('--shell')
        get_shell(sysargs).cmdloop()
        return _exit(old_cwd=old_cwd)

    ### Print success or failure message.
    return_tuple = entry(sysargs)
    rc = 0
    if isinstance(return_tuple, tuple):
        from meerschaum.utils.formatting import print_tuple
        print_tuple(return_tuple, upper_padding=1)
        rc = 0 if (return_tuple[0] is True) else 1

    return _exit(rc, old_cwd=old_cwd)

def _exit(return_code : int = 0, old_cwd : str = None) -> None:
    _close_pools()
    ### Restore the previous working directory.
    if old_cwd is not None and old_cwd != os.getcwd():
        os.chdir(old_cwd)
    sys.exit(return_code)

def _close_pools():
    """Close multiprocessing pools before exiting."""
    ### Final step: close global pools.
    from meerschaum.utils.pool import get_pools
    for class_name, pool in get_pools().items():
        #  try:
            #  pool.shutdown()
        #  except Exception as e:
            #  print(e)
        try:
            pool.close()
            pool.terminate()
            #  pool.join()
        except Exception as e:
            print(e)
            pass

if __name__ == "__main__":
    main(sys.argv[1:])
