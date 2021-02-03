#! /usr/bin/env python
# PYTHON_ARGCOMPLETE_OK
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

"""
mrsm CLI entrypoint
"""

def main():
    import sys
    sysargs = sys.argv[1:]

    ### Catch help flags.
    if '--help' in sysargs or '-h' in sysargs:
        from meerschaum.actions.arguments._parser import parse_help
        return parse_help(sysargs)

    ### Check for a custom configuration directory.
    if '--config-dir' in sysargs:
        from meerschaum.actions.arguments._parse_arguments import parse_arguments
        import meerschaum.config._paths as _paths, pathlib, os, sys
        args = parse_arguments(sysargs)
        original_config_dir_path = _paths.CONFIG_ROOT_PATH
        config_dir_path = pathlib.Path(args['config_dir'])
        if not config_dir_path.exists():
            print(
                f"Invalid config directory '{str(config_dir_path)}'.\n" +
                "Please enter a valid path for `--config-dir`."
            )
            sys.exit(1)
        _paths.CONFIG_ROOT_PATH = config_dir_path
        from meerschaum.config._read_config import read_config
        

    from meerschaum.actions import entry, get_shell

    ### Try to launch a shell if --shell is provided.
    if len(sysargs) == 0 or '--shell' in sysargs:
        if '--shell' in sysargs:
            sysargs.remove('--shell')
        return get_shell(sysargs).cmdloop()

    ### Print success or failure message.
    return_tuple = entry(sysargs)
    if isinstance(return_tuple, tuple):
        from meerschaum.utils.formatting import print_tuple
        print_tuple(return_tuple)

    ### Final step: close global pools.
    from meerschaum.utils.pool import get_pools
    for class_name, pool in get_pools().items():
        try:
            pool.close()
            pool.join()
        except:
            pass

if __name__ == "__main__":
    main()
