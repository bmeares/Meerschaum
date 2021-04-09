#! /usr/bin/env python
# PYTHON_ARGCOMPLETE_OK
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

"""
mrsm CLI entrypoint
"""

def main():
    import sys, os
    sysargs = sys.argv[1:]

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
        return parse_help(sysargs)

    if '--version' in sysargs or '-V' in sysargs:
        from meerschaum.actions.arguments._parser import parse_version
        return parse_version(sysargs)

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
        print_tuple(return_tuple, upper_padding=1)

    ### Final step: close global pools.
    from meerschaum.utils.pool import get_pools
    for class_name, pool in get_pools().items():
        try:
            pool.close()
            pool.join()
        except Exception as e:
            pass

if __name__ == "__main__":
    main()
