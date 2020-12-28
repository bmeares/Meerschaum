#! /usr/bin/env python
# PYTHON_ARGCOMPLETE_OK
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

"""
mrsm CLI entrypoint
"""

def main():
    import sys
    from meerschaum.actions import entry, get_shell
    from meerschaum.utils.formatting import print_tuple
    sysargs = sys.argv[1:]

    ### try to launch a shell if --shell is provided
    if len(sysargs) == 0 or '--shell' in sysargs:
        if '--shell' in sysargs: sysargs.remove('--shell')
        return get_shell(sysargs).cmdloop()

    ### print success or failure message
    return_tuple = entry(sysargs)
    if isinstance(return_tuple, tuple):
        print_tuple(return_tuple)

    ### final step: close global pools
    from meerschaum.utils.pool import get_pools
    for class_name, pool in get_pools().items():
        pool.close()
        pool.join()

if __name__ == "__main__":
    main()
