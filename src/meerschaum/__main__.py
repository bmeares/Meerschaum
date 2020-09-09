#! /usr/bin/env python
# PYTHON_ARGCOMPLETE_OK
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

"""
mrsm CLI entrypoint
"""

def main():
    import sys
    from meerschaum.actions import entry, shell
    sysargs = sys.argv[1:]
    if len(sysargs) == 0: return shell.cmdloop()
    return_tuple = entry(sysargs)
    if isinstance(return_tuple, tuple) and not return_tuple[0]:
        print(f"Error message: {return_tuple[1]}")

if __name__ == "__main__":
    main()
