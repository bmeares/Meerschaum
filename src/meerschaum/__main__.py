#! /usr/bin/env python
# PYTHON_ARGCOMPLETE_OK
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

"""
mrsm CLI entrypoint
"""

def main(sysargs=[]):
    from meerschaum.actions import entry, shell
    if len(sysargs) == 0: return shell.cmdloop()
    return entry(sysargs)

if __name__ == "__main__":
    import sys
    main(sys.argv[1:])
