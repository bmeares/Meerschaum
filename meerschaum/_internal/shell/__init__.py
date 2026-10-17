#! /usr/bin/env python
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

"""
The interactive shell is the native `mrsm` shell (`meerschaum-cli`).
`Shell(...).cmdloop()` runs it as a child process on the terminal.
"""

from __future__ import annotations

from meerschaum.utils.typing import Any, Dict, List, Optional, SuccessTuple


class Shell:
    """
    The interactive Meerschaum shell (the native `mrsm` shell, run as a child process).
    """

    def __init__(
        self,
        actions: Optional[Dict[str, Any]] = None,
        sysargs: Optional[List[str]] = None,
        instance_keys: Optional[str] = None,
    ):
        self.sysargs = list(sysargs or [])
        self.instance_keys = instance_keys

    def cmdloop(self, intro: Optional[str] = None) -> None:
        """Run the shell until it exits."""
        from meerschaum._internal.binary import run_binary
        sysargs = list(self.sysargs)
        if self.instance_keys is not None and '-i' not in sysargs and '--instance' not in sysargs:
            sysargs = ['-i', self.instance_keys] + sysargs
        run_binary(sysargs)

    def onecmd(self, line: str) -> SuccessTuple:
        """Run one command line natively and return its `SuccessTuple`."""
        import shlex
        from meerschaum._internal.entry import entry
        return entry(shlex.split(line))
