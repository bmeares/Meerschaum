#! /usr/bin/env python
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

"""
Attempt to create a pipe's requirements in one method.
"""

from __future__ import annotations
from meerschaum.utils.typing import SuccessTuple, Optional

def bootstrap(
        self,
        debug : bool = False,
        **kw
    ) -> SuccessTuple:
    """
    Prompt the user to create a pipe's requirements all from one method.
    This method shouldn't be used in any automated scripts because it interactively
    prompts the user and therefore may hang.
    """

    from meerschaum.utils.prompt import prompt, yes_no

    if self.get_id(debug=debug) is not None:
        self.delete(debug=debug)

    if self.connector.type == 'sql':
        pass

    return True, "Success"
