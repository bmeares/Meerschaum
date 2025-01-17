#! /usr/bin/env python
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

"""
Import the routes submodules to register them to the FastAPI app.
"""

import meerschaum.api.routes._login
import meerschaum.api.routes._actions
import meerschaum.api.routes._jobs
import meerschaum.api.routes._connectors
import meerschaum.api.routes._index
import meerschaum.api.routes._misc
import meerschaum.api.routes._pipes
import meerschaum.api.routes._plugins
import meerschaum.api.routes._users
import meerschaum.api.routes._version

from meerschaum.api import _include_dash
if _include_dash:
    import meerschaum.api.routes._webterm
