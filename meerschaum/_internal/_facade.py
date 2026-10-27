#! /usr/bin/env python
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

"""
The eager half of `import meerschaum`: the imports the package used to run at import time.

`meerschaum/__init__.py` is lazy (PEP 562) so that `mrsm`, `python -m meerschaum` and every
process the CLI starts again skip the cost of the façade (about 0.12 s): the first attribute
access of a façade name imports this module once. Importing it executes exactly the statements the
package body used to, in the same order, so the import-time side effects plugins rely on happen
at the same point; names that are bound before a circular import finishes are reachable through
`meerschaum.__getattr__` while this module is only partially imported.
"""

import atexit

from meerschaum.utils.typing import SuccessTuple
from meerschaum.utils.packages import attempt_import
from meerschaum.core.Pipe import Pipe
from meerschaum.core.Pipe._sync import SyncResult
from meerschaum.core.Pipe._status import get_sync_statuses
from meerschaum.core.Pipeline import Pipeline, Plan, RunResult, get_pipelines, get_runs
from meerschaum.plugins import Plugin
from meerschaum.utils.venv import Venv
from meerschaum.jobs import Job, make_executor
from meerschaum.connectors import get_connector, Connector, InstanceConnector, make_connector
from meerschaum.utils import get_pipes
from meerschaum.utils.formatting import pprint
from meerschaum._internal.docs import index as __doc__
from meerschaum.config import __version__, get_config
from meerschaum._internal.entry import entry
from meerschaum.__main__ import _close_pools

atexit.register(_close_pools)

