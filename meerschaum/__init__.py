#! /usr/bin/env python
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

"""
Copyright 2023 Bennett Meares

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

import atexit
from meerschaum.utils.typing import SuccessTuple
from meerschaum.utils.packages import attempt_import
from meerschaum.core.Pipe import Pipe
from meerschaum.plugins import Plugin
from meerschaum.utils.venv import Venv
from meerschaum.jobs import Job, make_executor
from meerschaum.connectors import get_connector, Connector, make_connector
from meerschaum.utils import get_pipes
from meerschaum.utils.formatting import pprint
from meerschaum._internal.docs import index as __doc__
from meerschaum.config import __version__, get_config
from meerschaum._internal.entry import entry
from meerschaum.__main__ import _close_pools

atexit.register(_close_pools)

__pdoc__ = {'gui': False, 'api': False, 'core': False, '_internal': False}
__all__ = (
    "get_pipes",
    "get_connector",
    "get_config",
    "Pipe",
    "Plugin",
    "Venv",
    "Plugin",
    "Job",
    "pprint",
    "attempt_import",
    "actions",
    "config",
    "connectors",
    "jobs",
    "plugins",
    "utils",
    "SuccessTuple",
    "Connector",
    "make_connector",
    "entry",
)
