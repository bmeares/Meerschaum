#! /usr/bin/env python
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

### Load version and docs metadata
from meerschaum.config import __version__
from meerschaum._internal.docs import index as __doc__

from meerschaum.utils.packages import lazy_import
actions = lazy_import('meerschaum.actions')
connectors = lazy_import('meerschaum.connectors')
utils = lazy_import('meerschaum.utils')
config = lazy_import('meerschaum.config')
Pipe = lazy_import('meerschaum.Pipe').Pipe
api = lazy_import('meerschaum.api')
get_pipes = utils.get_pipes
get_connector = connectors.get_connector

