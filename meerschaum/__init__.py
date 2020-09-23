#! /usr/bin/env python
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

### set warnings filter
import meerschaum.utils.warnings

### load metadata
from meerschaum.config import __version__
from meerschaum.config import __doc__

### TODO edit import_children to recursively lazy import submodules
### lazy import submodules
#  from meerschaum.utils.misc import import_children
from meerschaum.utils.misc import lazy_import
actions = lazy_import('meerschaum.actions')
connectors = lazy_import('meerschaum.connectors')
utils = lazy_import('meerschaum.utils')
config = lazy_import('meerschaum.config')
Pipe = lazy_import('meerschaum.Pipe').Pipe
api = lazy_import('meerschaum.api')
get_pipes = utils._get_pipes.get_pipes
get_connector = connectors.get_connector
