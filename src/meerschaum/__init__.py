#! /usr/bin/env python
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

### set warnings filter
import meerschaum.utils.warnings

### load metadata
from meerschaum.config import __version__
from meerschaum.config import __doc__

### lazy import submodules
from meerschaum.utils.misc import import_children
import_children(lazy=True, debug=True)
#  from meerschaum.utils.misc import lazy_import
#  actions = lazy_import('meerschaum.actions')
#  connectors = lazy_import('meerschaum.connectors')

