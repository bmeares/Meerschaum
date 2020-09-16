#! /usr/bin/env python
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

from meerschaum.config import __version__
from meerschaum.config import __doc__
from meerschaum.utils.apipkg import initpkg
initpkg(
    __name__,
    {
        'get_connector' : 'meerschaum.connectors:get_connector',
        'entry' : 'meerschaum.actions._entry:_entry',
    }
)

import meerschaum.utils.warnings
#  import meerschaum.connectors

