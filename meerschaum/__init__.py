#! /usr/bin/env python
# -*- coding: utf-8 -*-
# vim:fenc=utf-8
### Load version and docs metadata


from meerschaum.config import __version__
from meerschaum._internal.docs import index as __doc__

#  from meerschaum.utils.packages import lazy_import
from meerschaum.Pipe import Pipe
from meerschaum.utils import get_pipes
from meerschaum.connectors import get_connector


