#! /usr/bin/env python
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

"""
Import all routes from other modules in package
"""

### Although import_children works well, it's fairly ambiguous and does not
### freeze well. It will be depreciated in a future release.

# from meerschaum.utils.packages import import_children
# import_children()

from meerschaum.api.routes._actions import *
from meerschaum.api.routes._connectors import *
from meerschaum.api.routes._index import *
from meerschaum.api.routes._misc import *
from meerschaum.api.routes._pipes import *
from meerschaum.api.routes._plugins import *
from meerschaum.api.routes._users import *
from meerschaum.api.routes._version import *
