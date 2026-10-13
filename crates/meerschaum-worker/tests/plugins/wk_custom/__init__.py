#! /usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Worker test plugin: custom connectors.

- `wkfeed:<label>`: a plain connector with `fetch()`.
- `wkmem:<label>`: an `IS_INSTANCE = True` connector storing pipes as JSON files (one per label) under the root
  directory, so every worker process sees the same data.
- `wkstream:<label>`: an `IS_STREAMING = True` connector with `subscribe(pipe, sink)`.
"""

__version__ = '0.3.0'

from ._impl import WkfeedConnector, WkmemConnector, WkstreamConnector  # noqa: F401
