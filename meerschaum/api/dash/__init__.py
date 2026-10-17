#! /usr/bin/env python
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

"""
The `meerschaum.api.dash` surface for plugins.

Meerschaum's own console is the native `/dash` served by `mrsm start api`; it is no longer a Dash
app. What stays here is what plugin-owned Dash apps import (see `meerschaum.api.host.dash`):

- `make_dash_app()` / `get_dash_app()`, which create the Dash app of a plugin, mounted at
  `/dash/<plugin>/` in the API host with the core stores pre-populated,
- `sessions` (`set_session`, `get_user_from_session`, ...), the shared session store,
- the `components`, `keys`, `pipes` and `graphs` helpers.
"""

from __future__ import annotations

from meerschaum.utils.typing import Any, Optional
from meerschaum.api import (
    debug,
    _get_pipes,
    get_pipe as get_api_pipe,
    pipes as api_pipes,
    get_api_connector,
    endpoints,
)

### Meerschaum runs no Dash app of its own: each plugin owns its app (the host sets this surface up).
dash_app = None


def make_dash_app(plugin_name: str, pages: Optional[dict] = None, **kwargs: Any):
    """
    Create (once) the Dash app owned by `plugin_name`, mounted at `/dash/<plugin_name>/` in the
    API host. See `meerschaum.api.host.dash.make_dash_app()`.
    """
    from meerschaum.api.host.dash import make_dash_app as _make_dash_app
    return _make_dash_app(plugin_name, pages, **kwargs)


def get_dash_app(plugin_name: str):
    """Return the Dash app of a plugin, or `None` if it has none yet."""
    from meerschaum.api.host.dash import get_dash_app as _get_dash_app
    return _get_dash_app(plugin_name)
