# `meerschaum.api.dash`: the Dash surface for plugins

Meerschaum's console is the native `/dash` of `mrsm start api` (Rust, `crates/meerschaum-web`);
it is not a Dash app any more. This package keeps only what plugin-owned Dash apps import
(design §8.4; the migration guide lists what was removed):

```
api/dash/
├── __init__.py     # make_dash_app() / get_dash_app(): delegate to api/host/dash.py
├── sessions.py     # the shared session store (same `mrsm-session-id` as the console)
├── components.py   # build_cards_grid, alert_from_success_tuple, search_parameters_editor, ...
├── keys.py         # action / key dropdown builders
├── pipes.py        # pipe cards and key dropdowns
├── graphs.py       # graph cards
└── connectors.py   # get_web_connector(state)
```

`make_dash_app(plugin_name, pages)` creates (once) the Dash app a plugin owns, mounted by the API
host at `/dash/<plugin>/` with `session-store`, `mrsm-location` and the `mrsm-theme-*` stores
already in its layout (`meerschaum/api/host/dash.py`).

Removed in 5.0 (importing them raises a `ModuleNotFoundError` naming the replacement,
`meerschaum/_internal/_removed.py`): `callbacks/`, `pages/`, `jobs`, `plugins`, `tokens`, `users`,
`webterm`, `websockets`, `sync` and the console's `assets/`.
