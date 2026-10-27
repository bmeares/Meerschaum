# 🖥️ Web Pages, Callbacks and Routes

Plugins add pages to the [Web Console](/reference/api-instance/web-console/) with `#!python @web_page`, make them interactive with Dash callbacks, and add API routes with `#!python @api_plugin`. This page covers who may use them (signed-in visitors or anyone), the helpers for routes (`session_user`, `mount_static`) and how to style a page (`theme`, `head`, `add_head`). The decorators themselves are introduced in [Writing Your Own Plugins](/reference/plugins/writing-plugins/#the-dash_plugin-and-web_page-decorators).

## Who may run a callback

**A console callback runs only for a visitor who is signed in.** The web console checks every callback request (`POST /dash/_dash-update-component`) for a console session, the `mrsm-session-id` cookie the login page sets. A request without one is answered with `401`, and the browser goes to the login page and comes back to the page afterwards.

That is the right default for almost every page: your callbacks read pipes, users or jobs, and anonymous visitors should not run them. A page anyone may use needs two things:

1. `#!python @web_page(..., login_required=False)`, so visitors who are not signed in see the page instead of the login form;
2. **public callbacks**, so they run for those visitors too.

Mark callbacks public in one of two ways.

=== "@web_callback(..., public=True)"

    ```python
    from meerschaum.plugins import web_page, web_callback

    @web_page('visits', login_required=False)
    def visits_page():
        from dash import html
        return html.Div([
            html.Button('Visit', id='visit-button'),
            html.Div(id='visits-out'),
        ])

    @web_callback(output='visits-out.children', inputs='visit-button.n_clicks', public=True)
    def count_visits(n_clicks):
        return f"{n_clicks or 0} visits"
    ```

    `#!python @web_callback(...)` registers a callback at the top level of your plugin, without a `@dash_plugin` function, with or without `public=True`. Name its `output`, `inputs` and `state` as `'component-id.property'` strings (one or a list), so your plugin need not import Dash, which every `mrsm` command, job and worker would then load. It also takes what `#!python @dash_app.callback(...)` takes: `Output(...)`, `Input(...)` and `State(...)` objects, `prevent_initial_call`, `running`, ...

=== "@dash_plugin(public=True)"

    ```python
    from meerschaum.plugins import dash_plugin, web_page

    @dash_plugin(public=True)
    def init_public_pages(dash_app):
        from dash import Input, Output

        @dash_app.callback(Output('greeting', 'children'), Input('name', 'value'))
        def greet(name):
            return f"Hello, {name or 'stranger'}!"
    ```

    Every callback the function registers is public (whether it uses `dash_app.callback`, `dash.callback` or `dash_extensions.enrich.callback`). Keep private callbacks in another `@dash_plugin` function.

!!! warning "Public callbacks are public"

    Anyone who can reach the console can call a public callback with any inputs. Read only what the page shows anyway, and never take a pipe's keys, a username or a query from the inputs without checking them.

??? note "What counts as a session, and the exceptions"

    - A session is the `mrsm-session-id` cookie, or the `session-store` value the console sends with a callback (`State('session-store', 'data')`), which is then copied back into the cookie.
    - A session ID is the UUID the login creates. Any other value counts as no session, so pass the value you received straight to the helpers in `meerschaum.api.dash.sessions` (`is_session_active`, `get_username_from_session`, `is_state_authenticated`) and never build one yourself.
    - The console's own login and registration callbacks and its page router are public (registration only while `api:permissions:registration:users` is on).
    - A console control that fires by itself as it renders (the Sign out button on a public page's bar) is answered with "no update" instead of `401`.
    - Signed in is not the same as allowed: a callback that changes things still checks the user, as the console's own do (`meerschaum.api.dash.sessions.is_state_authenticated`, which applies `api:permissions:actions:non_admin`).
    - Dash's own MCP server (`api:dash:mcp:enabled`) follows the same rule, and also accepts an API token (`Authorization: Bearer ...`).

!!! tip "Turning the check off"

    `mrsm start api --no-auth` needs no session for anything. To keep logins but let any callback run without a session, set this in `mrsm edit config api`:

    ```yaml
    dash:
      callbacks_require_session: false
    ```

## The signed-in user in an API route

`session_user` is a FastAPI dependency: the `User` signed in to the web console in this browser, or `401`. Routes your pages call (`fetch()`, HTMX) send the session cookie by themselves. `optional_session_user` is `None` instead of `401`.

```python
from meerschaum.plugins import api_plugin

@api_plugin
def init(app):
    import fastapi
    from meerschaum.api import session_user, optional_session_user

    @app.get('/my-plugin/me')
    def me(user=fastapi.Depends(session_user)):
        return {'username': user.username}

    @app.get('/my-plugin/greeting')
    def greeting(user=fastapi.Depends(optional_session_user)):
        return {'greeting': f"Hello, {user.username if user else 'guest'}!"}
```

For clients that are not browsers (scripts, other services), use `#!python fastapi.Depends(ScopedAuth(['pipes:read']))`, which takes API tokens and scopes (see [Security and Tokens](/reference/api-instance/tokens/)). Import from `meerschaum.api` inside your `@api_plugin` function, not at the top of the plugin: the plugin is imported by every `mrsm` command, which do not need the API's packages.

## Serving files

`mount_static(app, path, directory)` serves a directory of files (images, scripts, data exports, a built front-end) from an `@api_plugin` function:

```python
import pathlib
from meerschaum.plugins import api_plugin

HERE = pathlib.Path(__file__).parent

@api_plugin
def init(app):
    from meerschaum.api import mount_static
    mount_static(app, '/my-plugin/files', HERE / 'files')
    mount_static(app, '/my-plugin/site', HERE / 'site', auth=False, html=True)
```

- **Signed in by default.** `auth=True` needs a console session or an API token (`Authorization: Bearer ...`). A browser without one is sent to the login page and back; other clients get `401`. `auth=False` serves anyone.
- **Only that directory.** No path outside it is ever served (`..`, encoded or not, and symlinks that point outside), nor hidden files and folders (`.env`, `.git/...`).
- Files get the content types browsers expect (`text/javascript`, `text/css`, `application/wasm`, ...) and `X-Content-Type-Options: nosniff`; with `auth=True`, `Cache-Control: private, no-cache`.
- `html=True` serves `index.html` for a folder.
- A path under `/dash`, `/static`, `/_pages`, `/_mrsm`, `/webterm`, `/docs` or `/redoc` is refused.

## Styling a page

The console is dark (the Bootswatch *Darkly* theme). Choose another theme for one page with `theme`, and add tags to its `<head>` with `head`:

```python
import dash_bootstrap_components as dbc
from meerschaum.plugins import web_page

@web_page('reports', theme='light')
def reports():
    ...

@web_page(
    'brochure',
    theme=dbc.themes.LUX,
    head=[
        {'tag': 'meta', 'name': 'description', 'content': 'Our weekly numbers'},
        {'tag': 'link', 'rel': 'stylesheet', 'href': 'https://fonts.googleapis.com/css2?family=Inter'},
    ],
)
def brochure():
    ...
```

- `theme` is `'dark'` (the default), `'light'` (*Flatly*) or the URL of any Bootstrap stylesheet: a Bootswatch theme from `dash_bootstrap_components.themes`, or a file you serve with `mount_static`. Exactly one theme applies while the page is open, and the console's returns on other pages.
- `dark_theme` decides the `dbc_dark` component overrides, which suit dark themes: on with `'dark'`, off with `'light'` and with a custom stylesheet unless you pass `dark_theme=True` (for a dark theme such as `dbc.themes.CYBORG`).
- `head` takes HTML strings, dictionaries (`{'tag': 'meta', ...}`; the text of a `script` or `style` under `children`) and Dash components (`html.Link(...)`, `html.Script(...)`). Stylesheets and meta tags apply while the page is open; a script runs once, the first time the page opens.
- A page that returns HTML (the page contract) gets its theme and tags in its own document.

For tags on **every** page (a font, an analytics snippet, a web app manifest), call `add_head(...)` at the top level of your plugin:

```python
from meerschaum.plugins import add_head

add_head(
    '<meta name="theme-color" content="#222222">',
    {'tag': 'link', 'rel': 'manifest', 'href': '/my-plugin/files/app.webmanifest'},
)
```

## Testing your pages

The [`meerschaum.testing`](/reference/plugins/testing/) pytest plugin renders your pages and fires their callbacks, signed in or not:

```python
def test_visits_page(render_page):
    page = render_page('/dash/visits')
    assert page.callback('visits-out.children', inputs={'visit-button.n_clicks': 2}) == {
        'visits-out': {'children': '2 visits'},
    }
```
