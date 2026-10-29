# 🧪 Testing Plugins

`meerschaum.testing` is a [pytest](https://docs.pytest.org) plugin: your tests get a throwaway Meerschaum root, a fresh SQLite instance for each test, pipes that clean up after themselves, the web API in the test process and the console's pages as a browser sees them. Nothing touches your real configuration, instances or jobs.

## Set up

Enable it in your project's `conftest.py`:

```python
pytest_plugins = ['meerschaum.testing']
```

(or run `pytest -p meerschaum.testing`). Point it at the plugins you are testing in `pytest.ini` (or the `[tool.pytest.ini_options]` table of `pyproject.toml`):

```ini
[pytest]
mrsm_plugins_dirs = plugins
```

| Option | Command line | Meaning |
|---|---|---|
| `mrsm_plugins_dirs` | `--mrsm-plugins-dir PATH` (repeatable) | Plugins directories to test, relative to the ini file. |
| `mrsm_venvs_dir` | `--mrsm-venvs-dir PATH` | Keep the plugins' virtual environments here between runs (default: inside the temporary root, so packages a plugin `required` are installed once per run). |
| `mrsm_env` | `--mrsm-env NAME=VALUE` (repeatable) | Variables to set after the environment is cleaned, e.g. `MRSM_NO_AUTO_INSTALL=1` when CI installs everything beforehand. |

## Fixtures

### `mrsm_root`

The test session's temporary root and plugins directory. Every inherited `MRSM_*` variable (and `TMUX`, `TMUX_PANE`) is removed for the session and restored afterwards; Meerschaum's paths, config and connectors point at the temporary root, and so do the jobs and `mrsm` commands your tests start. Jobs run locally (never as systemd units).

```python
def test_my_action(mrsm_root):
    mrsm_root.write_plugin('greeter', '''
from meerschaum.plugins import make_action

@make_action(name='greet everyone')
def greet(**kwargs):
    return True, "Hello, everyone!"
''')
    import meerschaum as mrsm
    assert mrsm.entry(['greet', 'everyone'])[0]
```

`mrsm_root.path` and `mrsm_root.plugins_dir` are the directories; `write_plugin(name, source, files=None)` adds a plugin (and reloads plugins); `env()` is the environment for a subprocess you start yourself.

### `mrsm_instance`

A temporary SQLite instance connector, `sql:test`, with a new database for each test. It is the default instance while the test runs: `mrsm.Pipe('a', 'b')` and `mrsm` commands use it.

### `mrsm_pipe`

A factory for pipes on `mrsm_instance`, registered (unless `register=False`) and deleted after the test:

```python
def test_fetch(mrsm_pipe):
    pipe = mrsm_pipe('plugin:noaa', 'weather', columns={'datetime': 'timestamp', 'id': 'station'})
    success, message = pipe.sync(begin='2026-01-01', end='2026-01-02')
    assert success, message
    assert len(pipe.get_data()) > 0
```

### `mrsm_api`

The API host (your `@api_plugin` routes, MCP tools and the web console with your pages) built in the test process for the session, on its own SQLite instance (`sql:api_test`). Needs `pip install "meerschaum[api]"` and `httpx`.

```python
def test_my_route(mrsm_api):
    assert mrsm_api.client.get('/my-plugin/me').status_code == 401
    client = mrsm_api.client_for('alice')          # signed in: console session + bearer token
    assert client.get('/my-plugin/me').json() == {'username': 'alice'}
```

| | |
|---|---|
| `mrsm_api.app`, `mrsm_api.client` | The FastAPI app and a `TestClient` without credentials. |
| `mrsm_api.user(name, password=None, admin=None)` | The user, registered first if needed (an admin when `admin`, by default when the name is `admin`). |
| `mrsm_api.session(name)` | A console session ID (the `mrsm-session-id` cookie). |
| `mrsm_api.token(name, scopes=None)` | A bearer JWT, as `/login` issues. |
| `mrsm_api.client_for(name)` | A `TestClient` with both. |

### `render_page`

`render_page(path, as_user=None, admin=None)` renders a console or plugin page as a browser with that user's session would (anonymous when `as_user` is `None`) and returns a `RenderedPage`. Needs `pip install "meerschaum[dash]"`.

```python
def test_dashboard_page(render_page):
    page = render_page('/dash/my-plugin/dashboard', as_user='alice')
    assert 'Daily means' in page.text
    assert 'station-dropdown' in page.ids
    response = page.callback('means-graph.figure', inputs={'station-dropdown.value': 'north'})
    assert response['means-graph']['figure']['data']

def test_anonymous_visitors_are_asked_to_sign_in(render_page):
    page = render_page('/dash/my-plugin/dashboard')
    assert 'login-button' in page.ids
```

| | |
|---|---|
| `page.kind` | `'dash'` (a Dash layout) or `'html'` (a page that returns HTML). |
| `page.status`, `page.layout`, `page.html`, `page.text`, `page.ids` | What the page is, and its text and component IDs for easy assertions (`'Hello' in page` works too). |
| `page.find(component_id)` | The component, as JSON. |
| `page.callback(output, inputs=None, state=None, trigger=None)` | Fire the callback that writes `output` (`'id.property'` or a component ID) with this page's session; values by `'id.property'` (the session's `session-store` and the page's location are filled in). Returns `{component_id: {property: value}}`, `None` when nothing updated, and raises `CallbackError` (with `.status`) when the console refused, e.g. `401` without a session for a callback that is not public. |
