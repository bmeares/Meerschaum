# Python plugins

Save `readings.py` in your plugins directory, or use `mrsm bootstrap plugins readings` to create it:

```python
__version__ = '0.1.0'

def register(pipe, **kwargs):
    return {'columns': {'id': 'id'}}

def fetch(pipe, begin=None, end=None, **kwargs):
    return [{'id': 1, 'temperature': 21.5}]
```

Register and sync a Pipe:

```bash
mrsm register pipe -c plugin:readings -m readings -i sql:local
mrsm sync pipes -c plugin:readings -i sql:local
```

`register` supplies initial parameters; `fetch` supplies rows. Keep optional arguments in `**kwargs`, and use `begin` / `end` to limit work for larger sources.

??? info "Dependencies and deployment"
    Declare packages in `required = ['requests']`, install them with `mrsm install required readings`, and keep heavy imports inside functions. For repeatable deployments, pin dependency versions and use the [environment controls](../../reference/environment.md#mrsm_no_auto_install).

Continue with [plugin functions](../../reference/plugins/writing-plugins.md), [web pages](../../reference/plugins/web-pages.md), or [plugin repositories](../../reference/plugins/repositories.md). Full module documentation is at [docs.meerschaum.io](https://docs.meerschaum.io/meerschaum/plugins.html).
