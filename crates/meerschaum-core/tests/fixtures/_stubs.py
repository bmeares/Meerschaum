#! /usr/bin/env python3
# vim:fenc=utf-8

"""
Shared isolation and stubs for the pipe and template fixture generators.

- `isolate()` must run before importing `meerschaum`: it clears `MRSM_*` variables, points
  `MRSM_ROOT_DIR` and `MRSM_PLUGINS_DIR` at empty temporary directories (no real config, no
  plugins), selects the Python implementation, and disables the pipes cache.
- `install()` replaces the instance-backed parts of `Pipe` with in-memory stubs:
  `attributes` serves `REGISTERED` parameters (patched with the in-memory ones),
  `instance_connector` is a non-SQL stub, and `infer_dtypes` returns `{}`.
"""

import copy
import json
import os
import tempfile

REGISTERED = {}


def isolate():
    for key in [k for k in os.environ if k.startswith('MRSM_')]:
        del os.environ[key]
    tmp = tempfile.mkdtemp(prefix='mrsm-fixture-')
    os.environ['MRSM_ROOT_DIR'] = tmp
    os.makedirs(os.path.join(tmp, 'plugins'), exist_ok=True)
    os.environ['MRSM_PLUGINS_DIR'] = os.path.join(tmp, 'plugins')
    os.environ['MRSM_CORE'] = 'python'
    os.environ['MRSM_NO_AUTO_INSTALL'] = 'true'
    os.environ['MRSM_CONFIG'] = json.dumps({'pipes': {'cache': {'enabled': False}}})
    return tmp


class StubInstance:
    type = 'stub'
    flavor = None

    def __init__(self, keys):
        self.keys = keys

    def __str__(self):
        return self.keys

    def get_pipe_id(self, pipe, **kw):
        return None

    def get_pipe_attributes(self, pipe, **kw):
        return {}


def install():
    import meerschaum
    from meerschaum.core.Pipe import Pipe
    from meerschaum.config._patch import apply_patch_to_config
    assert meerschaum.__file__.startswith(os.getcwd()), meerschaum.__file__

    def attributes(self):
        key = (self.connector_keys, self.metric_key, self.location_key, self.instance_keys)
        registered = REGISTERED.get(key)
        if registered is None:
            return self._attributes
        return {
            **self._attributes,
            'parameters': apply_patch_to_config(
                copy.deepcopy(registered),
                self._attributes.get('parameters', {}),
            ),
        }

    Pipe.attributes = property(attributes)
    Pipe.instance_connector = property(lambda self: StubInstance(self.instance_keys))
    Pipe.infer_dtypes = lambda self, *args, **kwargs: {}


def register(connector, metric, location, instance, parameters):
    REGISTERED[(connector, metric, location, instance)] = parameters
