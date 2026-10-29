#! /usr/bin/env python3
# vim:fenc=utf-8

"""
Record the Python v4 API server's OpenAPI document for `meerschaum-server`.

Starts the Python API server (`--no-dash --no-webterm`) on a free port against a fresh sqlite
instance in a temporary root (see `_pyserver.py`), fetches `/openapi.json`, normalizes it to the
parts the Rust server must reproduce (every path and method, its tags, its parameters with their
location, requiredness and type, its request body content types) and writes `openapi.json` next to
this script. The Rust test `openapi_parity.rs` asserts that every path and method here exists in
the Rust document with the same parameters.

Usage (from the repository root):

    PYTHONPATH=$(pwd) python crates/meerschaum-server/tests/fixtures/gen_openapi.py
    scripts/parity/run.sh crates/meerschaum-server/tests/fixtures/gen_openapi.py
"""

import json
import pathlib
import sys

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parent))
import _pyserver  # noqa: E402

_pyserver.reexec_in_temp_root(__file__)

OUT = _pyserver.HERE / 'openapi.json'


def schema_type(schema: dict, components: dict) -> str:
    """Collapse a JSON schema into a sorted `|`-joined set of type names."""
    if not schema:
        return 'any'
    if '$ref' in schema:
        name = schema['$ref'].rsplit('/', 1)[-1]
        return schema_type(components.get(name, {}), components)
    types = set()
    for key in ('anyOf', 'oneOf'):
        for sub in schema.get(key, []):
            types.update(schema_type(sub, components).split('|'))
    if 'type' in schema:
        typ = schema['type']
        types.update(typ if isinstance(typ, list) else [typ])
    return '|'.join(sorted(types)) or 'any'


def normalize(doc: dict) -> dict:
    components = doc.get('components', {}).get('schemas', {})
    paths = {}
    for path, item in sorted(doc.get('paths', {}).items()):
        for method, op in sorted(item.items()):
            params = [
                {
                    'name': p['name'],
                    'in': p['in'],
                    'required': bool(p.get('required', False)),
                    'type': schema_type(p.get('schema', {}), components),
                }
                for p in op.get('parameters', [])
            ]
            body = op.get('requestBody')
            paths.setdefault(path, {})[method] = {
                'tags': op.get('tags', []),
                'parameters': sorted(params, key=lambda p: (p['in'], p['name'])),
                'request_body': sorted(body.get('content', {}).keys()) if body else None,
                'body_required': bool(body.get('required', False)) if body else False,
            }
    schemes = doc.get('components', {}).get('securitySchemes', {})
    return {
        'openapi': doc.get('openapi'),
        'title': doc.get('info', {}).get('title'),
        'security_schemes': sorted(schemes.keys()),
        'security': doc.get('security', []),
        'paths': paths,
    }


def main():
    import requests
    _pyserver.seed_admin()
    port = _pyserver.start_server()
    doc = requests.get(f"http://127.0.0.1:{port}/openapi.json", timeout=30).json()
    docs_html = requests.get(f"http://127.0.0.1:{port}/docs", timeout=30).text
    out = normalize(doc)
    out['docs_html'] = docs_html
    _pyserver.write_fixture(OUT, json.loads(json.dumps(out, sort_keys=True)))
    print(f"Wrote {sum(len(v) for v in out['paths'].values())} operations to {OUT}.")


if __name__ == '__main__':
    try:
        main()
    finally:
        _pyserver.kill_server()
