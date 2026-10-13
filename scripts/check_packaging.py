#!/usr/bin/env python3
"""
Check that the 5.0 packaging metadata agrees with itself (docs/design/v5.0.0.md D9, D20, §12.1).

    python scripts/check_packaging.py            # run the checks
    python scripts/check_packaging.py --version  # print the wheel version (PEP 440)

Checks (exit 1 on any failure):

1. The Cargo workspace version (the single source of truth), converted to PEP 440, equals the
   `meerschaum-cli==X` pin in the root pyproject.toml.
2. `[project.optional-dependencies]` in the root pyproject.toml equals `extras` in
   meerschaum/utils/packages/_packages.py (with `[full]` expanded), and `dependencies` holds only
   `meerschaum-cli`.
3. `meerschaum/config/_version.py` equals the Cargo version once its major version is 5 or more.
   While it is still on the 4.x parity line the mismatch is reported as a note, not a failure:
   the wheels take their version from Cargo and `meerschaum.__version__` follows at release.
4. Both pyproject files declare the maturin backend and the expected distribution names.

Runs on any Python >= 3.11 with no third-party imports.
"""

from __future__ import annotations

import re
import sys
import tomllib
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent


def cargo_to_pep440(version: str) -> str:
    """`5.0.0-a1` -> `5.0.0a1`, `5.0.0-rc.1` -> `5.0.0rc1`, `5.0.0-dev.2` -> `5.0.0.dev2`."""
    m = re.fullmatch(r'(\d+\.\d+\.\d+)(?:-(.+))?', version)
    if not m:
        raise ValueError(f"unsupported Cargo version: {version!r}")
    base, pre = m.groups()
    if not pre:
        return base
    pm = re.fullmatch(r'(a|alpha|b|beta|rc|dev)\.?(\d+)', pre)
    if not pm:
        raise ValueError(f"unsupported pre-release segment: {pre!r}")
    tag, num = pm.groups()
    if tag == 'dev':
        return f"{base}.dev{num}"
    return f"{base}{ {'alpha': 'a', 'beta': 'b'}.get(tag, tag) }{num}"


def normalize(requirement: str) -> str:
    return re.sub(r'\s+', '', requirement).lower()


def main() -> int:
    if sys.argv[1:] == ['--version']:
        cargo = tomllib.loads((ROOT / 'Cargo.toml').read_text(encoding='utf-8'))
        print(cargo_to_pep440(cargo['workspace']['package']['version']))
        return 0
    failures: list[str] = []
    notes: list[str] = []

    cargo = tomllib.loads((ROOT / 'Cargo.toml').read_text(encoding='utf-8'))
    cargo_version = cargo['workspace']['package']['version']
    pep440 = cargo_to_pep440(cargo_version)

    root = tomllib.loads((ROOT / 'pyproject.toml').read_text(encoding='utf-8'))
    cli = tomllib.loads((ROOT / 'crates/meerschaum-cli/pyproject.toml').read_text(encoding='utf-8'))
    project = root['project']

    ### 4. Backends and names.
    for label, data, name in (('root', root, 'meerschaum'), ('cli', cli, 'meerschaum-cli')):
        if data['build-system']['build-backend'] != 'maturin':
            failures.append(f"{label} pyproject.toml: build-backend is not maturin")
        if data['project']['name'] != name:
            failures.append(f"{label} pyproject.toml: project name is not {name!r}")
        if 'version' not in data['project'].get('dynamic', []) or 'version' in data['project']:
            failures.append(f"{label} pyproject.toml: version must be dynamic (read from Cargo)")
    if cli['tool']['maturin'].get('bindings') != 'bin':
        failures.append("meerschaum-cli pyproject.toml: bindings must be 'bin'")
    if cli['project'].get('dependencies'):
        failures.append("meerschaum-cli pyproject.toml: must have no Python dependencies")

    ### 1 + 2a. The cli pin.
    expected_deps = [f"meerschaum-cli=={pep440}"]
    if project.get('dependencies') != expected_deps:
        failures.append(
            f"root pyproject.toml: dependencies are {project.get('dependencies')!r}, "
            f"expected {expected_deps!r} (Cargo version {cargo_version})"
        )

    ### 2b. Extras.
    sys.path.insert(0, str(ROOT))
    namespace: dict = {}
    source = (ROOT / 'meerschaum/utils/packages/_packages.py').read_text(encoding='utf-8')
    exec(compile(source, '_packages.py', 'exec'), namespace)
    py_extras = {k: sorted(map(normalize, v)) for k, v in namespace['extras'].items()}

    toml_extras: dict[str, list[str]] = {}
    for name, reqs in project.get('optional-dependencies', {}).items():
        expanded: list[str] = []
        for req in reqs:
            m = re.fullmatch(r'meerschaum\[(.+)\]', req.strip())
            if m:
                for inner in m.group(1).split(','):
                    expanded.extend(project['optional-dependencies'][inner.strip()])
            else:
                expanded.append(req)
        toml_extras[name] = sorted(set(map(normalize, expanded)))

    for name in sorted(set(py_extras) | set(toml_extras)):
        if name not in toml_extras:
            failures.append(f"extra {name!r} is in _packages.py but not in pyproject.toml")
        elif name not in py_extras:
            failures.append(f"extra {name!r} is in pyproject.toml but not in _packages.py")
        elif sorted(set(py_extras[name])) != toml_extras[name]:
            failures.append(
                f"extra {name!r} differs: _packages.py={sorted(set(py_extras[name]))} "
                f"pyproject.toml={toml_extras[name]}"
            )

    ### 3. The Python version.
    py_version = re.search(
        r'__version__\s*=\s*["\']([^"\']+)["\']',
        (ROOT / 'meerschaum/config/_version.py').read_text(encoding='utf-8'),
    ).group(1)
    if int(py_version.split('.')[0]) >= 5:
        if py_version != pep440:
            failures.append(f"meerschaum/config/_version.py is {py_version}, Cargo is {pep440}")
    elif py_version != pep440:
        notes.append(
            f"parity phase: meerschaum/config/_version.py is {py_version}, wheels will be {pep440} "
            "(Cargo); align them when the Python line moves to 5.x"
        )

    for note in notes:
        print(f"note: {note}")
    for failure in failures:
        print(f"FAIL: {failure}", file=sys.stderr)
    if failures:
        return 1
    print(f"packaging ok: Cargo {cargo_version} = wheel {pep440}; {len(toml_extras)} extras in sync")
    return 0


if __name__ == '__main__':
    sys.exit(main())
