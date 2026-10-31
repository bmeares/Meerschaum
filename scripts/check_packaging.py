#!/usr/bin/env python3
"""
Check that the 5.0 packaging metadata agrees with itself (docs/design/v5.0.0.md D9, D20, §12.1).

    python scripts/check_packaging.py                   # run the source-tree checks
    python scripts/check_packaging.py --version         # print the wheel version (PEP 440)
    python scripts/check_packaging.py --write           # regenerate requirements/ and CLI tables
    python scripts/check_packaging.py --wheels DIST     # also check built wheels (see below)

Source-tree checks (exit 1 on any failure):

1. The root pyproject.toml has no `dependencies` (D8 revised, D20: one wheel, nothing else to
   install; the CLI is inside the extension) and no `meerschaum-cli` distribution exists
   (`crates/meerschaum-cli/pyproject.toml` is gone).
2. `[project.optional-dependencies]` in the root pyproject.toml equals `extras` in
   meerschaum/utils/packages/_packages.py (with `[full]` expanded).
3. `meerschaum/config/_version.py` equals the Cargo version (PEP 440 form): the Python package,
   the wheel metadata and `mrsm --version` report one version.
4. The root pyproject.toml declares the maturin backend (`bindings = "pyo3"`), the distribution name
   `meerschaum`, and the console scripts `mrsm` and `meerschaum` (both `_meerschaum_mrsm:main`, a
   top-level module shipped in the wheel via `[tool.maturin] include`).
5. `requirements/<extra>.txt` holds exactly the packages the `<extra>` extra declares (and
   `requirements/minimal.txt` is empty: the base wheel has no dependencies, D20). Regenerate with
   `--write` (scripts/docker/update_requirements.sh calls it).
6. `crates/meerschaum-cli/src/extras.rs` (the extras the CLI's `show venvs` / `prune venvs` treat
   as wanted packages) equals the extras in the root pyproject.toml. Regenerate with `--write`.
   `src/native/packages_table.rs` also matches the dependency groups in `_packages.py`.

Version scheme (checks 1 and 3 use it): Cargo and PEP 440 spell a pre-release differently, so the
Cargo version `X.Y.Z-aN` / `-bN` / `-rc.N` / `-dev.N` maps to `X.Y.ZaN` / `bN` / `rcN` / `.devN`
(`cargo_to_pep440`, and the inverse `pep440_to_cargo`, which always emits the dot form). Cargo
compares pre-release identifiers piece by piece, so `rc10` sorts below `rc2` while `rc.10` sorts
above `rc.2`: a Cargo `rc`/`beta` pre-release must use the dot form (`5.0.0-rc.1`, never
`5.0.0-rc1`); the legacy `5.0.0-a1` spelling of the alpha is accepted with a note.

Crates.io publishing (checks 9 and 10): every `[workspace.dependencies]` entry for a workspace
crate carries `version = "=<Cargo version>"` (lockstep; cargo cannot inherit the version there),
and every published crate (`publish` not `false`) has the metadata crates.io requires
(`description`, `license`, `repository`, `readme` + the file, `keywords`, `categories`, `include`).

Wheel checks (`--wheels DIST`, for the wheels `maturin build` wrote into DIST; used by
.github/workflows/wheels.yaml right after the build):

7. The `meerschaum` wheel is present once and its `METADATA` `Version` equals the Cargo version in
   PEP 440 form; it has no base `Requires-Dist`, carries a `meerschaum/config/_version.py` with
   that same version and the extension `meerschaum/_core/_native*`, declares the `mrsm` and
   `meerschaum` console scripts, and contains no `mrsm` binary (no `*.data/scripts/mrsm` that is
   not a Python script): the CLI runs from the extension.
8. No `meerschaum_cli-*.whl` is in DIST (the second wheel is gone).

Runs on any Python >= 3.11 with no third-party imports.
"""

from __future__ import annotations

import argparse
import re
import subprocess
import sys
import tomllib
import zipfile
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
REQUIREMENTS_DIR = ROOT / 'requirements'
EXTRAS_RS = ROOT / 'crates/meerschaum-cli/src/extras.rs'
PACKAGES_TABLE = ROOT / 'crates/meerschaum-cli/src/native/packages_table.rs'


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


def pep440_to_cargo(version: str) -> str:
    """`5.0.0a1` -> `5.0.0-alpha.1`, `5.0.0rc1` -> `5.0.0-rc.1`, `5.0.0.dev2` -> `5.0.0-dev.2`."""
    m = re.fullmatch(r'(\d+\.\d+\.\d+)(?:(a|b|rc)(\d+)|\.dev(\d+))?', version)
    if not m:
        raise ValueError(f"unsupported PEP 440 version: {version!r}")
    base, tag, num, dev = m.groups()
    if dev is not None:
        return f"{base}-dev.{dev}"
    if tag is None:
        return base
    return f"{base}-{ {'a': 'alpha', 'b': 'beta'}.get(tag, tag) }.{num}"


def cargo_prerelease_problem(version: str) -> tuple[str, str] | None:
    """
    `('fail' | 'note', message)` if the Cargo pre-release spelling sorts wrongly, else `None`.

    Cargo orders `rc10` before `rc2` (alphanumeric identifiers compare as text) but `rc.10`
    after `rc.2` (numeric identifiers compare as numbers).
    """
    m = re.fullmatch(r'\d+\.\d+\.\d+-(a|alpha|b|beta|rc|dev)(\.?)(\d+)', version)
    if not m or m.group(2):
        return None
    tag = m.group(1)
    message = (
        f"Cargo version {version} sorts {tag}10 before {tag}2; "
        f"use the dot form {version.replace(tag, tag + '.', 1)}"
    )
    return ('note', message) if tag in ('a', 'alpha') else ('fail', message)


def normalize(requirement: str) -> str:
    return re.sub(r'\s+', '', requirement).lower()


def package_name(requirement: str) -> str:
    """The PEP 503 normalized distribution name of a requirement string."""
    base = re.split(r'[\[=<>,!~;\s@]', requirement.strip(), maxsplit=1)[0]
    return re.sub(r'[-_.]+', '-', base).lower()


def expand_extras(optional_dependencies: dict[str, list[str]]) -> dict[str, list[str]]:
    """Each extra's requirements in declaration order, `meerschaum[a,b]` self-references expanded."""
    expanded: dict[str, list[str]] = {}
    for name, reqs in optional_dependencies.items():
        out: list[str] = []
        for req in reqs:
            m = re.fullmatch(r'meerschaum\[(.+)\]', req.strip())
            members = (
                [e.strip() for e in m.group(1).split(',')]
                if m
                else None
            )
            for item in ([optional_dependencies[e] for e in members] if members else [[req]]):
                for r in item:
                    if r not in out:
                        out.append(r)
        expanded[name] = out
    return expanded


def requirements_text(requirements: list[str]) -> str:
    return ''.join(f"{r}\n" for r in requirements)


def extras_rs_text(extras: dict[str, list[str]]) -> str:
    """The generated `crates/meerschaum-cli/src/extras.rs` (kept in sync by `--write`)."""
    lines = [
        "//! The packages the `meerschaum` extras declare (docs/design/v5.0.0.md D20, §12.1), so",
        "//! `show venvs` / `prune venvs` treat them as wanted in every venv.",
        "//!",
        "//! @generated by `python scripts/check_packaging.py --write` from",
        "//! `[project.optional-dependencies]` in the root pyproject.toml; do not edit by hand",
        "//! (`scripts/check_packaging.py` fails when it drifts).",
        "//!",
        "//! The hook: `PluginNeeds::required_names` in `native/venvs.rs` chains",
        "//! [`declared_packages`] onto the plugins' `required` names.",
        "",
        "use std::collections::BTreeSet;",
        "",
        "/// Every extra and the requirements it declares (`[full]` expanded), as written in the",
        "/// root pyproject.toml.",
        "pub const EXTRAS: &[(&str, &[&str])] = &[",
    ]
    ### Laid out the way rustfmt does (array_width 60, max_width 100) so `cargo fmt --check` passes.
    for name in sorted(extras):
        inline = '&[' + ', '.join(f'"{req}"' for req in extras[name]) + ']'
        if len(inline) <= 60 and len(f'    ("{name}", {inline}),') <= 100:
            lines.append(f'    ("{name}", {inline}),')
            continue
        lines += ["    (", f'        "{name}",', "        &["]
        lines += [f'            "{req}",' for req in extras[name]]
        lines += ["        ],", "    ),"]
    lines += [
        "];",
        "",
        "/// PEP 503 normalization of a requirement's distribution name (specifiers, extras and",
        "/// markers are dropped).",
        "fn normalize(requirement: &str) -> String {",
        "    let base = requirement",
        "        .trim()",
        "        .split(['[', '=', '<', '>', ',', '!', '~', ';', ' ', '@'])",
        "        .next()",
        "        .unwrap_or_default();",
        "    let mut out = String::new();",
        "    let mut last_sep = false;",
        "    for c in base.chars() {",
        "        if matches!(c, '-' | '_' | '.') {",
        "            if !last_sep {",
        "                out.push('-');",
        "            }",
        "            last_sep = true;",
        "        } else {",
        "            out.extend(c.to_lowercase());",
        "            last_sep = false;",
        "        }",
        "    }",
        "    out",
        "}",
        "",
        "/// The normalized names of every package any extra declares.",
        "pub fn declared_packages() -> BTreeSet<String> {",
        "    EXTRAS",
        "        .iter()",
        "        .flat_map(|(_, requirements)| requirements.iter())",
        "        .map(|requirement| normalize(requirement))",
        "        .collect()",
        "}",
        "",
        "#[cfg(test)]",
        "mod tests {",
        "    use super::*;",
        "",
        "    #[test]",
        "    fn declares_the_core_extras() {",
        "        let names = declared_packages();",
        '        for expected in ["pandas", "polars", "sqlalchemy", "fastapi", "dash"] {',
        "            assert!(",
        "                names.contains(expected),",
        '                "{expected} missing from {names:?}"',
        "            );",
        "        }",
        "        // Specifiers, brackets and case are normalized away.",
        '        assert!(names.contains("psycopg"));',
        "        assert!(names.iter().all(|n| !n.contains(['[', ']', '<', '>', '='])));",
        "    }",
        "",
        "    #[test]",
        "    fn normalizes_like_pep_503() {",
        '        assert_eq!(normalize("Foo_Bar.baz>=1"), "foo-bar-baz");',
        '        assert_eq!(normalize("psycopg[binary]>=3.2.9"), "psycopg");',
        "    }",
        "}",
        "",
    ]
    return "\n".join(lines)


def expected_requirements(extras: dict[str, list[str]]) -> dict[str, str]:
    files = {f"{name}.txt": requirements_text(reqs) for name, reqs in extras.items()}
    ### The base wheel has no dependencies (D20): `minimal` is empty.
    files['minimal.txt'] = ''
    return files


def read_py_version() -> str:
    m = re.search(
        r'__version__\s*=\s*["\']([^"\']+)["\']',
        (ROOT / 'meerschaum/config/_version.py').read_text(encoding='utf-8'),
    )
    return m.group(1) if m else ''


def load_extras_from_packages_py() -> dict[str, list[str]]:
    namespace: dict = {}
    source = (ROOT / 'meerschaum/utils/packages/_packages.py').read_text(encoding='utf-8')
    exec(compile(source, '_packages.py', 'exec'), namespace)
    return {k: list(v) for k, v in namespace['extras'].items()}


def check_wheels(dist: Path, pep440: str) -> tuple[list[str], list[str]]:
    failures: list[str] = []
    notes: list[str] = []

    def one(pattern: str) -> Path | None:
        found = sorted(dist.glob(pattern))
        if len(found) != 1:
            failures.append(f"{dist}: expected exactly one {pattern}, found {[p.name for p in found]}")
            return None
        return found[0]

    mrsm_wheel = one('meerschaum-*.whl')
    stray = sorted(dist.glob('meerschaum_cli-*.whl'))
    if stray:
        failures.append(f"{dist}: {[p.name for p in stray]} must not exist (one wheel, D8 revised)")

    def metadata(path: Path) -> tuple[dict[str, list[str]], zipfile.ZipFile]:
        zf = zipfile.ZipFile(path)
        name = next(n for n in zf.namelist() if n.endswith('.dist-info/METADATA'))
        fields: dict[str, list[str]] = {}
        for line in zf.read(name).decode('utf-8').split('\n\n', 1)[0].splitlines():
            if ': ' in line:
                key, value = line.split(': ', 1)
                fields.setdefault(key, []).append(value)
        return fields, zf

    if mrsm_wheel:
        fields, zf = metadata(mrsm_wheel)
        version = fields.get('Version', [''])[0]
        if version != pep440:
            failures.append(f"{mrsm_wheel.name}: METADATA Version is {version}, Cargo is {pep440}")
        requires = [r for r in fields.get('Requires-Dist', []) if 'extra ==' not in r]
        if requires:
            failures.append(f"{mrsm_wheel.name}: base Requires-Dist is {requires}, expected none (D20)")
        try:
            inner = re.search(
                r'__version__\s*=\s*["\']([^"\']+)["\']',
                zf.read('meerschaum/config/_version.py').decode('utf-8'),
            ).group(1)
            if inner != pep440:
                failures.append(f"{mrsm_wheel.name}: meerschaum/config/_version.py is {inner}, Cargo is {pep440}")
        except (KeyError, AttributeError):
            failures.append(f"{mrsm_wheel.name}: has no readable meerschaum/config/_version.py")

    if mrsm_wheel:
        names = zf.namelist()
        if not any(re.fullmatch(r'meerschaum/_core/_native.*\.(so|pyd)', n) for n in names):
            failures.append(f"{mrsm_wheel.name}: has no meerschaum/_core/_native extension")
        entry_points = next((n for n in names if n.endswith('.dist-info/entry_points.txt')), None)
        scripts_text = zf.read(entry_points).decode('utf-8') if entry_points else ''
        for script in ('mrsm', 'meerschaum'):
            if not re.search(rf'^{script}\s*=\s*_meerschaum_mrsm:main\s*$', scripts_text, re.M):
                failures.append(f"{mrsm_wheel.name}: console script {script!r} is not _meerschaum_mrsm:main")
        if '_meerschaum_mrsm.py' not in names:
            failures.append(f"{mrsm_wheel.name}: has no top-level _meerschaum_mrsm.py")
        for name in names:
            if re.search(r'\.data/scripts/mrsm(\.exe)?$', name):
                head = zf.read(name)[:2]
                if head != b'#!':
                    failures.append(f"{mrsm_wheel.name}: contains a native binary {name} (the CLI runs from the extension)")
        notes.append(
            f"{mrsm_wheel.name}: `mrsm --version` is checked by the wheel smoke test "
            "(pip install in a fresh venv), not here"
        )
    return failures, notes


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument('--version', action='store_true', help="print the wheel version (PEP 440) and exit")
    parser.add_argument('--write', action='store_true', help="regenerate requirements/ and the CLI dependency tables")
    parser.add_argument('--wheels', metavar='DIST', type=Path, help="also check the built wheels in DIST")
    args = parser.parse_args()

    cargo = tomllib.loads((ROOT / 'Cargo.toml').read_text(encoding='utf-8'))
    cargo_version = cargo['workspace']['package']['version']
    pep440 = cargo_to_pep440(cargo_version)
    if args.version:
        print(pep440)
        return 0

    failures: list[str] = []
    notes: list[str] = []

    root = tomllib.loads((ROOT / 'pyproject.toml').read_text(encoding='utf-8'))
    project = root['project']

    ### 4. Backend, name, console scripts.
    if root['build-system']['build-backend'] != 'maturin':
        failures.append("root pyproject.toml: build-backend is not maturin")
    if project['name'] != 'meerschaum':
        failures.append("root pyproject.toml: project name is not 'meerschaum'")
    if 'version' not in project.get('dynamic', []) or 'version' in project:
        failures.append("root pyproject.toml: version must be dynamic (read from Cargo)")
    if root['tool']['maturin'].get('bindings') != 'pyo3':
        failures.append("root pyproject.toml: bindings must be 'pyo3'")
    scripts = project.get('scripts', {})
    for script in ('mrsm', 'meerschaum'):
        if scripts.get(script) != '_meerschaum_mrsm:main':
            failures.append(
                f"root pyproject.toml: [project.scripts] {script} is {scripts.get(script)!r}, "
                "expected '_meerschaum_mrsm:main'"
            )
    if not (ROOT / '_meerschaum_mrsm.py').is_file():
        failures.append("_meerschaum_mrsm.py (the console scripts' module) is missing")
    includes = root['tool']['maturin'].get('include', [])
    if not any(i.get('path') == '_meerschaum_mrsm.py' and 'wheel' in i.get('format', []) for i in includes):
        failures.append("root pyproject.toml: [tool.maturin] include must ship _meerschaum_mrsm.py in the wheel")
    if (ROOT / 'crates/meerschaum-cli/pyproject.toml').exists():
        failures.append("crates/meerschaum-cli/pyproject.toml exists: there is one PyPI wheel (D8 revised)")

    ### Version spelling (see the module docstring).
    problem = cargo_prerelease_problem(cargo_version)
    if problem:
        (failures if problem[0] == 'fail' else notes).append(problem[1])
    if cargo_to_pep440(pep440_to_cargo(pep440)) != pep440:
        failures.append(f"version mapping is not reversible for {cargo_version} <-> {pep440}")

    ### 9. Lockstep workspace dependency versions.
    for name, spec in cargo['workspace'].get('dependencies', {}).items():
        if not name.startswith('meerschaum') or not isinstance(spec, dict) or 'path' not in spec:
            continue
        if spec.get('version') != f"={cargo_version}":
            failures.append(
                f"Cargo.toml [workspace.dependencies] {name}: version is {spec.get('version')!r}, "
                f"expected '={cargo_version}' (lockstep; update it with [workspace.package] version)"
            )

    ### 10. crates.io metadata of every published crate.
    required = ('description', 'readme', 'keywords', 'categories', 'include')
    members = cargo['workspace']['members']
    for member in members:
        manifest = tomllib.loads((ROOT / member / 'Cargo.toml').read_text(encoding='utf-8'))
        pkg = manifest['package']
        label = f"{member}/Cargo.toml"
        if pkg.get('publish', True) is False:
            continue
        for key in required:
            if not pkg.get(key):
                failures.append(f"{label}: published crate lacks `{key}`")
        for key in ('license', 'repository', 'version'):
            ### A value, or `key.workspace = true` (which parses as `{'workspace': True}`).
            if not pkg.get(key):
                failures.append(f"{label}: published crate lacks `{key}`")
        readme = pkg.get('readme')
        if isinstance(readme, str) and not (ROOT / member / readme).is_file():
            failures.append(f"{label}: readme {readme!r} does not exist")
        if len(pkg.get('keywords', [])) > 5:
            failures.append(f"{label}: more than 5 keywords")
        for dep_table in ('dependencies', 'build-dependencies'):
            for dep, spec in manifest.get(dep_table, {}).items():
                if dep.startswith('meerschaum') and isinstance(spec, dict) and 'path' in spec \
                        and 'version' not in spec:
                    failures.append(f"{label}: path dependency {dep} has no `version`")

    ### 1. No dependencies.
    if project.get('dependencies'):
        failures.append(
            f"root pyproject.toml: dependencies are {project.get('dependencies')!r}, "
            "expected none (D8 revised, D20: the base wheel depends on nothing)"
        )

    ### 2b. Extras.
    toml_extras = expand_extras(project.get('optional-dependencies', {}))
    py_extras = {k: sorted(map(normalize, v)) for k, v in load_extras_from_packages_py().items()}
    for name in sorted(set(py_extras) | set(toml_extras)):
        if name not in toml_extras:
            failures.append(f"extra {name!r} is in _packages.py but not in pyproject.toml")
        elif name not in py_extras:
            failures.append(f"extra {name!r} is in pyproject.toml but not in _packages.py")
        elif sorted(set(py_extras[name])) != sorted(set(map(normalize, toml_extras[name]))):
            failures.append(
                f"extra {name!r} differs: _packages.py={sorted(set(py_extras[name]))} "
                f"pyproject.toml={sorted(set(map(normalize, toml_extras[name])))}"
            )

    ### 5 + 6. Generated files (requirements/*.txt and the CLI's extras table).
    want_requirements = expected_requirements(toml_extras)
    want_extras_rs = extras_rs_text(toml_extras)
    if args.write:
        REQUIREMENTS_DIR.mkdir(exist_ok=True)
        for path in REQUIREMENTS_DIR.glob('*.txt'):
            if path.name not in want_requirements:
                path.unlink()
        for filename, text in want_requirements.items():
            path = REQUIREMENTS_DIR / filename
            if not path.exists() or path.read_text(encoding='utf-8') != text:
                path.write_text(text, encoding='utf-8')
                print(f"wrote {path.relative_to(ROOT)}")
        if not EXTRAS_RS.exists() or EXTRAS_RS.read_text(encoding='utf-8') != want_extras_rs:
            EXTRAS_RS.write_text(want_extras_rs, encoding='utf-8')
            print(f"wrote {EXTRAS_RS.relative_to(ROOT)}")
    have_files = {p.name for p in REQUIREMENTS_DIR.glob('*.txt')} if REQUIREMENTS_DIR.exists() else set()
    for filename in sorted(have_files | set(want_requirements)):
        path = REQUIREMENTS_DIR / filename
        if filename not in want_requirements:
            failures.append(f"requirements/{filename} does not correspond to an extra (run --write)")
        elif filename not in have_files:
            failures.append(f"requirements/{filename} is missing (run --write)")
        elif path.read_text(encoding='utf-8') != want_requirements[filename]:
            failures.append(f"requirements/{filename} differs from the {filename[:-4]!r} extra (run --write)")
    if not EXTRAS_RS.exists():
        failures.append(f"{EXTRAS_RS.relative_to(ROOT)} is missing (run --write)")
    elif EXTRAS_RS.read_text(encoding='utf-8') != want_extras_rs:
        failures.append(f"{EXTRAS_RS.relative_to(ROOT)} differs from the extras in pyproject.toml (run --write)")

    generated = subprocess.run(
        [sys.executable, '-I', str(ROOT / 'scripts/rust/gen_packages_table.py')],
        capture_output=True, text=True,
    )
    if generated.returncode:
        failures.append('CLI dependency table generation failed: ' + generated.stderr.strip())
    else:
        current = PACKAGES_TABLE.read_text(encoding='utf-8') if PACKAGES_TABLE.exists() else None
        if current != generated.stdout:
            if args.write:
                PACKAGES_TABLE.write_text(generated.stdout, encoding='utf-8')
                print('wrote the CLI dependency groups table')
            else:
                failures.append('CLI packages_table.rs differs from _packages.py (run --write)')

    ### 3. The Python version.
    py_version = read_py_version()
    if py_version != pep440:
        failures.append(f"meerschaum/config/_version.py is {py_version!r}, Cargo is {pep440} ({cargo_version})")

    ### 7 + 8. Built wheels.
    if args.wheels:
        wheel_failures, wheel_notes = check_wheels(args.wheels, pep440)
        failures += wheel_failures
        notes += wheel_notes

    for note in notes:
        print(f"note: {note}")
    for failure in failures:
        print(f"FAIL: {failure}", file=sys.stderr)
    if failures:
        return 1
    suffix = f"; wheels in {args.wheels} agree" if args.wheels else ""
    print(
        f"packaging ok: Cargo {cargo_version} = _version.py {py_version} = wheel {pep440}; "
        f"{len(toml_extras)} extras in sync with requirements/ and CLI tables{suffix}"
    )
    return 0


if __name__ == '__main__':
    sys.exit(main())
