#!/usr/bin/env python3
"""
Check the `.crate` archives `cargo package --workspace` wrote to `target/package/`
(docs/design/v5-build.md, "Publishing to crates.io").

    python scripts/rust/check_packages.py [--dir target/package] [--limit-mb 10]

For every archive:

- the size is under crates.io's 10 MB limit (`--limit-mb`; `meerschaum-web`, which vendors Plotly,
  MapLibre and Cytoscape, is the one that matters);
- it ships no tests, fixtures, benches, generators, `.github`, virtualenvs or build output (each
  crate declares an explicit `include`, so a path here means the list is too broad);
- it carries a README, the root `LICENSE` and `NOTICE`, and a `Cargo.toml` with `description`, `license` and `repository`.

- the `meerschaum` front crate also ships `src/main.rs` and its manifest declares the `mrsm` binary
  (`[[bin]] name = "mrsm"`, `required-features = ["cli"]`), a `cli` feature and `default = [.., "cli"]`:
  `cargo install meerschaum` must build the standalone CLI.

Prints a size table. Exit 1 on any failure. Standard library only.
"""

from __future__ import annotations

import argparse
import re
import sys
import tarfile
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent.parent

FORBIDDEN = re.compile(
    r'(^|/)(tests?|fixtures|benches|bench|\.github|\.venv[^/]*|node_modules|target)(/|$)'
    r'|(^|/)gen_[^/]*\.py$|\.cast$|PARITY\.md$'
)


def check(archive: Path, limit: int) -> tuple[int, list[str]]:
    failures: list[str] = []
    size = archive.stat().st_size
    if size >= limit:
        failures.append(f"{archive.name}: {size / 1e6:.1f} MB is not under the {limit / 1e6:.0f} MB limit")
    stem = archive.name[: -len('.crate')]
    with tarfile.open(archive, 'r:gz') as tar:
        names = [m.name for m in tar.getmembers() if m.isfile()]
        manifest_name = f"{stem}/Cargo.toml"
        for name in names:
            relative = name[len(stem) + 1:] if name.startswith(stem + '/') else name
            if FORBIDDEN.search(relative):
                failures.append(f"{archive.name}: ships {relative}")
        if not any(re.fullmatch(re.escape(stem) + r'/README(\.md)?', n) for n in names):
            failures.append(f"{archive.name}: has no README.md")
        for legal in ('LICENSE', 'NOTICE'):
            if f"{stem}/{legal}" not in names:
                failures.append(f"{archive.name}: has no {legal} (symlink it to the root file and add it to `include`)")
        if manifest_name not in names:
            failures.append(f"{archive.name}: has no Cargo.toml")
        else:
            manifest = tar.extractfile(manifest_name).read().decode('utf-8')
            for key in ('description', 'license', 'repository'):
                if not re.search(rf'^{key}\s*=', manifest, re.M):
                    failures.append(f"{archive.name}: Cargo.toml has no `{key}`")
            if re.fullmatch(r'meerschaum-\d.*', stem):
                if f"{stem}/src/main.rs" not in names:
                    failures.append(f"{archive.name}: has no src/main.rs (the `mrsm` binary)")
                if not re.search(r'\[\[bin\]\]\s*(?:#[^\n]*\n\s*)*name\s*=\s*"mrsm"', manifest):
                    failures.append(f"{archive.name}: Cargo.toml declares no `mrsm` binary")
                if not re.search(r'^cli\s*=', manifest, re.M):
                    failures.append(f"{archive.name}: Cargo.toml has no `cli` feature")
                default = re.search(r'^default\s*=\s*\[([^\]]*)\]', manifest, re.M)
                if not default or '"cli"' not in default.group(1):
                    failures.append(f"{archive.name}: the `cli` feature is not a default feature")
    return size, failures


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument('--dir', type=Path, default=ROOT / 'target' / 'package')
    parser.add_argument('--limit-mb', type=float, default=10.0)
    args = parser.parse_args()

    archives = sorted(args.dir.glob('*.crate'))
    if not archives:
        print(f"FAIL: no .crate archives in {args.dir} (run `cargo package --workspace --no-verify` first)", file=sys.stderr)
        return 1
    limit = int(args.limit_mb * 1_000_000)
    failures: list[str] = []
    print(f"{'archive':<48} {'size':>10}")
    for archive in archives:
        size, problems = check(archive, limit)
        failures += problems
        print(f"{archive.name:<48} {size / 1e6:>8.2f} MB")
    for failure in failures:
        print(f"FAIL: {failure}", file=sys.stderr)
    if failures:
        return 1
    print(f"packages ok: {len(archives)} archives, all under {args.limit_mb:g} MB with no tests, fixtures or benches")
    return 0


if __name__ == '__main__':
    sys.exit(main())
