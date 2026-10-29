"""
Where a test finds a standalone `mrsm` binary (`cargo build -p meerschaum --bin mrsm`).

The product runs the CLI from the extension (`pip install meerschaum`), so only the tests which
compare the two programs, or exercise the binary's own behavior (`tests/console`, the loopback API
server fixtures), need one.

- `MRSM_TEST_BIN` (else `MRSM_BIN`) names it explicitly. CI downloads it with a provenance manifest.
  Neither changes what the product runs: `MRSM_BIN` does, which is why the workflows pass
  `MRSM_TEST_BIN`.
- Outside CI the newest `target/{release,debug}/mrsm` of this checkout is used when nothing is set.
  CI never looks in `target/`: the shared runner checkout keeps binaries from earlier runs.
- A binary is **checked** before it is used: its version must equal the extension's, and downloaded
  artifacts must match the actual checkout commit/tree, build profile and payload checksums.
  Local builds without manifests retain the source-age check. A mismatch fails loudly in CI.
"""

from __future__ import annotations

import functools
import os
import pathlib
import subprocess
import tempfile
from typing import Optional

REPO = pathlib.Path(__file__).resolve().parent.parent
NAME = 'mrsm.exe' if os.name == 'nt' else 'mrsm'
ENV = 'MRSM_TEST_BIN'
MANIFEST_ENV = 'MRSM_TEST_ARTIFACT_MANIFEST'
SOURCE_SUFFIXES = ('.rs', '.toml', '.lock')


class StaleBinaryError(RuntimeError):
    """The standalone `mrsm` is not the build of this checkout."""


def in_ci() -> bool:
    return bool(os.environ.get('CI') or os.environ.get('GITHUB_ACTIONS'))


def skip_or_fail(message: str) -> None:
    """
    For a dependency CI is supposed to provide (the package index, `.venv-ref`): fail the test in
    CI, where a silent skip would hide a broken setup, and skip it on a developer's machine.
    """
    import pytest
    if in_ci():
        pytest.fail(message)
    pytest.skip(message)


def _is_program(path: pathlib.Path) -> bool:
    if not (path.is_file() and os.access(path, os.X_OK)):
        return False
    try:
        with open(path, 'rb') as f:
            return f.read(2) != b'#!'
    except OSError:
        return False


@functools.lru_cache(maxsize=1)
def newest_source_mtime() -> float:
    """The modification time of the newest Rust source or manifest of this checkout."""
    newest = 0.0
    for top in ('Cargo.toml', 'Cargo.lock'):
        path = REPO / top
        if path.exists():
            newest = max(newest, path.stat().st_mtime)
    for root, dirs, files in os.walk(REPO / 'crates'):
        dirs[:] = [d for d in dirs if d not in ('target', 'tests', 'node_modules', '.git')]
        for name in files:
            if name.endswith(SOURCE_SUFFIXES):
                try:
                    newest = max(newest, os.stat(os.path.join(root, name)).st_mtime)
                except OSError:
                    continue
    return newest


@functools.lru_cache(maxsize=None)
def _binary_version(path: str) -> str:
    with tempfile.TemporaryDirectory(prefix='mrsm-binary-check-') as scratch:
        env = {
            k: v for k, v in os.environ.items()
            if not k.startswith('MRSM_') and k not in ('TMUX', 'TMUX_PANE')
        }
        env.update({
            'MRSM_ROOT_DIR': scratch,
            'MRSM_PLUGINS_DIR': scratch,
        })
        result = subprocess.run(
            [path, 'show', 'version', '--nopretty'],
            capture_output=True, text=True, env=env, timeout=120, cwd=scratch,
        )
    return result.stdout.strip()


def check_binary(path: str) -> None:
    """Require this checkout's version and verified artifact provenance (or local source age)."""
    manifest = os.environ.get(MANIFEST_ENV)
    if manifest:
        from scripts.ci.artifacts import ArtifactError, verify_manifest

        try:
            verify_manifest(
                pathlib.Path(manifest), kind='binary',
                profile=os.environ.get('MRSM_TEST_BIN_PROFILE', 'release'),
                artifact=pathlib.Path(path), repo=REPO,
            )
        except ArtifactError as exc:
            raise StaleBinaryError(f'{path}: {exc}') from exc
    elif in_ci():
        raise StaleBinaryError(
            f'CI requires {MANIFEST_ENV} for {path}; download its verified manifest with the binary.'
        )
    from meerschaum import _core

    extension = _core.__version__
    reported = _binary_version(path)
    if reported != extension:
        raise StaleBinaryError(
            f"{path} reports version {reported!r}, the extension is {extension!r}: it is not a "
            "build of this tree. Rebuild it with `cargo build -p meerschaum --bin mrsm`."
        )
    if manifest:
        return
    built = pathlib.Path(path).stat().st_mtime
    newest = newest_source_mtime()
    if built < newest:
        raise StaleBinaryError(
            f"{path} is older than the Rust sources of this checkout ({built:.0f} < {newest:.0f}): "
            "rebuild it with `cargo build -p meerschaum --bin mrsm`."
        )


def find_binary() -> Optional[str]:
    """
    Return the path of a verified standalone `mrsm` binary, or `None` when there is none (or, outside
    CI, when it is stale: the reason is in `last_problem`). A stale or mismatched binary raises
    `StaleBinaryError` in CI.
    """
    global last_problem
    last_problem = ''
    explicit = os.environ.get(ENV) or os.environ.get('MRSM_BIN', '')
    candidates = []
    if explicit:
        candidates.append(explicit)
    elif not in_ci():
        builds = [REPO / 'target' / profile / NAME for profile in ('release', 'debug')]
        builds = [build for build in builds if _is_program(build)]
        builds.sort(key=lambda build: build.stat().st_mtime, reverse=True)
        candidates.extend(str(build) for build in builds[:1])
    for candidate in candidates:
        if not _is_program(pathlib.Path(candidate)):
            last_problem = f"{candidate} is not an executable program"
            if in_ci():
                raise StaleBinaryError(last_problem)
            continue
        try:
            check_binary(candidate)
        except StaleBinaryError as e:
            last_problem = str(e)
            if in_ci():
                raise
            continue
        return candidate
    return None


last_problem = ''


def binary_or_skip():
    """The verified binary's path, or `pytest.skip` saying why there is none."""
    import pytest

    binary = find_binary()
    if binary is None:
        pytest.skip(
            last_problem
            or f"No standalone `mrsm` (build it: cargo build -p meerschaum --bin mrsm; or set {ENV})."
        )
    return binary
