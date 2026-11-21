#!/usr/bin/env python3
"""Assemble pinned, already-vendored compiler inputs; never invoke Cargo or rustup.

Administrator image-build tool only. The runtime reads the fixed installed policy; no
argument or environment from this tool is an execution-context selector. Fetch/vendor
preparation is a separate release build step using the exact reviewed dependency lock.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import os
from pathlib import Path
import re
import shutil
import stat
import struct
import tempfile

MAX_FILES = 16_384
MAX_BYTES = 1024 * 1024 * 1024
PREFIX = Path("/usr/local/lib/meerschaum/compiler")
POLICY = Path("etc/meerschaum/native-runtime.json")


def encoded(value: object) -> bytes:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False).encode()


def digest(value: bytes) -> str:
    return hashlib.sha256(value).hexdigest()


def valid_digest(value: object) -> bool:
    return isinstance(value, str) and len(value) == 64 and all(c in "0123456789abcdef" for c in value)


def regular(path: Path):
    fd = os.open(path, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK | os.O_CLOEXEC)
    opened = os.fdopen(fd, "rb")
    if not stat.S_ISREG(os.fstat(fd).st_mode):
        opened.close()
        raise ValueError("Compiler inputs must be regular files without links or special nodes.")
    return opened


def files(root: Path) -> dict[str, tuple[Path, str, int]]:
    if root.is_symlink() or not root.is_dir() or root.resolve() != root:
        raise ValueError("Compiler input roots must be explicit canonical directories.")
    result: dict[str, tuple[Path, str, int]] = {}
    total = 0
    nodes = 0
    pending = [(root, 0)]
    while pending:
        directory, depth = pending.pop()
        if depth > 32:
            raise ValueError("Compiler input depth exceeds its bound.")
        for entry in sorted(directory.iterdir()):
            nodes += 1
            if nodes > MAX_FILES * 2:
                raise ValueError("Compiler inputs exceed their directory/entry bound.")
            mode = entry.lstat().st_mode
            if stat.S_ISDIR(mode):
                pending.append((entry, depth + 1))
                continue
            name = entry.relative_to(root).as_posix()
            if not stat.S_ISREG(mode) or "\\" in name or any(ord(c) < 32 for c in name):
                raise ValueError("Compiler inputs cannot contain links, special nodes or control names.")
            if len(result) >= MAX_FILES or len(name) > 1024:
                raise ValueError("Compiler inputs exceed their file/path bound; use a minimal toolchain.")
            hasher = hashlib.sha256()
            with regular(entry) as stream:
                while chunk := stream.read(64 * 1024):
                    total += len(chunk)
                    if total > MAX_BYTES:
                        raise ValueError("Compiler inputs exceed their one GiB bound.")
                    hasher.update(chunk)
            # Paths, rather than mutable input execute bits, define deterministic modes.
            # Direct toolchain/bin and rustlib/<target>/bin tools remain executable.
            result[name] = (entry, hasher.hexdigest(), 0o555 if "bin" in Path(name).parts else 0o444)
    return result


def tree_digest(entries: dict[str, tuple[Path, str, int]]) -> str:
    return digest(b"mrsm-compiler-inputs-v1\0" + encoded({name: item[1] for name, item in entries.items()}))


def architecture(path: Path) -> str:
    with regular(path) as stream:
        header = stream.read(20)
    if len(header) != 20 or header[:6] != b"\x7fELF\x02\x01":
        raise ValueError("The snapshot needs direct 64-bit little-endian ELF tools, not wrappers/shims.")
    machine = struct.unpack_from("<H", header, 18)[0]
    try:
        return {62: "x86_64", 183: "aarch64"}[machine]
    except KeyError as error:
        raise ValueError("Unsupported native compiler target architecture.") from error


def read_lock(path: Path) -> bytes:
    import tomllib
    with regular(path) as stream:
        data = stream.read(1024 * 1024 + 1)
    if len(data) > 1024 * 1024:
        raise ValueError("The source dependency lock exceeds its bound.")
    lock = tomllib.loads(data.decode())
    local = [package for package in lock.get("package", []) if "source" not in package]
    if len(local) != 1 or local[0].get("name") != "mrsm_native_source" or local[0].get("version") != "0.0.0":
        raise ValueError("The lock needs exactly the generated mrsm_native_source 0.0.0 local root.")
    return data


def validate_release(release: object) -> str:
    if (
        not isinstance(release, str)
        or len(release) > 128
        or re.fullmatch(r"[0-9]+\.[0-9]+\.[0-9]+(?:-[0-9A-Za-z.-]+)?(?:\+[0-9A-Za-z.-]+)?", release) is None
    ):
        raise ValueError("Invalid compiler release pin.")
    return release


def pins(path: Path, *, require_release: bool = True) -> dict:
    with regular(path) as stream:
        data = stream.read(16 * 1024 + 1)
    if len(data) > 16 * 1024:
        raise ValueError("Compiler input pins exceed their bound.")
    value = json.loads(data)
    expected_keys = {"schema", "architecture", "toolchain_sha256", "vendor_sha256", "lock_sha256"}
    if require_release:
        expected_keys.add("release")
    if not isinstance(value, dict) or set(value) != expected_keys:
        raise ValueError("Unknown or missing compiler input pin fields.")
    if type(value["schema"]) is not int or value["schema"] != 1:
        raise ValueError("Invalid compiler release pin.")
    if require_release:
        validate_release(value["release"])
    if value["architecture"] not in {"x86_64", "aarch64"} or not all(valid_digest(value[name]) for name in ("toolchain_sha256", "vendor_sha256", "lock_sha256")):
        raise ValueError("Invalid compiler material pin.")
    return value


def _freeze_tree(stage: Path) -> None:
    # Modes/mtime and all manifest ordering are independent of builder uid, input mtime,
    # source cache layout, CARGO_HOME, PATH, timezone, and SOURCE_DATE_EPOCH environment.
    nodes = 0
    for directory, dirs, names in os.walk(stage, topdown=False):
        nodes += len(dirs) + len(names)
        if nodes > MAX_FILES * 2:
            raise ValueError("The assembled snapshot exceeds its native entry bound.")
        for name in names:
            path = Path(directory) / name
            path.chmod(0o555 if path.stat().st_mode & 0o111 else 0o444)
            os.utime(path, (0, 0), follow_symlinks=False)
        for name in dirs:
            path = Path(directory) / name
            path.chmod(0o555)
            os.utime(path, (0, 0), follow_symlinks=False)
    stage.chmod(0o555)
    os.utime(stage, (0, 0))


def _assemble(
    toolchain: Path,
    vendor: Path,
    lock_path: Path,
    pin_path: Path,
    output: Path,
    *,
    include_policy: bool,
) -> str:
    """Write a new rootfs bundle after all supplied material matches its reviewed pins."""
    expected = pins(pin_path, require_release=include_policy)
    toolchain_files, vendor_files = files(toolchain), files(vendor)
    lock = read_lock(lock_path)
    if not vendor_files:
        raise ValueError("An offline compiler snapshot needs its vendored dependencies.")
    if (tree_digest(toolchain_files), tree_digest(vendor_files), digest(lock)) != (
        expected["toolchain_sha256"], expected["vendor_sha256"], expected["lock_sha256"]
    ):
        raise ValueError("Compiler inputs changed from their administrator-reviewed pins.")
    if not {"bin/cargo", "bin/rustc"}.issubset(toolchain_files):
        raise ValueError("The snapshot needs direct Cargo and rustc binaries.")
    if toolchain_files["bin/cargo"][1] == toolchain_files["bin/rustc"][1]:
        raise ValueError("The snapshot cannot use rustup's identical multicall shims.")
    if architecture(toolchain / "bin/cargo") != expected["architecture"] or architecture(toolchain / "bin/rustc") != expected["architecture"]:
        raise ValueError("Compiler ELF architecture differs from the image input pin.")
    entries = {"toolchain/" + name: item for name, item in toolchain_files.items()}
    entries.update({"vendor/" + name: item for name, item in vendor_files.items()})
    if len(entries) + 1 > MAX_FILES:
        raise ValueError("The combined compiler snapshot exceeds its file bound.")
    if output.exists() or output.is_symlink() or not output.is_absolute():
        raise ValueError("Output must be an explicit new absolute rootfs bundle directory.")
    output.parent.mkdir(parents=True, exist_ok=True)
    stage = Path(tempfile.mkdtemp(prefix=".native-compiler-stage-", dir=output.parent))
    try:
        snapshot = stage / "snapshot"
        snapshot.mkdir()
        hashes = {"Cargo.lock": digest(lock)}
        total = len(lock)
        for name, (source, pin, mode) in sorted(entries.items()):
            target = snapshot / name
            target.parent.mkdir(parents=True, exist_ok=True)
            hasher = hashlib.sha256()
            with regular(source) as src, target.open("xb") as dest:
                while chunk := src.read(64 * 1024):
                    total += len(chunk)
                    if total > MAX_BYTES:
                        raise ValueError("The combined snapshot exceeds its one GiB bound.")
                    hasher.update(chunk)
                    dest.write(chunk)
            if hasher.hexdigest() != pin:
                raise ValueError("Compiler input changed while being copied.")
            hashes[name] = pin
            target.chmod(mode)
        (snapshot / "Cargo.lock").write_bytes(lock)
        manifest = encoded({"schema": 1, "files": hashes})
        if len(manifest) > 2 * 1024 * 1024:
            raise ValueError("The compiler manifest exceeds its native loader bound.")
        identity = digest(manifest)
        (snapshot / "compiler.json").write_bytes(manifest)
        installed = stage / PREFIX.relative_to("/") / identity
        installed.parent.mkdir(parents=True)
        snapshot.rename(installed)
        if include_policy:
            policy = stage / POLICY
            policy.parent.mkdir(parents=True)
            policy.write_bytes(encoded({
                "schema": 1, "release": expected["release"], "architecture": expected["architecture"],
                "compiler": {"snapshot": str(PREFIX / identity), "manifest_sha256": identity, "anchor": "/usr/lib"},
            }))
        _freeze_tree(stage)
        stage.rename(output)
        return identity
    finally:
        if stage.exists():
            # Only this never-published staging bundle is removed on failure.
            for directory, _, _ in os.walk(stage):
                Path(directory).chmod(0o700)
            shutil.rmtree(stage)


def assemble(toolchain: Path, vendor: Path, lock_path: Path, pin_path: Path, output: Path) -> str:
    """Write a new rootfs bundle with both snapshot material and release policy."""
    return _assemble(toolchain, vendor, lock_path, pin_path, output, include_policy=True)


def assemble_material(toolchain: Path, vendor: Path, lock_path: Path, pin_path: Path, output: Path) -> str:
    """Write a release-independent snapshot rootfs (no /etc/meerschaum/native-runtime.json)."""
    return _assemble(toolchain, vendor, lock_path, pin_path, output, include_policy=False)


def locate_snapshot(snapshot_rootfs: Path) -> tuple[str, Path]:
    """Return the sole verified (identity, snapshot_dir) under snapshot_rootfs/PREFIX."""
    prefix_dir = snapshot_rootfs / PREFIX.relative_to("/")
    if prefix_dir.is_symlink() or not prefix_dir.is_dir():
        raise ValueError("Snapshot rootfs does not contain a compiler snapshot directory.")
    entries = sorted(prefix_dir.iterdir())
    if len(entries) != 1 or entries[0].is_symlink() or not entries[0].is_dir():
        raise ValueError("Snapshot rootfs must contain exactly one compiler snapshot.")
    snapshot_dir = entries[0]
    identity = snapshot_dir.name
    if not valid_digest(identity):
        raise ValueError("Compiler snapshot directory name is not a valid SHA-256 identity.")
    with regular(snapshot_dir / "compiler.json") as stream:
        manifest_bytes = stream.read(2 * 1024 * 1024 + 1)
    if len(manifest_bytes) > 2 * 1024 * 1024 or digest(manifest_bytes) != identity:
        raise ValueError("Compiler snapshot manifest digest does not match its identity.")
    manifest = json.loads(manifest_bytes)
    if (
        not isinstance(manifest, dict)
        or set(manifest) != {"schema", "files"}
        or manifest["schema"] != 1
        or not isinstance(manifest["files"], dict)
        or not {"Cargo.lock", "toolchain/bin/cargo", "toolchain/bin/rustc"}.issubset(manifest["files"])
    ):
        raise ValueError("Compiler snapshot manifest is incomplete or malformed.")
    return identity, snapshot_dir


def write_policy(snapshot_rootfs: Path, release: str, architecture_name: str, output: Path) -> str:
    """Write only /etc/meerschaum/native-runtime.json for an already-sealed compiler snapshot."""
    validate_release(release)
    if architecture_name not in {"x86_64", "aarch64"}:
        raise ValueError("Invalid compiler material pin.")
    if output.exists() or output.is_symlink() or not output.is_absolute():
        raise ValueError("Output must be an explicit new absolute rootfs bundle directory.")
    identity, snapshot_dir = locate_snapshot(snapshot_rootfs)
    if (
        architecture(snapshot_dir / "toolchain/bin/cargo") != architecture_name
        or architecture(snapshot_dir / "toolchain/bin/rustc") != architecture_name
    ):
        raise ValueError("Compiler ELF architecture differs from the image input pin.")
    output.parent.mkdir(parents=True, exist_ok=True)
    stage = Path(tempfile.mkdtemp(prefix=".native-compiler-policy-", dir=output.parent))
    try:
        policy = stage / POLICY
        policy.parent.mkdir(parents=True)
        policy.write_bytes(encoded({
            "schema": 1, "release": release, "architecture": architecture_name,
            "compiler": {"snapshot": str(PREFIX / identity), "manifest_sha256": identity, "anchor": "/usr/lib"},
        }))
        _freeze_tree(stage)
        stage.rename(output)
        return identity
    finally:
        if stage.exists():
            for directory, _, _ in os.walk(stage):
                Path(directory).chmod(0o700)
            shutil.rmtree(stage)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    for name in ("toolchain", "vendor", "lock", "pins", "output"):
        parser.add_argument("--" + name, required=True, type=Path)
    args = parser.parse_args()
    print(assemble(args.toolchain, args.vendor, args.lock, args.pins, args.output))


if __name__ == "__main__":
    main()
