"""Assembler unit fixtures only; no Cargo, Rust, image, or services are invoked."""
from __future__ import annotations

import importlib.machinery
import importlib.util
import json
import os
from pathlib import Path
import stat
import struct
import tempfile
import unittest

SCRIPT = Path(__file__).resolve().parents[1] / "scripts/docker/native_compiler_snapshot.py"
loader = importlib.machinery.SourceFileLoader("native_compiler_snapshot", str(SCRIPT))
spec = importlib.util.spec_from_loader(loader.name, loader)
snapshot = importlib.util.module_from_spec(spec)
loader.exec_module(snapshot)


class SnapshotAssembly(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.root = Path(self.temp.name).resolve()
        self.toolchain = self.root / "toolchain"
        self.vendor = self.root / "vendor"
        self.toolchain.joinpath("bin").mkdir(parents=True)
        self.vendor.joinpath("fixture", "src").mkdir(parents=True)
        header = bytearray(20)
        header[:6] = b"\x7fELF\x02\x01"
        struct.pack_into("<H", header, 18, 62)
        # These distinct ELF-marked fixtures are never executed by the assembler.
        (self.toolchain / "bin/cargo").write_bytes(header + b"cargo")
        (self.toolchain / "bin/rustc").write_bytes(header + b"rustc")
        (self.vendor / "fixture/src/lib.rs").write_text("pub fn fixture() {}")
        self.lock = self.root / "Cargo.lock"
        self.lock.write_text('version = 4\n[[package]]\nname = "mrsm_native_source"\nversion = "0.0.0"\n')
        self.pinfile = self.root / "pins.json"
        self.pins = {
            "schema": 1, "release": "5.1.0", "architecture": "x86_64",
            "toolchain_sha256": snapshot.tree_digest(snapshot.files(self.toolchain)),
            "vendor_sha256": snapshot.tree_digest(snapshot.files(self.vendor)),
            "lock_sha256": snapshot.digest(self.lock.read_bytes()),
        }
        self.pinfile.write_bytes(snapshot.encoded(self.pins))

    def tearDown(self):
        # Only this test's own never-deployed rootfs bundles are made removable.
        for directory, _, _ in os.walk(self.root):
            Path(directory).chmod(0o700)
        self.temp.cleanup()

    def assemble(self, name="output"):
        output = self.root / name
        identity = snapshot.assemble(self.toolchain, self.vendor, self.lock, self.pinfile, output)
        return output, identity

    def test_identical_pinned_inputs_produce_identical_frozen_bundle_and_policy(self):
        first, identity = self.assemble("one")
        # Mutable input mtime and execute bits cannot change bundle identity/modes.
        os.utime(self.toolchain / "bin/cargo", (12345, 12345))
        (self.toolchain / "bin/rustc").chmod(0o777)
        second, again = self.assemble("two")
        self.assertEqual(identity, again)
        self.assertEqual(snapshot.tree_digest(snapshot.files(first)), snapshot.tree_digest(snapshot.files(second)))
        policy = json.loads(first.joinpath(snapshot.POLICY).read_bytes())
        self.assertEqual(policy, {
            "schema": 1, "release": "5.1.0", "architecture": "x86_64",
            "compiler": {"snapshot": str(snapshot.PREFIX / identity), "manifest_sha256": identity, "anchor": "/usr/lib"},
        })
        material = first / snapshot.PREFIX.relative_to("/") / identity
        manifest_bytes = (material / "compiler.json").read_bytes()
        self.assertEqual(snapshot.digest(manifest_bytes), identity)
        manifest = json.loads(manifest_bytes)
        self.assertEqual(set(manifest), {"schema", "files"})
        self.assertIn("Cargo.lock", manifest["files"])
        self.assertIn("vendor/fixture/src/lib.rs", manifest["files"])
        for directory, _, names in os.walk(first):
            self.assertEqual(stat.S_IMODE(Path(directory).stat().st_mode), 0o555)
            for name in names:
                path = Path(directory) / name
                self.assertEqual(path.stat().st_mode & 0o222, 0)
                self.assertEqual(path.stat().st_mtime_ns, 0)
        self.assertEqual(stat.S_IMODE(material.joinpath("toolchain/bin/cargo").stat().st_mode), 0o555)

    def test_changed_vendor_or_lock_is_rejected_before_output_creation(self):
        source = self.vendor / "fixture/src/lib.rs"
        source.write_text("pub fn changed() {}")
        with self.assertRaisesRegex(ValueError, "reviewed pins"):
            self.assemble()
        self.assertFalse(self.root.joinpath("output").exists())
        source.write_text("pub fn fixture() {}")
        self.lock.write_text(self.lock.read_text() + "\n# changed\n")
        with self.assertRaisesRegex(ValueError, "reviewed pins"):
            self.assemble()
        self.assertFalse(self.root.joinpath("output").exists())

    def test_vendor_link_and_toolchain_multicall_shims_are_refused(self):
        alias = self.vendor / "alias"
        alias.symlink_to(self.vendor / "fixture", target_is_directory=True)
        with self.assertRaisesRegex(ValueError, "links"):
            self.assemble()
        alias.unlink()
        (self.toolchain / "bin/rustc").write_bytes((self.toolchain / "bin/cargo").read_bytes())
        self.pins["toolchain_sha256"] = snapshot.tree_digest(snapshot.files(self.toolchain))
        self.pinfile.write_bytes(snapshot.encoded(self.pins))
        with self.assertRaisesRegex(ValueError, "multicall"):
            self.assemble()
        self.assertFalse(self.root.joinpath("output").exists())

    def test_wrong_architecture_unknown_pin_and_existing_output_refuse(self):
        self.pins["architecture"] = "aarch64"
        self.pinfile.write_bytes(snapshot.encoded(self.pins))
        with self.assertRaisesRegex(ValueError, "architecture"):
            self.assemble()
        self.pins["architecture"] = "x86_64"
        self.pins["cargo_wrapper"] = "/untrusted"
        self.pinfile.write_bytes(snapshot.encoded(self.pins))
        with self.assertRaisesRegex(ValueError, "Unknown"):
            self.assemble()
        del self.pins["cargo_wrapper"]
        self.pinfile.write_bytes(snapshot.encoded(self.pins))
        output = self.root / "output"
        output.mkdir()
        output.joinpath("operator-file").write_text("keep")
        with self.assertRaisesRegex(ValueError, "new absolute"):
            self.assemble()
        self.assertEqual(output.joinpath("operator-file").read_text(), "keep")

    def test_split_material_and_policy_match_combined_assembly(self):
        combined, combined_id = self.assemble("combined")
        material_pins = {k: v for k, v in self.pins.items() if k != "release"}
        material_pinfile = self.root / "material-pins.json"
        material_pinfile.write_bytes(snapshot.encoded(material_pins))
        material_out = self.root / "material"
        material_id = snapshot.assemble_material(
            self.toolchain, self.vendor, self.lock, material_pinfile, material_out
        )
        self.assertEqual(material_id, combined_id)
        self.assertFalse(material_out.joinpath(snapshot.POLICY).exists())

        policy_out = self.root / "policy"
        policy_id = snapshot.write_policy(material_out, "5.1.0", "x86_64", policy_out)
        self.assertEqual(policy_id, combined_id)
        self.assertEqual(
            policy_out.joinpath(snapshot.POLICY).read_bytes(),
            combined.joinpath(snapshot.POLICY).read_bytes(),
        )
        self.assertEqual(stat.S_IMODE(policy_out.joinpath(snapshot.POLICY).stat().st_mode), 0o444)
        self.assertEqual(policy_out.joinpath(snapshot.POLICY).stat().st_mtime_ns, 0)

        # Updating the release version only regenerates the tiny policy, never the snapshot.
        policy_next = self.root / "policy-next"
        self.assertEqual(snapshot.write_policy(material_out, "5.1.1", "x86_64", policy_next), combined_id)
        self.assertEqual(json.loads(policy_next.joinpath(snapshot.POLICY).read_bytes())["release"], "5.1.1")

        # Invalid release or mismatched architecture is refused with no partial output.
        with self.assertRaisesRegex(ValueError, "release pin"):
            snapshot.write_policy(material_out, "not-a-release", "x86_64", self.root / "bad-rel")
        self.assertFalse(self.root.joinpath("bad-rel").exists())
        with self.assertRaisesRegex(ValueError, "architecture"):
            snapshot.write_policy(material_out, "5.1.1", "aarch64", self.root / "bad-arch")
        self.assertFalse(self.root.joinpath("bad-arch").exists())


if __name__ == "__main__":
    unittest.main()
