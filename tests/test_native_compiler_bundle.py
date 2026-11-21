"""Image-assembler fixtures only: tiny ELF headers are never executed.

These checks do not compile a plugin or establish final Docker/runtime compatibility.
"""
from __future__ import annotations

import importlib.util
import json
import os
from pathlib import Path
import re
import stat
import struct
import sys
import tempfile
import tomllib
import unittest
from unittest import mock


SCRIPTS = Path(__file__).resolve().parents[1] / 'scripts/docker'


def load(name, path):
    spec = importlib.util.spec_from_file_location(name, path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


snapshot = load('_mrsm_bundle_snapshot_fixture', SCRIPTS / 'native_compiler_snapshot.py')
with mock.patch.dict(sys.modules, {'native_compiler_snapshot': snapshot}):
    bundle = load('_mrsm_compiler_bundle_fixture', SCRIPTS / 'build_native_compiler_bundle.py')


def elf(label, machine=62):
    header = bytearray(20)
    header[:6] = b'\x7fELF\x02\x01'
    struct.pack_into('<H', header, 18, machine)
    return bytes(header) + label.encode()


class CompilerBundle(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.root = Path(self.temp.name).resolve()
        self.toolchain = self.root / 'stable-x86_64-unknown-linux-gnu'
        self.toolchain.joinpath('bin').mkdir(parents=True)
        for name in ['cargo', 'rustc', 'rustfmt', 'clippy-driver']:
            self.toolchain.joinpath('bin', name).write_bytes(elf(name))
        self.toolchain.joinpath('lib').mkdir()
        self.toolchain.joinpath('lib', 'fixture.rlib').write_bytes(b'never-linked fixture library')
        self.vendor = self.root / 'vendor'
        self.vendor.joinpath('fixture', 'src').mkdir(parents=True)
        self.vendor.joinpath('fixture', 'src', 'lib.rs').write_text('pub fn fixture() {}')
        self.lock = self.root / 'Cargo.lock'
        self.lock.write_text('version = 4\n[[package]]\nname = "mrsm_native_source"\nversion = "0.0.0"\n')
        self.manifest = self.root / 'Cargo.toml'
        self.manifest.write_text('[workspace.package]\nversion = "5.1.0-rc.1"\n')
        self.settings = self.root / 'settings.toml'
        self.settings.write_text('version = "12"\ndefault_toolchain = "' + self.toolchain.name
                                 + '"\nprofile = "minimal"\n\n[overrides]\n')
        self.output = self.root / 'output'

    def tearDown(self):
        # Only this test's isolated, never-published bundles are made removable.
        # followlinks=False deliberately leaves compatibility-link destinations alone.
        for directory, _, _ in os.walk(self.root, followlinks=False):
            Path(directory).chmod(0o700)
        self.temp.cleanup()

    def build(self, output=None):
        # Any accidental process invocation is a failure. The fixtures are ELF headers,
        # not executable Rust tools, and remain non-executable in the input directory.
        with (
            mock.patch('subprocess.run', side_effect=AssertionError('No compiler invocation')),
            mock.patch('subprocess.Popen', side_effect=AssertionError('No compiler invocation')),
        ):
            return bundle.build(self.toolchain, self.vendor, self.lock, self.manifest,
                                self.settings, output or self.output)

    def test_policy_pins_and_ordinary_rustup_use_one_frozen_toolchain(self):
        identity = self.build()
        rootfs = self.output / 'rootfs'
        material = rootfs / snapshot.PREFIX.relative_to('/') / identity
        pins = json.loads(self.output.joinpath('input-pins.json').read_bytes())
        self.assertEqual(pins, {
            'schema': 1, 'release': '5.1.0-rc.1', 'architecture': 'x86_64',
            'toolchain_sha256': snapshot.tree_digest(snapshot.files(self.toolchain)),
            'vendor_sha256': snapshot.tree_digest(snapshot.files(self.vendor)),
            'lock_sha256': snapshot.digest(self.lock.read_bytes()),
        })
        self.assertEqual(json.loads(rootfs.joinpath(snapshot.POLICY).read_bytes()), {
            'schema': 1, 'release': '5.1.0-rc.1', 'architecture': 'x86_64',
            'compiler': {'snapshot': str(snapshot.PREFIX / identity),
                         'manifest_sha256': identity, 'anchor': '/usr/lib'},
        })
        manifest = material / 'compiler.json'
        self.assertEqual(snapshot.digest(manifest.read_bytes()), identity)
        hashes = json.loads(manifest.read_bytes())['files']
        self.assertEqual(material.joinpath('Cargo.lock').read_bytes(), self.lock.read_bytes())
        self.assertEqual(hashes['Cargo.lock'], pins['lock_sha256'])
        compat = self.output / 'compat/usr/local/rustup'
        self.assertEqual(compat.joinpath('settings.toml').read_bytes(), self.settings.read_bytes())
        self.assertEqual(stat.S_IMODE((compat / 'update-hashes').stat().st_mode), 0o555)
        self.assertEqual(list(compat.joinpath('toolchains').iterdir()),
                         [compat / 'toolchains' / self.toolchain.name])
        alias = compat / 'toolchains' / self.toolchain.name
        self.assertTrue(alias.is_symlink())
        self.assertEqual(os.readlink(alias), str(snapshot.PREFIX / identity / 'toolchain'))
        # The sole absolute compatibility alias targets the exact content-addressed tree;
        # there is no second materialized Rustup toolchain hidden elsewhere in the bundle.
        copies = []
        for directory, _, names in os.walk(self.output, followlinks=False):
            if 'rustc' in names:
                copies.append(Path(directory) / 'rustc')
        self.assertEqual(copies, [material / 'toolchain/bin/rustc'])
        for name in ['cargo', 'rustc', 'rustfmt', 'clippy-driver']:
            tool = material / 'toolchain/bin' / name
            self.assertEqual(tool.read_bytes(), elf(name))
            self.assertEqual(stat.S_IMODE(tool.stat().st_mode), 0o555)
        self.assertEqual(stat.S_IMODE(compat.joinpath('settings.toml').stat().st_mode), 0o444)

    def test_settings_must_select_the_actual_toolchain_and_stay_bounded(self):
        original = self.settings.read_bytes()
        for bad in [
            b'default_toolchain = "nightly-other"\n',
            b'profile = "minimal"\n',
            b'default_toolchain = ["stable-x86_64-unknown-linux-gnu"]\n',
            b'default_toolchain = "unterminated',
            b' ' * (16 * 1024 + 1),
        ]:
            with self.subTest(settings=bad[:60]):
                self.settings.write_bytes(bad)
                with self.assertRaises(ValueError):
                    self.build()
                self.assertFalse(self.output.exists())
        self.settings.write_bytes(original)

    def test_aarch64_pins_come_from_direct_tools_not_the_build_hosts_architecture(self):
        selected = self.root / 'stable-aarch64-unknown-linux-gnu'
        self.toolchain.rename(selected)
        self.toolchain = selected
        for name in ['cargo', 'rustc', 'rustfmt', 'clippy-driver']:
            self.toolchain.joinpath('bin', name).write_bytes(elf(name, machine=183))
        self.settings.write_text('default_toolchain = "' + selected.name + '"\n')
        identity = self.build()
        policy = json.loads(self.output.joinpath('rootfs', snapshot.POLICY).read_bytes())
        self.assertEqual(policy['architecture'], 'aarch64')
        alias = self.output / 'compat/usr/local/rustup/toolchains' / selected.name
        self.assertEqual(os.readlink(alias), str(snapshot.PREFIX / identity / 'toolchain'))

    def test_wrong_lock_release_or_cargo_architecture_leave_no_partial_output(self):
        original_lock = self.lock.read_bytes()
        self.lock.write_text('version=4\n[[package]]\nname="different_local_root"\nversion="0.0.0"\n')
        with self.assertRaisesRegex(ValueError, 'mrsm_native_source'):
            self.build()
        self.assertFalse(self.output.exists())
        self.lock.write_bytes(original_lock)
        self.manifest.write_text('[workspace.package]\nversion="not-a-release"\n')
        with self.assertRaisesRegex(ValueError, 'release pin'):
            self.build()
        self.assertFalse(self.output.exists())
        self.manifest.write_text('[workspace.package]\nversion="5.1.0-rc.1"\n')
        self.toolchain.joinpath('bin/cargo').write_bytes(elf('cargo', machine=183))
        with self.assertRaisesRegex(ValueError, 'architecture'):
            self.build()
        self.assertFalse(self.output.exists())
        self.assertFalse(list(self.root.glob('.native-compiler-stage-*')))

    def test_reviewed_material_changed_between_pin_and_copy_is_refused_and_cleaned(self):
        original_assemble = snapshot.assemble

        def changed_material(*args, **kwargs):
            self.vendor.joinpath('fixture/src/lib.rs').write_text('pub fn changed_after_pin() {}')
            return original_assemble(*args, **kwargs)

        survivor = self.root / 'operator-owned-file'
        survivor.write_text('keep')
        with mock.patch.object(snapshot, 'assemble', side_effect=changed_material):
            with self.assertRaisesRegex(ValueError, 'reviewed pins'):
                self.build()
        self.assertFalse(self.output.exists())
        self.assertFalse(list(self.root.glob('.native-compiler-stage-*')))
        self.assertEqual(survivor.read_text(), 'keep')

    def test_existing_output_and_output_parent_symlink_are_never_adopted(self):
        self.output.mkdir()
        sentinel = self.output / 'operator-owned-file'
        sentinel.write_text('keep')
        with self.assertRaisesRegex(ValueError, 'new absolute'):
            self.build()
        self.assertEqual(sentinel.read_text(), 'keep')
        alias = self.root / 'output-alias'
        alias.symlink_to(self.output, target_is_directory=True)
        with self.assertRaisesRegex(ValueError, 'new absolute'):
            self.build(alias)
        self.assertTrue(alias.is_symlink())
        parent = self.root / 'parent-alias'
        parent.symlink_to(self.root, target_is_directory=True)
        with self.assertRaisesRegex(ValueError, 'new absolute'):
            self.build(parent / 'uncreated-output')
        self.assertFalse(self.root.joinpath('uncreated-output').exists())

    def test_internal_and_outside_input_links_remain_explicit_refusals(self):
        outside = self.root / 'outside'
        outside.write_text('do not copy this through a link')
        for name, target in [('internal-alias', self.toolchain / 'lib/fixture.rlib'),
                             ('outside-alias', outside)]:
            link = self.toolchain / 'lib' / name
            link.symlink_to(target)
            with self.subTest(link=name):
                with self.assertRaisesRegex(ValueError, 'links'):
                    self.build()
                self.assertFalse(self.output.exists())
            link.unlink()
        link = self.vendor / 'outside-alias'
        link.symlink_to(outside)
        with self.assertRaisesRegex(ValueError, 'links'):
            self.build()
        self.assertFalse(self.output.exists())
        self.assertEqual(outside.read_text(), 'do not copy this through a link')

    def test_compatibility_link_failure_removes_only_the_new_bundle(self):
        survivor = self.root / 'operator-owned-file'
        survivor.write_text('keep')
        with mock.patch.object(Path, 'symlink_to', side_effect=PermissionError('fixture link refusal')):
            with self.assertRaises(PermissionError):
                self.build()
        self.assertFalse(self.output.exists())
        self.assertFalse(list(self.root.glob('.native-compiler-stage-*')))
        self.assertEqual(survivor.read_text(), 'keep')

    def test_split_snapshot_and_policy_are_identical_and_independent_of_release_bumps(self):
        combined_out = self.root / 'combined'
        combined_id = self.build(combined_out)

        snap_out = self.root / 'snapshot-only'
        policy_out = self.root / 'policy-only'
        with (
            mock.patch('subprocess.run', side_effect=AssertionError('No compiler invocation')),
            mock.patch('subprocess.Popen', side_effect=AssertionError('No compiler invocation')),
        ):
            snap_id = bundle.build_snapshot(
                self.toolchain, self.vendor, self.lock, self.settings, snap_out
            )
            policy_id = bundle.build_policy(snap_out / 'rootfs', self.manifest, policy_out)

        self.assertEqual(snap_id, combined_id)
        self.assertEqual(policy_id, combined_id)
        self.assertFalse((snap_out / 'rootfs' / snapshot.POLICY).exists())
        self.assertEqual(
            (policy_out / snapshot.POLICY).read_bytes(),
            (combined_out / 'rootfs' / snapshot.POLICY).read_bytes(),
        )
        # Snapshot files match combined bundle files byte-for-byte.
        self.assertEqual(
            snapshot.tree_digest(snapshot.files(snap_out / 'rootfs' / snapshot.PREFIX.relative_to('/'))),
            snapshot.tree_digest(snapshot.files(combined_out / 'rootfs' / snapshot.PREFIX.relative_to('/'))),
        )
        # Re-binding the exact same snapshot to a bumped workspace version updates only the policy.
        self.manifest.write_text('[workspace.package]\nversion = "5.1.1"\n')
        policy_next = self.root / 'policy-next'
        with (
            mock.patch('subprocess.run', side_effect=AssertionError('No compiler invocation')),
            mock.patch('subprocess.Popen', side_effect=AssertionError('No compiler invocation')),
        ):
            next_id = bundle.build_policy(snap_out / 'rootfs', self.manifest, policy_next)
        self.assertEqual(next_id, snap_id)
        self.assertEqual(json.loads((policy_next / snapshot.POLICY).read_bytes())['release'], '5.1.1')

    def test_cli_snapshot_only_and_policy_only_modes(self):
        snap_out = self.root / 'cli-snap'
        policy_out = self.root / 'cli-policy'
        with (
            mock.patch('subprocess.run', side_effect=AssertionError('No compiler invocation')),
            mock.patch('subprocess.Popen', side_effect=AssertionError('No compiler invocation')),
            mock.patch.object(
                sys, 'argv', [
                    'build_native_compiler_bundle.py', '--snapshot-only',
                    '--toolchain', str(self.toolchain), '--vendor', str(self.vendor),
                    '--lock', str(self.lock), '--settings', str(self.settings),
                    '--output', str(snap_out),
                ],
            ),
            mock.patch('builtins.print') as print_snap,
        ):
            bundle.main()
        with (
            mock.patch('subprocess.run', side_effect=AssertionError('No compiler invocation')),
            mock.patch('subprocess.Popen', side_effect=AssertionError('No compiler invocation')),
            mock.patch.object(
                sys, 'argv', [
                    'build_native_compiler_bundle.py', '--policy-only',
                    '--snapshot-rootfs', str(snap_out / 'rootfs'),
                    '--manifest', str(self.manifest), '--output', str(policy_out),
                ],
            ),
            mock.patch('builtins.print') as print_policy,
        ):
            bundle.main()
        self.assertEqual(print_snap.call_args.args[0], print_policy.call_args.args[0])
        self.assertEqual(
            json.loads((policy_out / snapshot.POLICY).read_bytes())['release'],
            '5.1.0-rc.1',
        )


class GeneratedSourceSeed(unittest.TestCase):
    def test_offline_seed_matches_the_native_generators_dependencies_and_profile(self):
        # This compares independently maintained build inputs to the actual generator,
        # so changing its dependency features cannot silently leave a stale offline seed.
        repository = SCRIPTS.parents[1]
        source = repository.joinpath('crates/meerschaum-plugin/src/single_file.rs').read_text()
        match = re.search(r'r#"\n(\[package\]\nname = \{name:\?\}.*?)"#', source, re.S)
        self.assertIsNotNone(match, 'Locate the generator manifest before reviewing its seed')
        generated_text = match.group(1).replace('{name:?}', '"mrsm_native_source"')
        generated_text = generated_text.replace('{{', '{').replace('}}', '}')
        generated = tomllib.loads(generated_text)
        seed = tomllib.loads(SCRIPTS.joinpath('compiler_source/Cargo.toml').read_text())
        self.assertEqual(seed['dependencies'], generated['dependencies'])
        self.assertEqual(seed['profile']['release'], generated['profile']['release'])
        self.assertEqual(seed['lib'], generated['lib'])
        for field in ['name', 'version', 'edition']:
            self.assertEqual(seed['package'][field], generated['package'][field])
        lock = tomllib.loads(SCRIPTS.joinpath('compiler_source/Cargo.lock').read_text())
        local = [package for package in lock['package'] if 'source' not in package]
        self.assertEqual([(package['name'], package['version']) for package in local],
                         [('mrsm_native_source', '0.0.0')])


if __name__ == '__main__':
    unittest.main()
