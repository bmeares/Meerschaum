"""Static native Factory manifest tests. Tiny ELF fixtures are never executed."""
import importlib.util
import json
import os
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch


_SOURCE = Path(__file__).resolve().parents[1] / 'scripts/docker/write_native_factory_manifest.py'
_SPEC = importlib.util.spec_from_file_location('native_factory_manifest_test_helper', _SOURCE)
manifest = importlib.util.module_from_spec(_SPEC)
_SPEC.loader.exec_module(manifest)


def elf(machine=62):
    header = bytearray(20)
    header[:6] = b'\x7fELF\x02\x01'
    header[18:20] = machine.to_bytes(2, 'little')
    return bytes(header)


class FactoryManifestTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory(prefix='tpr-factory-manifest-')
        self.root = Path(self.temporary.name)
        self.sdk = '/usr/local/lib/python3.14/site-packages'
        self.put(manifest.CLI, elf())
        self.put(self.sdk + '/meerschaum/__init__.py', b'')
        self.put(self.sdk + '/meerschaum/_core/_native.abi3.so', elf())
        self.put(self.sdk + '/_meerschaum_mrsm.py', b'# fixed SDK shim\n')
        self.freeze()

    def tearDown(self):
        for parent, directories, _ in os.walk(self.root):
            os.chmod(parent, 0o700)
            for name in directories:
                path = Path(parent) / name
                if not path.is_symlink():
                    os.chmod(path, 0o700)
        self.temporary.cleanup()

    def put(self, name, payload):
        path = self.root / name.lstrip('/')
        current = self.root
        os.chmod(current, 0o700)
        for part in path.parent.relative_to(self.root).parts:
            current /= part
            current.mkdir(exist_ok=True)
            os.chmod(current, 0o700)
        if path.exists():
            os.chmod(path, 0o600)
        path.write_bytes(payload)
        os.chmod(path, 0o444)
        return path

    def freeze(self):
        for parent, directories, _ in os.walk(self.root, topdown=False):
            for name in directories:
                path = Path(parent) / name
                if not path.is_symlink():
                    os.chmod(path, 0o555)
            os.chmod(parent, 0o555)

    def build(self, sdk=True):
        with patch('subprocess.run', side_effect=AssertionError('No executable may run.')), \
                patch('subprocess.Popen', side_effect=AssertionError('No executable may run.')):
            return manifest.build(self.root, '5.1.0', 'x86_64', self.sdk if sdk else None,
                                  runtime_uid=os.geteuid())

    def compiler(self):
        files = {'Cargo.lock': b'version = 4\n', 'toolchain/bin/cargo': elf(),
                 'toolchain/bin/rustc': elf() + b'rustc', 'vendor/item/empty': b''}
        description = {'schema': 1, 'files': {name: manifest.hashlib.sha256(value).hexdigest()
                                             for name, value in files.items()}}
        raw = json.dumps(description, sort_keys=True, separators=(',', ':')).encode()
        identity = manifest.hashlib.sha256(raw).hexdigest()
        prefix = manifest.COMPILERS + identity
        for name, value in {**files, 'compiler.json': raw}.items():
            self.put(prefix + '/' + name, value)
        policy = {'schema': 1, 'release': '5.1.0', 'architecture': 'x86_64',
                  'compiler': {'snapshot': prefix, 'manifest_sha256': identity, 'anchor': '/usr/lib'}}
        self.put(manifest.POLICY, json.dumps(policy).encode())
        self.freeze()
        return prefix, identity

    def test_exact_static_sdk_and_native_only_material(self):
        value = json.loads(self.build())
        self.assertEqual(value['cli']['sha256'], manifest.hashlib.sha256(elf()).hexdigest())
        self.assertEqual(value['sdk']['files']['meerschaum/__init__.py']['bytes'], 0)
        self.assertIsNone(value['compiler'])
        self.assertEqual(self.build(), self.build())
        self.assertIsNone(json.loads(self.build(sdk=False))['sdk'])

    def test_compiler_policy_and_every_snapshot_byte_are_bound(self):
        prefix, identity = self.compiler()
        value = json.loads(self.build())
        self.assertEqual(value['compiler']['manifest_sha256'], identity)
        self.assertEqual(value['compiler']['files']['vendor/item/empty']['bytes'], 0)
        self.assertEqual(value['compiler']['policy']['sha256'],
                         manifest.hashlib.sha256((self.root / manifest.POLICY[1:]).read_bytes()).hexdigest())
        self.put(prefix + '/vendor/item/empty', b'changed')
        self.freeze()
        with self.assertRaises(ValueError):
            self.build()

    def test_unlisted_compiler_file_and_missing_sdk_extension_refuse(self):
        prefix, _ = self.compiler()
        self.put(prefix + '/unlisted', b'unknown')
        self.freeze()
        with self.assertRaises(ValueError):
            self.build()
        extra = self.root / prefix[1:] / 'unlisted'
        os.chmod(extra.parent, 0o700)
        extra.unlink()
        native = self.root / self.sdk[1:] / 'meerschaum/_core/_native.abi3.so'
        os.chmod(native.parent, 0o700)
        native.unlink()
        self.freeze()
        with self.assertRaises(ValueError):
            self.build()

    def test_wrong_architecture_wrapper_and_writable_material_refuse(self):
        for payload in [elf(183), b'#!/bin/sh\nexit 0\n']:
            self.put(manifest.CLI, payload)
            self.freeze()
            with self.assertRaises(ValueError):
                self.build()
        cli = self.put(manifest.CLI, elf())
        self.freeze()
        os.chmod(cli, 0o644)
        with self.assertRaises(ValueError):
            self.build()

    def test_present_invalid_policy_and_duplicate_policy_fields_refuse(self):
        for raw in [b'{}', b'{"schema":1,"schema":1}', b'not json']:
            self.put(manifest.POLICY, raw)
            self.freeze()
            with self.assertRaises((ValueError, KeyError, TypeError)):
                self.build()

    def test_input_and_output_links_refuse_without_replacing_existing_output(self):
        cli = self.root / manifest.CLI[1:]
        os.chmod(cli.parent, 0o700)
        cli.unlink()
        cli.symlink_to('/dev/null')
        self.freeze()
        with self.assertRaises((ValueError, OSError)):
            self.build()
        output = self.root / manifest.MANIFEST[1:]
        os.chmod(self.root, 0o700)
        current = self.root
        for part in output.parent.relative_to(self.root).parts:
            current /= part
            if current.exists():
                os.chmod(current, 0o700)
            else:
                current.mkdir()
        output.write_bytes(b'original')
        with self.assertRaises(FileExistsError):
            manifest.write(self.root, b'replacement')
        self.assertEqual(output.read_bytes(), b'original')

    def test_aggregate_bounds_and_noncanonical_paths_refuse(self):
        with patch.object(manifest, 'MAX_FILES', 1):
            with self.assertRaises(ValueError):
                self.build()
        with patch.object(manifest, 'MAX_TOTAL', 19):
            with self.assertRaises(ValueError):
                self.build()
        for path in ['/a//b', '/a/../b', '/a/./b', '/a\\b', '/a\nb', '/a/']:
            with self.assertRaises(ValueError):
                manifest.canonical(path, True)


if __name__ == '__main__':
    unittest.main()
