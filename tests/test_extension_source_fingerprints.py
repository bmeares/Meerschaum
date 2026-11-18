"""The fixed confined inspector consumes native-computed typed source identities."""

import copy
import pytest

from meerschaum.worker._plugin_registry import _source_fingerprints


def test_native_fingerprint_map_preserves_host_files_without_fabricated_archive():
    catalog = {
        'placement': {'kind': 'host', 'team_id': None,
                      'generation': '00000000-0000-0000-0000-000000000001',
                      'files': {'probe.py': 'a' * 64}, 'view_sha256': 'b' * 64},
        'plugins': {'probe': {'kind': 'python_module'}},
    }
    fingerprints = {'probe': {'kind': 'files', 'tree_sha256': 'c' * 64}}
    assert _source_fingerprints(catalog, fingerprints) == fingerprints
    assert 'archive_sha256' not in fingerprints['probe']
    for wrong in (
        {},
        {**fingerprints, 'other': {'kind': 'files', 'tree_sha256': 'c' * 64}},
        {'probe': {'kind': 'archive', 'source_kind': 'python_module', 'archive_sha256': 'c' * 64}},
        {'probe': {'kind': 'files', 'tree_sha256': 'c' * 64, 'archive_sha256': 'c' * 64}},
        {'probe': {'kind': 'files', 'tree_sha256': '../outside'}},
    ):
        with pytest.raises(PermissionError):
            _source_fingerprints(catalog, wrong)
    forged = copy.deepcopy(catalog)
    forged['plugins']['probe']['archive_sha256'] = 'c' * 64
    with pytest.raises(PermissionError):
        _source_fingerprints(forged, fingerprints)


def test_executor_archive_fingerprint_requires_exact_source_kind_and_content():
    catalog = {'plugins': {'probe': {'kind': 'python_module', 'archive_sha256': 'a' * 64}}}
    fingerprints = {'probe': {'kind': 'archive', 'source_kind': 'python_module', 'archive_sha256': 'a' * 64}}
    assert _source_fingerprints(catalog, fingerprints) == fingerprints
    for wrong in (
        {'probe': {'kind': 'files', 'tree_sha256': 'a' * 64}},
        {'probe': {'kind': 'archive', 'source_kind': 'native_source', 'archive_sha256': 'a' * 64}},
        {'probe': {'kind': 'archive', 'source_kind': 'python_module', 'archive_sha256': 'b' * 64}},
        {'probe': {**fingerprints['probe'], 'credential': 'forbidden'}},
    ):
        with pytest.raises(PermissionError):
            _source_fingerprints(catalog, wrong)
