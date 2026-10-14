"""
Wave 9 (task Q): the `_core.get_config` read-only fast path (PARITY row 12).

Every scenario runs in `python` mode and in `rust` mode in one process (the gate is re-read with
`set_mode`) against the same live `meerschaum.config.config` dictionary, and the observed values
must match. The scenarios are the mutation paths a cache could miss: callers editing returned
dicts, `set_config`, `replace_config`, `MRSM{}` symlinks added after a key was loaded, and
`write_config` followed by a reload.
"""

import copy

import pytest

import meerschaum.config as mrsm_config
from meerschaum._core import _gate

try:
    from meerschaum import _core
    HAS_CORE = _core.load() is not None
except Exception:
    HAS_CORE = False

pytestmark = pytest.mark.skipif(not HAS_CORE, reason="the Rust extension is not built")


@pytest.fixture(autouse=True)
def isolated_config():
    """Restore the gate and the in-memory config dictionary after each test."""
    original_mode = (_gate._MODE, _gate._FEATURES)
    mrsm_config.get_config('system')
    mrsm_config.get_config('meerschaum')
    original = copy.deepcopy(mrsm_config.config)
    yield
    _gate._MODE, _gate._FEATURES = original_mode
    mrsm_config.set_config(original)


def run_both(scenario):
    """Run `scenario()` in python mode then rust mode, restoring the config in between."""
    start = copy.deepcopy(mrsm_config.config)
    _gate.set_mode('python')
    expected = scenario()
    mrsm_config.set_config(copy.deepcopy(start))
    _gate.set_mode('rust')
    actual = scenario()
    return expected, actual


def test_returned_dict_is_live():
    def scenario():
        from meerschaum.config import get_config
        sql = get_config('system', 'connectors', 'sql')
        same = sql is mrsm_config.config['system']['connectors']['sql']
        sql['chunksize'] = 4321
        first = get_config('system', 'connectors', 'sql', 'chunksize')
        mrsm_config.config['system']['connectors']['sql']['chunksize'] = 1234
        second = get_config('system', 'connectors', 'sql', 'chunksize')
        return same, first, second

    expected, actual = run_both(scenario)
    assert expected == (True, 4321, 1234)
    assert actual == expected


def test_set_config_is_seen_immediately():
    def scenario():
        from meerschaum.config import get_config, set_config
        before = get_config('pipes', 'verify', 'max_chunks_syncs')
        cf = copy.deepcopy(mrsm_config.config)
        cf['pipes']['verify']['max_chunks_syncs'] = 99
        set_config(cf)
        after = get_config('pipes', 'verify', 'max_chunks_syncs')
        return before, after

    expected, actual = run_both(scenario)
    assert expected[1] == 99
    assert actual == expected


def test_replace_config_is_seen_and_restored():
    def scenario():
        from meerschaum.config import get_config, replace_config
        outside = get_config('meerschaum', 'instance')
        patched = copy.deepcopy(mrsm_config.config)
        patched['meerschaum']['instance'] = 'sql:w9q_replaced'
        with replace_config(patched):
            inside = get_config('meerschaum', 'instance')
        restored = get_config('meerschaum', 'instance')
        return outside, inside, restored

    expected, actual = run_both(scenario)
    assert expected[1] == 'sql:w9q_replaced'
    assert expected[0] == expected[2]
    assert actual == expected


def test_symlink_added_after_load_is_substituted():
    def scenario():
        from meerschaum.config import get_config
        instance = get_config('meerschaum', 'instance')
        mrsm_config.config['system']['w9q_link'] = 'MRSM{meerschaum:instance}'
        linked = get_config('system', 'w9q_link')
        again = get_config('system', 'w9q_link')
        other = get_config('system', 'connectors', 'sql', 'chunksize')
        raw = get_config('system', 'w9q_link', substitute=False)
        return instance, linked, again, other, raw

    expected, actual = run_both(scenario)
    assert expected[1] == expected[0]
    assert actual == expected


def test_substitute_false_returns_raw_symlink():
    def scenario():
        from meerschaum.config import get_config
        get_config('system')
        mrsm_config.config['system']['w9q_raw'] = 'MRSM{meerschaum:instance}'
        raw = get_config('system', 'w9q_raw', substitute=False)
        subbed = get_config('system', 'w9q_raw')
        return raw, subbed

    expected, actual = run_both(scenario)
    assert expected[0] == 'MRSM{meerschaum:instance}'
    assert actual == expected


def test_invalid_and_default_keys():
    def scenario():
        from meerschaum.config import get_config
        missing = get_config('system', 'w9q_does_not_exist', warn=False, as_tuple=True)
        mrsm_config.config['system'].pop('fetch_pipes_keys', None)
        default = get_config('system', 'fetch_pipes_keys', as_tuple=True, write_missing=False)
        valid = get_config('system', 'connectors', 'sql', 'chunksize', as_tuple=True)
        return missing, default[0], valid

    expected, actual = run_both(scenario)
    assert expected[0] == (False, None)
    assert actual == expected


def test_write_config_then_reload(tmp_path):
    def scenario():
        from meerschaum.config import get_config, write_config
        pipes_cf = get_config('pipes')
        pipes_cf['verify']['w9q_written'] = 7
        assert write_config({'pipes': pipes_cf}, directory=tmp_path)
        live = get_config('pipes', 'verify', 'w9q_written')
        from meerschaum.config._read_config import read_config
        on_disk = read_config(directory=tmp_path, keys=['pipes'])['pipes']['verify']['w9q_written']
        mrsm_config.config.pop('pipes')
        mrsm_config.config['pipes'] = read_config(directory=tmp_path, keys=['pipes'])['pipes']
        reloaded = get_config('pipes', 'verify', 'w9q_written')
        return live, on_disk, reloaded

    expected, actual = run_both(scenario)
    assert expected == (7, 7, 7)
    assert actual == expected


def test_fast_path_values_match_python_for_loaded_keys():
    keys_list = [
        ('system', 'connectors', 'sql', 'chunksize'),
        ('pipes', 'attributes', 'local_cache_timeout_seconds'),
        ('meerschaum', 'instance'),
        ('system', 'experimental'),
        ('pipes', 'dtypes', 'columns_types_cache_seconds'),
        ('meerschaum', 'connectors', 'sql'),
    ]

    def scenario():
        from meerschaum.config import get_config
        return [copy.deepcopy(get_config(*keys)) for keys in keys_list]

    expected, actual = run_both(scenario)
    assert actual == expected
