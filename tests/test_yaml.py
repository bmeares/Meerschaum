"""
Wave 13 (task G): the native YAML codec (`meerschaum._core.yaml`) and its Python façade
(`meerschaum.utils.yaml`).

Three layers:

- **Recorded parity.** `crates/meerschaum-core/tests/fixtures/yaml_parity.json` holds what
  released `meerschaum==4.1.4` (PyYAML) does with the corpus in `yaml_corpus.py`: typed `load` /
  `safe_load` results or exception class and message, the exact text `dump` writes for every
  keyword set, and the outcomes of `read_config` / `write_config` on throwaway config
  directories. These tests need no PyYAML.
- **Live differential** (when PyYAML is importable): the native codec against PyYAML in the same
  process, including what a fixture cannot express (object identity of aliases, file objects and
  their error messages, bytes/UTF-16 input).
- **No-PyYAML subprocess**: a config write and read round trip with `yaml` blocked and every
  install request refused.
"""

import datetime
import importlib.util
import json
import os
import pathlib
import site
import subprocess
import sys
import textwrap

import pytest

ROOT = pathlib.Path(__file__).resolve().parents[1]
FIXTURES = ROOT / 'crates' / 'meerschaum-core' / 'tests' / 'fixtures'
sys.path.insert(0, str(FIXTURES))
import yaml_corpus as corpus  # noqa: E402
from yaml_corpus import tag, untag, outcome  # noqa: E402

try:
    from meerschaum import _core
    NATIVE = _core.load() is not None and hasattr(_core.load(), 'yaml')
except Exception:
    NATIVE = False

pytestmark = pytest.mark.skipif(not NATIVE, reason='requires the native extension')

HAS_PYYAML = importlib.util.find_spec('yaml') is not None


def _wrapper():
    from meerschaum.utils.yaml import yaml, _native
    assert _native is not None
    return yaml


@pytest.fixture(scope='module')
def parity():
    return json.loads((FIXTURES / 'yaml_parity.json').read_text(encoding='utf-8'))


# Cases where the released `yaml.load` (PyYAML's full `Loader`) builds arbitrary Python objects
# and the native codec refuses them with the `SafeLoader` error (PARITY.md, Wave 13).
PYTHON_TAG_DEVIATIONS = {
    'python_object_tag', 'python_apply_tag', 'python_name_tag', 'python_module_tag',
    'python_str_tag', 'python_complex_tag', 'python_dict_tag', 'python_list_tag',
}


# ---------------------------------------------------------------------------------------------
# Recorded parity
# ---------------------------------------------------------------------------------------------

def test_recorded_reference_versions(parity):
    assert parity['reference']['meerschaum'] == '4.1.4'
    assert len(parity['load_cases']) > 200
    assert len(parity['dump_cases']) > 3000


@pytest.mark.parametrize('label', ['load', 'safe_load'])
def test_load_cases_match_released_pyyaml(parity, label):
    yaml = _wrapper()
    function = getattr(yaml, label)
    failures = []
    for case in parity['load_cases']:
        actual = outcome(function, case['text'])
        expected = case[label]
        if label == 'load' and case['name'] in PYTHON_TAG_DEVIATIONS:
            assert actual.get('error') == 'ConstructorError', case['name']
            assert actual['message'].startswith('could not determine a constructor for the tag')
            continue
        if actual != expected:
            failures.append((case['name'], expected, actual))
    assert not failures, failures[:3]


def test_dump_cases_match_released_pyyaml(parity):
    yaml = _wrapper()
    failures = []
    for case in parity['dump_cases']:
        data = untag(case['data'])
        try:
            actual = {'ok': yaml.dump(data, **case['kwargs'])}
        except Exception as e:
            actual = {'error': type(e).__name__, 'message': str(e)}
        if actual != case['expected']:
            failures.append((case['name'], case['expected'], actual))
    assert not failures, failures[:3]


# ---------------------------------------------------------------------------------------------
# What a typed JSON fixture cannot express
# ---------------------------------------------------------------------------------------------

def test_python_types_are_real_python_objects():
    yaml = _wrapper()
    value = yaml.safe_load(
        'day: 2024-01-01\n'
        'stamp: 2024-01-01 12:30:00.123456\n'
        'aware: 2024-01-01T12:30:00+05:30\n'
        'utc: 2024-01-01T12:30:00Z\n'
        'raw: !!binary aGVsbG8=\n'
        'big: 123456789012345678901234567890\n'
        'octal: 017\n'
        'flag: yes\n'
        'nothing: ~\n'
        'ratio: 1.5\n'
        'inf: .inf\n'
    )
    assert type(value['day']) is datetime.date and value['day'] == datetime.date(2024, 1, 1)
    assert type(value['stamp']) is datetime.datetime
    assert value['stamp'] == datetime.datetime(2024, 1, 1, 12, 30, 0, 123456)
    assert value['stamp'].tzinfo is None
    assert value['aware'].utcoffset() == datetime.timedelta(hours=5, minutes=30)
    assert value['utc'].tzinfo is datetime.timezone.utc
    assert value['raw'] == b'hello' and type(value['raw']) is bytes
    assert value['big'] == 123456789012345678901234567890 and type(value['big']) is int
    assert value['octal'] == 15
    assert value['flag'] is True and value['nothing'] is None
    assert value['ratio'] == 1.5 and value['inf'] == float('inf')


def test_python_dict_key_semantics():
    yaml = _wrapper()
    assert yaml.safe_load('1: a\ntrue: b\n') == {1: 'b'}
    assert list(yaml.safe_load('1: a\ntrue: b\n').keys()) == [1] and \
        type(list(yaml.safe_load('1: a\ntrue: b\n'))[0]) is int
    assert yaml.safe_load('true: a\n1: b\n') == {True: 'b'}
    assert type(next(iter(yaml.safe_load('true: a\n1: b\n')))) is bool
    assert yaml.safe_load('1: a\n1.0: b\n') == {1: 'b'}
    assert yaml.safe_load('"1": a\n1: b\n') == {'1': 'a', 1: 'b'}
    assert yaml.safe_load('2024-01-01: a\n') == {datetime.date(2024, 1, 1): 'a'}
    assert yaml.safe_load('~: a\n') == {None: 'a'}
    assert yaml.safe_load('? !!binary aGk=\n: v\n') == {b'hi': 'v'}
    assert yaml.safe_load('a: 1\na: 2\n') == {'a': 2}


def test_aliases_share_python_objects():
    yaml = _wrapper()
    value = yaml.safe_load('a: &x [1, 2]\nb: *x\nc: &y {k: v}\nd: *y\ne: &z 2024-01-01\nf: *z\n')
    assert value['a'] is value['b']
    assert value['c'] is value['d']
    assert value['e'] is value['f']
    value['a'].append(3)
    assert value['b'] == [1, 2, 3]


def test_merge_keys_use_shared_values():
    yaml = _wrapper()
    value = yaml.safe_load('base: &b {x: [1]}\nchild:\n  <<: *b\n  y: 2\n')
    assert value['child']['x'] is value['base']['x']
    assert list(value['child']) == ['x', 'y']


def test_tuple_tag_is_loaded_by_load_only():
    yaml = _wrapper()
    assert yaml.load('a: !!python/tuple [1, 2]\n') == {'a': (1, 2)}
    with pytest.raises(Exception) as excinfo:
        yaml.safe_load('a: !!python/tuple [1, 2]\n')
    assert type(excinfo.value).__name__ == 'ConstructorError'
    assert yaml.dump({'a': (1, 2)}, sort_keys=False) == 'a: !!python/tuple\n- 1\n- 2\n'


@pytest.mark.parametrize('text', [
    'a: !!python/object/apply:builtins.int ["5"]\n',
    'a: !!python/name:os.system\n',
    'a: !!python/object:builtins.object {}\n',
    'a: !!python/module:os\n',
])
def test_unsafe_python_tags_are_refused_by_load(text):
    """The decision: `!!python/object*`, `name`, `module` (and every other `!!python/*` tag
    except `tuple`) raise the `SafeLoader` error instead of importing and calling things."""
    yaml = _wrapper()
    for function in (yaml.load, yaml.safe_load):
        with pytest.raises(Exception) as excinfo:
            function(text)
        assert type(excinfo.value).__name__ == 'ConstructorError'
        assert 'could not determine a constructor for the tag' in str(excinfo.value)


def test_exception_hierarchy():
    from meerschaum._core import _core as extension
    native = extension.yaml
    assert issubclass(native.ScannerError, native.MarkedYAMLError)
    assert issubclass(native.ParserError, native.MarkedYAMLError)
    assert issubclass(native.ComposerError, native.MarkedYAMLError)
    assert issubclass(native.ConstructorError, native.MarkedYAMLError)
    assert issubclass(native.MarkedYAMLError, native.YAMLError)
    assert issubclass(native.ReaderError, native.YAMLError)
    assert issubclass(native.YAMLError, Exception)
    yaml = _wrapper()
    with pytest.raises(native.YAMLError):
        yaml.load('a: b: c')
    with pytest.raises(native.ScannerError):
        yaml.load('a: b: c')
    with pytest.raises(native.ComposerError):
        yaml.load('a: 1\n---\nb: 2\n')


def test_file_objects_and_error_names(tmp_path):
    yaml = _wrapper()
    path = tmp_path / 'ok.yaml'
    path.write_text('a: 1\nb: [x, y]\n', encoding='utf-8')
    with open(path, 'r', encoding='utf-8') as f:
        assert yaml.load(f) == {'a': 1, 'b': ['x', 'y']}
    bad = tmp_path / 'bad.yaml'
    bad.write_text('a: b: c\n', encoding='utf-8')
    with open(bad, 'r', encoding='utf-8') as f:
        with pytest.raises(Exception) as excinfo:
            yaml.load(f)
    message = str(excinfo.value)
    # PyYAML names the file and quotes no snippet for file objects.
    assert f'in "{bad}", line 1, column 5' in message
    assert '^' not in message
    with pytest.raises(Exception) as excinfo:
        yaml.load('a: b: c\n')
    assert 'in "<unicode string>", line 1, column 5:\n    a: b: c\n        ^' in str(excinfo.value)


def test_bytes_and_utf16_input():
    yaml = _wrapper()
    assert yaml.load('a: é\n'.encode('utf-8')) == {'a': 'é'}
    assert yaml.load('\ufeffa: 1\n'.encode('utf-16-le')) == {'a': 1}
    assert yaml.load('\ufeffa: 1\n'.encode('utf-16-be')) == {'a': 1}
    with pytest.raises(Exception) as excinfo:
        yaml.load(b'a: \xff\n')
    assert type(excinfo.value).__name__ == 'ReaderError'
    assert "'utf-8' codec can't decode byte #xff: invalid start byte" in str(excinfo.value)
    assert 'in "<byte string>", position 3' in str(excinfo.value)


def test_dump_streams_and_return_values(tmp_path):
    import io
    yaml = _wrapper()
    buffer = io.StringIO()
    assert yaml.dump({'a': 1}, stream=buffer, sort_keys=False) is None
    assert buffer.getvalue() == 'a: 1\n'
    path = tmp_path / 'out.yaml'
    with open(path, 'w', encoding='utf-8') as f:
        assert yaml.dump({'a': 1, 'b': 'é'}, f, sort_keys=False) is None
    assert path.read_text(encoding='utf-8') == 'a: 1\n\nb: "\\xE9"\n'
    assert yaml.dump({'a': 1}, encoding='utf-8') == b'a: 1\n'
    assert yaml.dump({'a': 'é'}, allow_unicode=True, sort_keys=False) == 'a: é\n'


def test_dump_unknown_keywords_are_ignored():
    yaml = _wrapper()
    assert yaml.dump({'a': 1}, sort_keys=False, nonsense=True) == 'a: 1\n'


def test_dump_shares_containers_as_anchors():
    yaml = _wrapper()
    shared = [1, 2]
    assert yaml.dump({'a': shared, 'b': shared}, sort_keys=False) == \
        'a: &id001\n- 1\n- 2\n\nb: *id001\n'
    when = datetime.date(2024, 1, 1)
    assert yaml.dump({'a': when, 'b': when}, sort_keys=False) == \
        'a: &id001 2024-01-01\n\nb: *id001\n'
    # Equal but distinct objects are not shared.
    assert yaml.dump({'a': [1], 'b': [1]}, sort_keys=False) == 'a:\n- 1\n\nb:\n- 1\n'


def test_dump_dict_subclasses_and_mappings():
    import collections
    import types
    yaml = _wrapper()
    assert yaml.dump(collections.OrderedDict(a=1, b=2), sort_keys=False) == 'a: 1\n\nb: 2\n'
    assert yaml.dump(types.MappingProxyType({'a': 1}), sort_keys=False) == 'a: 1\n'

    class D(dict):
        pass

    assert yaml.dump(D(a=D(b=1)), sort_keys=False) == 'a:\n  b: 1\n'


def test_dump_refuses_unrepresentable_objects():
    yaml = _wrapper()
    with pytest.raises(Exception) as excinfo:
        yaml.dump({'a': object()})
    assert type(excinfo.value).__name__ == 'RepresenterError'
    with pytest.raises(Exception) as excinfo:
        yaml.dump({'a': {1, frozenset()}})
    assert type(excinfo.value).__name__ == 'RepresenterError'


def test_dump_recursive_structures_are_an_error_not_a_crash():
    yaml = _wrapper()
    loop = []
    loop.append(loop)
    with pytest.raises(Exception) as excinfo:
        yaml.dump(loop)
    assert type(excinfo.value).__name__ == 'RepresenterError'


def test_deeply_nested_input_raises_recursion_error():
    yaml = _wrapper()
    with pytest.raises(RecursionError):
        yaml.load('[' * 5000 + ']' * 5000)
    nested = []
    for _ in range(5000):
        nested = [nested]
    with pytest.raises(RecursionError):
        yaml.dump(nested)


def test_lone_surrogate_escape_deviation():
    """PARITY: Python keeps a lone surrogate; a Rust string cannot, so it becomes U+FFFD."""
    yaml = _wrapper()
    assert yaml.safe_load('a: "\\ud800"\n') == {'a': '\ufffd'}


def test_load_without_data_returns_none():
    yaml = _wrapper()
    assert yaml.load('') is None
    assert yaml.safe_load('# only a comment\n') is None


# ---------------------------------------------------------------------------------------------
# Live differential against PyYAML
# ---------------------------------------------------------------------------------------------

@pytest.mark.skipif(not HAS_PYYAML, reason='PyYAML is not installed')
def test_native_matches_pyyaml_on_the_load_corpus():
    import yaml as pyyaml
    from meerschaum._core import _core as extension
    native = extension.yaml
    mismatches = []
    for name, text in corpus.LOAD_CASES:
        if name in PYTHON_TAG_DEVIATIONS or name == 'alias_recursive_seq':
            continue
        for label, reference, candidate in (
            ('load', lambda t: pyyaml.load(t, Loader=pyyaml.Loader), native.load),
            ('safe_load', pyyaml.safe_load, native.safe_load),
        ):
            expected = outcome(reference, text)
            actual = outcome(candidate, text)
            if expected != actual:
                mismatches.append((name, label, expected, actual))
    assert not mismatches, mismatches[:3]


@pytest.mark.skipif(not HAS_PYYAML, reason='PyYAML is not installed')
def test_native_dump_matches_pyyaml_dumper_with_meerschaum_customizations():
    import yaml as pyyaml
    from meerschaum._core import _core as extension
    native = extension.yaml

    def presenter(dumper, data):
        kw = {}
        if len(data.splitlines()) > 1:
            kw['style'] = '|'
        return dumper.represent_scalar('tag:yaml.org,2002:str', data, **kw)

    class CustomDumper(pyyaml.Dumper):
        def write_line_break(self, data=None):
            if len(self.indents) == 1:
                super().write_line_break(data)
            super().write_line_break(data)

    CustomDumper.add_representer(str, presenter)
    data = {
        'b': [1, 2.5, None, True, {'x': (1, 2), 'y': {3, 4}}, 'multi\nline', 'yes', ''],
        'a': {'when': datetime.date(2024, 1, 1), 'at': datetime.datetime(2024, 1, 1, 1, 2, 3),
              'raw': b'\x00\x01', 'big': 10 ** 25, 'nan': float('nan'), 'inf': float('-inf')},
        1: 'int key', ('t', 1): 'tuple key',
    }
    for kwargs in ({}, {'sort_keys': False}, {'default_flow_style': None}, {'width': 20}):
        expected = pyyaml.dump(data, Dumper=CustomDumper, **kwargs)
        assert native.dump(data, **kwargs) == expected


def _by_key(spec):
    """A tagged value with every mapping's entries in a canonical order (`sort_keys` reorders)."""
    if spec['t'] == 'dict':
        entries = [[_by_key(k), _by_key(v)] for k, v in spec['v']]
        return {'t': 'dict', 'v': sorted(entries, key=json.dumps)}
    if spec['t'] in ('list', 'tuple'):
        return {'t': spec['t'], 'v': [_by_key(i) for i in spec['v']]}
    return spec


@pytest.mark.skipif(not HAS_PYYAML, reason='PyYAML is not installed')
def test_pyyaml_reads_what_native_writes():
    import yaml as pyyaml
    yaml = _wrapper()
    checked = 0
    for case in corpus.DUMP_CASES:
        data = untag(case['data'])
        ### A forced `default_style` re-types the scalar on purpose (`|` makes `true` a string), and
        ### v4's `CustomDumper` doubles the line breaks of a *root* scalar (the blank line it
        ### inserts between top-level keys), which no loader reads back: the reference writes
        ### the same text (`dump_cases` pins it byte for byte), so only mappings, sequences
        ### and single-line root scalars round-trip.
        if 'default_style' in case['kwargs']:
            continue
        if isinstance(data, str) and data and (
            data != data.strip() or len(data.splitlines()) != 1 or len(data) > 60
        ):
            continue
        text = yaml.dump(data, **case['kwargs'])
        got, want = tag(pyyaml.load(text, Loader=pyyaml.Loader)), tag(data)
        if case['kwargs'].get('sort_keys', True):
            got, want = _by_key(got), _by_key(want)
        assert got == want, case['name']
        checked += 1
    assert checked > 1000


# ---------------------------------------------------------------------------------------------
# The config files: read_config / write_config, replayed in a fresh root
# ---------------------------------------------------------------------------------------------

_CONFIG_RUNNER = textwrap.dedent('''
    import json, os, pathlib, shutil, sys
    sys.path.insert(0, sys.argv[1])
    import yaml_corpus as corpus
    from meerschaum.config import paths, get_config
    ### The generator's reference process had loaded (and written) the formatting config while it
    ### imported its helpers; do the same so the cases only see their own files.
    get_config('formatting', 'ansi')
    out = {}
    for case in corpus.CONFIG_CASES:
        directory = pathlib.Path(paths.CONFIG_DIR_PATH)
        shutil.rmtree(directory, ignore_errors=True)
        directory.mkdir(parents=True)
        out[case['name']] = corpus.run_config_case(case, directory)
    print(json.dumps(out))
''')


def _run_config_cases(tmp_path, mode):
    root = tmp_path / 'root'
    plugins = tmp_path / 'plugins'
    root.mkdir()
    plugins.mkdir()
    env = {k: v for k, v in os.environ.items() if not k.startswith('MRSM_')}
    env.update({
        'MRSM_ROOT_DIR': str(root),
        'MRSM_PLUGINS_DIR': str(plugins),
        'MRSM_CORE': mode,
        'MRSM_NO_AUTO_INSTALL': '1',
        'PYTHONPATH': str(ROOT),
        'HOME': str(tmp_path),
        ### A throwaway HOME must not hide the user site (`warn()` needs its terminal packages).
        'PYTHONUSERBASE': site.getuserbase(),
    })
    result = subprocess.run(
        [sys.executable, '-c', _CONFIG_RUNNER, str(FIXTURES)],
        env=env, capture_output=True, text=True, timeout=180, stdin=subprocess.DEVNULL,
    )
    assert result.returncode == 0, result.stdout + result.stderr
    return json.loads(result.stdout.strip().splitlines()[-1])


def _unwrapped(outcome, root=None):
    """
    The outcome with whitespace removed from stderr. The runner scrubs the root before `warn()`'s
    line wrapping is undone, so a long (xdist) temporary root split across lines survives the
    scrub: replace it again once the text is unwrapped.
    """
    stderr = ''.join(outcome['stderr'].split())
    if root is not None:
        stderr = stderr.replace(str(root), '{ROOT}')
    return {**outcome, 'stderr': stderr}


def _without_v5_defaults(spec):
    """
    A tagged value in which the JSON text of the stack's `MRSM_CONFIG` default is parsed and
    stripped of the keys v5 adds to the default config (`system:engine`), which v4.1.4 lacks.
    """
    if isinstance(spec, list):
        return [_without_v5_defaults(i) for i in spec]
    if not isinstance(spec, dict):
        return spec
    if spec.get('t') == 'str' and spec['v'].lstrip().startswith('{\n    "meerschaum"'):
        try:
            parsed = json.loads(spec['v'])
        except ValueError:
            return spec
        parsed.get('system', {}).pop('engine', None)
        return {'t': 'str', 'json': parsed}
    return {k: _without_v5_defaults(v) for k, v in spec.items()}


@pytest.mark.parametrize('mode', ['python', 'rust'])
def test_config_cases_match_released_reference(parity, tmp_path, mode):
    actual = _run_config_cases(tmp_path, mode)
    failures = []
    for case in parity['config_cases']:
        expected = case['outcome']
        got = actual[case['name']]
        ### `warn()` wraps long lines at the terminal width, and the temporary root's path length
        ### decides where: compare the warnings without whitespace.
        expected, got = _unwrapped(expected), _unwrapped(got, tmp_path / 'root')
        if mode == 'rust':
            ### PARITY (wave 13, PARITY.md): the Rust default config carries `system:engine`.
            expected, got = _without_v5_defaults(expected), _without_v5_defaults(got)
        if got != expected:
            failures.append((case['name'], {
                key: (expected[key], got[key]) for key in expected if expected[key] != got[key]
            }))
    assert not failures, failures[:3]


# ---------------------------------------------------------------------------------------------
# No PyYAML, no network, no installer
# ---------------------------------------------------------------------------------------------

_NO_PYYAML = textwrap.dedent('''
    import sys, datetime
    sys.modules['yaml'] = None            # `import yaml` raises ImportError
    sys.modules['ruamel'] = None
    import meerschaum as mrsm
    from meerschaum.utils import packages

    def forbidden(*args, **kwargs):
        raise AssertionError('install attempt: %r' % (args,))

    packages.pip_install = forbidden
    packages.attempt_import = forbidden
    from meerschaum.config import read_config, write_config
    import meerschaum.config.paths as paths

    data = {
        'filetype': 'yaml', 'value': 1, 'text': 'on', 'nested': {'enabled': True},
        'when': datetime.date(2024, 1, 1), 'keys': {1: 'a', 'b': 2},
    }
    assert write_config({'smoke_probe': data})
    text = (paths.CONFIG_DIR_PATH / 'smoke_probe.yaml').read_text()
    assert 'when: 2024-01-01' in text and "text: 'on'" in text, text
    back = read_config(keys=['smoke_probe'], substitute=False)['smoke_probe']
    assert back == data, back
    assert mrsm.get_config('meerschaum', 'instance') == 'sql:main'
    from meerschaum.utils.yaml import yaml, _native
    assert _native is not None
    assert yaml.load(text) == data
    assert 'packaging' not in sys.modules and 'packaging.version' not in sys.modules
    print('ok')
''')


@pytest.mark.parametrize('mode', ['python', 'rust'])
def test_config_round_trip_without_pyyaml_or_installs(tmp_path, mode):
    root = tmp_path / 'root'
    plugins = tmp_path / 'plugins'
    root.mkdir()
    plugins.mkdir()
    env = {k: v for k, v in os.environ.items() if not k.startswith('MRSM_')}
    env.update({
        'MRSM_ROOT_DIR': str(root),
        'MRSM_PLUGINS_DIR': str(plugins),
        'MRSM_CORE': mode,
        'MRSM_NO_AUTO_INSTALL': '1',
        'PIP_NO_INDEX': '1',
        'PYTHONPATH': str(ROOT),
        'HOME': str(tmp_path),
    })
    result = subprocess.run(
        [sys.executable, '-c', _NO_PYYAML],
        env=env, capture_output=True, text=True, timeout=120, stdin=subprocess.DEVNULL,
    )
    assert result.returncode == 0, result.stdout + result.stderr
    assert result.stdout.strip().endswith('ok')
