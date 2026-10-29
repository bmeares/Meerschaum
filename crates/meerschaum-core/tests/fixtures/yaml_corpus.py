# vim:fenc=utf-8

"""
The synthetic corpus of the YAML codec parity fixture (`yaml_parity.json`).

Pure data plus the typed projection helpers: no `meerschaum` import at module level, so the
generator (`gen_yaml.py`, run with the released reference through `scripts/parity/run.sh`), the
Rust tests and `tests/test_yaml.py` all read the same cases.

The typed projection is `{"t": "int", "v": "12"}`, `{"t": "dict", "v": [[key, value], ...]}`, ...;
keep `tag` / `untag` in step with `crates/meerschaum-core/src/yaml/node.rs::tagged_json`.
"""

import datetime
import json

# ---------------------------------------------------------------------------------------------
# The typed projection
# ---------------------------------------------------------------------------------------------

class Untaggable(Exception):
    """A constructed value outside the typed projection (an arbitrary Python object)."""


def tag(obj):
    if obj is None:
        return {'t': 'none'}
    if isinstance(obj, bool):
        return {'t': 'bool', 'v': obj}
    if isinstance(obj, int):
        return {'t': 'int', 'v': str(obj)}
    if isinstance(obj, float):
        return {'t': 'float', 'v': repr(obj)}
    if isinstance(obj, str):
        return {'t': 'str', 'v': obj}
    if isinstance(obj, bytes):
        return {'t': 'bytes', 'v': obj.hex()}
    if isinstance(obj, datetime.datetime):
        return {'t': 'datetime', 'v': obj.isoformat('T')}
    if isinstance(obj, datetime.date):
        return {'t': 'date', 'v': obj.isoformat()}
    if isinstance(obj, list):
        return {'t': 'list', 'v': [tag(i) for i in obj]}
    if isinstance(obj, tuple):
        return {'t': 'tuple', 'v': [tag(i) for i in obj]}
    if isinstance(obj, dict):
        return {'t': 'dict', 'v': [[tag(k), tag(v)] for k, v in obj.items()]}
    if isinstance(obj, (set, frozenset)):
        return {'t': 'set', 'v': sorted((tag(i) for i in obj), key=lambda j: json.dumps(j))}
    raise Untaggable(type(obj).__name__)


def untag(spec, shared=None):
    """The inverse of `tag`, with `shared` ids reused by identity."""
    if shared is None:
        shared = {}
    t = spec['t']
    if t == 'shared':
        if 'v' in spec:
            shared[spec['id']] = untag(spec['v'], shared)
        return shared[spec['id']]
    if t == 'none':
        return None
    if t == 'bool':
        return spec['v']
    if t == 'int':
        return int(spec['v'])
    if t == 'float':
        return float(spec['v'])
    if t == 'str':
        return spec['v']
    if t == 'bytes':
        return bytes.fromhex(spec['v'])
    if t == 'datetime':
        return datetime.datetime.fromisoformat(spec['v'])
    if t == 'date':
        return datetime.date.fromisoformat(spec['v'])
    if t == 'list':
        return [untag(i, shared) for i in spec['v']]
    if t == 'tuple':
        return tuple(untag(i, shared) for i in spec['v'])
    if t == 'dict':
        return {untag(k, shared): untag(v, shared) for k, v in spec['v']}
    if t == 'set':
        return {untag(i, shared) for i in spec['v']}
    raise ValueError(spec)


def outcome(function, *args, **kwargs):
    """`{'ok': tagged result}` or `{'error': class name, 'message': str(error)}`."""
    try:
        return {'ok': tag(function(*args, **kwargs))}
    except Untaggable as e:
        return {'untaggable': str(e)}
    except Exception as e:
        return {'error': type(e).__name__, 'message': str(e)}


# ---------------------------------------------------------------------------------------------
# Load cases
# ---------------------------------------------------------------------------------------------

LOAD_CASES = [
    # --- implicit typing, YAML 1.1 ---
    ('ints', 'a: 0\nb: 12\nc: -12\nd: +12\ne: 1_000\nf: 017\ng: 0o17\nh: 08\ni: 0x1F\nj: 0b101\nk: -0\nl: 0_\nm: 00\nn: +0x10\n'),
    ('bigints', 'a: 12345678901234567890123456789\nb: -99999999999999999999999\nc: 0xFFFFFFFFFFFFFFFFFFFF\nd: 0b' + '1' * 70 + '\ne: 0777777777777777777777777\n'),
    ('base60', 'a: 190:20:30\nb: 1:30\nc: -1:30\nd: 190:20:30.15\ne: 1:30.5\nf: 0:30\ng: 1:60\nh: 12:34:56:78\n'),
    ('floats', 'a: 1.5\nb: .5\nc: 5.\nd: 1e5\ne: 1.0e+5\nf: 1.0e5\ng: -1.5e-3\nh: +.5\ni: 1_0.5\nj: 1.\nk: -.5\nl: 6.02e+23\nm: 1.e+3\n'),
    ('infnan', 'a: .inf\nb: -.inf\nc: +.inf\nd: .Inf\ne: .INF\nf: .nan\ng: .NaN\nh: .NAN\ni: inf\nj: nan\nk: Infinity\n'),
    ('bools', 'a: yes\nb: No\nc: ON\nd: off\ne: true\nf: False\ng: y\nh: n\ni: Y\nj: T\nk: yEs\nl: TRUE\n'),
    ('nulls', 'a: ~\nb: null\nc: Null\nd: NULL\ne:\nf: \ng: nil\nh: None\ni: NuLL\n'),
    ('quoted_lookalikes', 'a: "yes"\nb: \'on\'\nc: "1"\nd: \'2024-01-01\'\ne: "null"\nf: "~"\ng: \'1.5\'\nh: "0x1F"\n'),
    ('string_lookalikes', 'a: 1e5\nb: 0.1.2\nc: 1,000\nd: +\ne: -\nf: 12abc\ng: 0x\nh: 0b2\ni: 1:70\nj: 2024-1\nk: v1.2\n'),
    ('dates', 'a: 2024-01-01\nb: 2024-1-1\nc: 0001-01-01\nd: 9999-12-31\ne: 2024-02-29\n'),
    ('datetimes', 'a: 2024-01-01T12:30:00\nb: 2024-01-01 12:30:00\nc: 2024-01-01t12:30:00\nd: 2024-01-01 12:30:00.5\ne: 2024-01-01T12:30:00.123456\nf: 2024-01-01T12:30:00.123456789\ng: 2024-01-01T12:30:00.\nh: 2024-1-1T1:30:00\ni: 2024-01-01   12:30:00\n'),
    ('datetime_offsets', 'a: 2024-01-01T12:30:00Z\nb: 2024-01-01T12:30:00+05:30\nc: 2024-01-01T12:30:00-08:00\nd: 2024-01-01T12:30:00+5\ne: 2024-01-01 12:30:00 +0\nf: 2024-01-01T12:30:00.25 Z\ng: 2024-01-01T12:30:00+00:00\nh: 2024-01-01T12:30:00 -3:30\n'),
    ('invalid_dates', 'a: 2024-13-45\n'),
    ('invalid_date_feb30', 'a: 2024-02-30\n'),
    ('invalid_year_zero', 'a: 0000-01-01\n'),
    ('invalid_hour', 'a: 2024-01-01T24:00:00\n'),
    ('invalid_tz', 'a: 2024-01-01T12:30:00+99:99\n'),
    ('timestamp_tagged', 'a: !!timestamp 2024-01-01\nb: !!timestamp "2024-01-01 10:00:00"\n'),
    ('timestamp_tagged_bad', 'a: !!timestamp hello\n'),
    ('binary', 'a: !!binary aGVsbG8=\nb: !!binary |\n  aGVsbG8gd29y\n  bGQ=\nc: !!binary ""\n'),
    ('binary_bad', 'a: !!binary a\n'),
    ('binary_padding', 'a: !!binary aGVsbG8\n'),
    ('binary_non_ascii', 'a: !!binary "é"\n'),
    ('set', 'a: !!set {x, y, z}\nb: !!set\n  ? 1\n  ? 2\n'),
    ('omap', 'a: !!omap\n  - x: 1\n  - y: 2\n'),
    ('pairs', 'a: !!pairs\n  - x: 1\n  - x: 2\n'),
    ('omap_bad', 'a: !!omap\n  - x: 1\n    y: 2\n'),
    ('omap_scalar_item', 'a: !!omap\n  - x\n'),
    ('str_tags', 'a: !!str 12\nb: !!str true\nc: !!str\nd: !!str ~\ne: ! 12\nf: !!str 2024-01-01\n'),
    ('int_tags', 'a: !!int "12"\nb: !!int 0x1F\nc: !!float 1\nd: !!float "1e3"\ne: !!bool yes\nf: !!null ""\ng: !!null anything\n'),
    ('bool_tag_bad', 'a: !!bool maybe\n'),
    ('int_tag_bad', 'a: !!int abc\n'),
    ('float_tag_bad', 'a: !!float abc\n'),
    ('int_tag_empty', 'a: !!int\n'),
    ('seq_tag_on_mapping', 'a: !!seq {x: 1}\n'),
    ('map_tag_on_scalar', 'a: !!map foo\n'),
    ('str_tag_on_seq', 'a: !!str [1, 2]\n'),
    ('map_tag_on_seq', 'a: !!map [1, 2]\n'),
    ('seq_tag_on_scalar', 'a: !!seq foo\n'),
    ('set_tag_on_scalar', 'a: !!set foo\n'),
    ('unknown_tag', 'a: !foo bar\n'),
    ('unknown_tag_seq', 'a: !foo [1]\n'),
    ('unknown_tag_map', 'a: !foo {x: 1}\n'),
    ('verbatim_tag', 'a: !<tag:yaml.org,2002:int> "12"\n'),
    ('local_tag_handle', '%TAG !e! tag:example.com,2000:\n---\na: !e!foo bar\n'),
    ('undefined_tag_handle', 'a: !e!foo bar\n'),
    ('tuple_tag', 'a: !!python/tuple [1, 2]\n'),
    ('tuple_tag_nested', 'a: !!python/tuple\n- 1\n- !!python/tuple [2, 3]\n'),
    ('tuple_key', '? !!python/tuple [1, 2]\n: v\n'),
    ('python_object_tag', 'a: !!python/object:builtins.object {}\n'),
    ('python_apply_tag', 'a: !!python/object/apply:builtins.int ["5"]\n'),
    ('python_name_tag', 'a: !!python/name:os.system\n'),
    ('python_module_tag', 'a: !!python/module:os\n'),
    ('python_str_tag', 'a: !!python/str hello\n'),
    ('python_complex_tag', 'a: !!python/complex 1+2j\n'),
    ('python_dict_tag', 'a: !!python/dict {x: 1}\n'),
    ('python_list_tag', 'a: !!python/list [1]\n'),
    ('merge_tag_value', 'a: <<\n'),
    ('merge_tag_in_seq', '- <<\n'),
    ('value_tag_value', 'a: =\n'),
    ('value_tag_key', '=: 1\n'),
    # --- merge keys and aliases ---
    ('merge_basic', 'base: &b {x: 1, y: 2}\nchild:\n  <<: *b\n  z: 3\n'),
    ('merge_override', 'base: &b {x: 1, y: 2}\nchild:\n  <<: *b\n  y: 20\n'),
    ('merge_own_first', 'base: &b {x: 1, y: 2}\nchild:\n  y: 20\n  <<: *b\n'),
    ('merge_list', 'a: &a {x: 1, y: 2}\nb: &b {y: 20, z: 30}\nc:\n  <<: [*a, *b]\n  w: 4\n'),
    ('merge_list_order', 'a: &a {k: first}\nb: &b {k: second}\nc:\n  <<: [*b, *a]\n'),
    ('merge_nested', 'a: &a {x: 1}\nb: &b\n  <<: *a\n  y: 2\nc:\n  <<: *b\n'),
    ('merge_inline_flow', 'a: &a {x: 1}\nb: {<<: *a, y: 2}\n'),
    ('merge_scalar_value', 'a:\n  <<: 5\n'),
    ('merge_seq_of_scalars', 'a:\n  <<: [1, 2]\n'),
    ('merge_seq_with_scalar', 'x: &x {a: 1}\na:\n  <<: [*x, 2]\n'),
    ('merge_quoted_key', 'x: &x {a: 1}\na:\n  "<<": *x\n'),
    ('merge_duplicate', 'x: &x {a: 1}\ny: &y {b: 2}\nc:\n  <<: *x\n  <<: *y\n'),
    ('merge_key_order', 'x: &x {b: 1, a: 2}\nc:\n  z: 0\n  <<: *x\n  a: 9\n'),
    ('alias_scalar', 'a: &x hello\nb: *x\nc: [*x, *x]\n'),
    ('alias_collection', 'a: &x [1, 2]\nb: *x\nc: &y {k: v}\nd: *y\n'),
    ('alias_key', '&k key: 1\nother: *k\n'),
    ('alias_undefined', 'a: *nope\n'),
    ('alias_duplicate_anchor', 'a: &x 1\nb: &x 2\n'),
    ('alias_recursive_seq', 'a: &x [*x]\n'),
    ('anchor_empty_value', 'a: &x\nb: *x\n'),
    ('anchor_on_tag', 'a: &x !!str 12\nb: *x\n'),
    ('tag_then_anchor', 'a: !!str &x 12\nb: *x\n'),
    ('anchor_names', 'a: &a-b_1 1\nb: *a-b_1\n'),
    ('anchor_bad_char', 'a: &x.y 1\n'),
    ('alias_bad', 'a: *\n'),
    ('anchor_bad', 'a: &\n'),
    # --- keys ---
    ('key_types', '1: a\ntrue: b\nnull: c\n2.5: d\n2024-01-01: e\n"1": f\n~: g\n'),
    ('key_collision_bool_int', '1: a\ntrue: b\n'),
    ('key_collision_int_bool', 'true: a\n1: b\n'),
    ('key_collision_int_float', '1: a\n1.0: b\n'),
    ('key_collision_yes_true', 'yes: a\ntrue: b\non: c\n'),
    ('key_collision_nulls', 'null: a\n~: b\n"": c\n'),
    ('key_str_vs_int', '"1": a\n1: b\n'),
    ('key_duplicate', 'a: 1\nb: 2\na: 3\n'),
    ('key_date_datetime_distinct', '2024-01-01: a\n2024-01-01T00:00:00: b\n'),
    ('key_unhashable_seq', '? [a, b]\n: c\n'),
    ('key_unhashable_map', '? {a: b}\n: c\n'),
    ('key_complex', '? a\n: b\n? c\n:\n'),
    ('key_empty', ': value\n'),
    ('key_empty_flow', '{: a, b: }\n'),
    ('key_large_int', '123456789012345678901234567890: a\n'),
    ('key_nan', '.nan: a\n.nan: b\n'),
    ('key_inf', '.inf: a\n-.inf: b\n'),
    ('key_negative_zero', '0: a\n-0: b\n0.0: c\n'),
    ('key_quoted_special', '"a: b": 1\n\'c #d\': 2\n"e\\nf": 3\n'),
    ('key_set', '? !!set {a}\n: b\n'),
    ('key_binary', '? !!binary aGk=\n: v\n'),
    # --- collections and flow syntax ---
    ('empty_collections', 'a: {}\nb: []\nc:\nd: ""\n'),
    ('flow_nested', 'a: [1, [2, 3], {x: [4, 5]}, "s"]\n'),
    ('flow_trailing_comma', 'a: [1, 2,]\nb: {x: 1,}\n'),
    ('flow_single_pair', 'a: [x: 1, y: 2]\n'),
    ('flow_single_pair_empty', 'a: [? x, ? y : z]\n'),
    ('flow_multiline', 'a: [1,\n  2,\n  3]\nb: {x: 1,\n  y: 2}\n'),
    ('flow_plain_with_spaces', 'a: [hello world, foo bar]\nb: {k v: x y}\n'),
    ('flow_missing_comma', 'a: [1 2]\n'),
    ('flow_unclosed_seq', 'a: [1, 2\n'),
    ('flow_unclosed_map', 'a: {x: 1\n'),
    ('flow_extra_close', 'a: 1]\n'),
    ('flow_colon_no_space', 'a: {x:1}\n'),
    ('flow_url', 'a: [http://example.com, b]\n'),
    ('block_seq_nested', '- - 1\n  - 2\n- - 3\n- - - 4\n'),
    ('block_seq_of_maps', '- a: 1\n  b: 2\n- c: 3\n-\n  d: 4\n'),
    ('indentless_seq', 'a:\n- 1\n- 2\nb:\n  - 3\n'),
    ('seq_empty_entries', '- \n- a\n-\n'),
    ('map_empty_values', 'a:\nb:\nc: x\n'),
    ('nested_maps', 'a:\n  b:\n    c:\n      d: 1\n    e: 2\n  f: 3\n'),
    ('deep_nesting', ''.join('  ' * i + 'k%d:\n' % i for i in range(18)) + '  ' * 18 + 'leaf: 1\n'),
    ('deep_flow_nesting', 'a: ' + '[' * 20 + ']' * 20 + '\n'),
    ('only_scalar', 'hello\n'),
    ('only_int', '42\n'),
    ('only_list', '- a\n- b\n'),
    ('empty_document', ''),
    ('only_comment', '# nothing here\n'),
    ('only_whitespace', '   \n\n  \n'),
    ('only_doc_marker', '---\n'),
    ('doc_marker_scalar', '--- hello\n'),
    ('doc_end_marker', 'a: 1\n...\n'),
    ('doc_end_then_comment', 'a: 1\n...\n# trailing\n'),
    ('multiple_docs', 'a: 1\n---\nb: 2\n'),
    ('multiple_docs_empty', '---\n---\n'),
    ('docs_with_end', 'a: 1\n...\n---\nb: 2\n'),
    ('three_docs', '1\n---\n2\n---\n3\n'),
    # --- scalars: plain, quoted, block ---
    ('plain_multiline', 'a: hello\n  world\n  again\nb: x\n'),
    ('plain_with_hash', 'a: foo #comment\nb: foo#notcomment\nc: "x # y"\n'),
    ('plain_with_colon', 'a: http://x.y:80/z\nb: a:b\nc: "d: e"\n'),
    ('plain_special_starts', 'a: -x\nb: ?x\nc: :x\nd: -\n'),
    ('plain_url_like', 'a: C:\\path\\to\nb: ~/home\nc: @user\n'),
    ('at_sign', 'a: @foo\n'),
    ('backtick', 'a: `foo\n'),
    ('percent_mid', 'a: %foo\n'),
    ('percent_doc', '%FOO bar\n---\na: 1\n'),
    ('single_quoted', "a: 'it''s'\nb: 'multi\n  line'\nc: ''\nd: '  spaced  '\ne: 'a\n\n  b'\n"),
    ('double_quoted_escapes', 'a: "tab\\there"\nb: "nl\\nthere"\nc: "\\x41\\u00e9\\U0001F600"\nd: "q\\"q"\ne: "bs\\\\"\nf: "\\0\\a\\b\\e\\v\\f\\r"\ng: "\\N\\_\\L\\P"\nh: "slash\\/"\ni: "sp\\ ace"\n'),
    ('double_quoted_line_continuation', 'a: "one \\\n  two"\nb: "x\n  y"\nc: "x\n\n  y"\n'),
    ('double_quoted_bad_escape', 'a: "\\q"\n'),
    ('double_quoted_short_hex', 'a: "\\x4"\n'),
    ('double_quoted_too_big', 'a: "\\U00110000"\n'),
    ('unterminated_double', 'a: "unterminated\n'),
    ('unterminated_single', "a: 'unterminated\n"),
    ('quoted_doc_separator', 'a: "x\n---\ny"\n'),
    ('literal_block', 'a: |\n  line1\n  line2\nb: |-\n  strip\nc: |+\n  keep\n\nd: x\n'),
    ('literal_indent_indicator', 'a: |2\n    two extra\n  base\nb: x\n'),
    ('literal_leading_blank', 'a: |\n\n  after blank\n'),
    ('literal_empty', 'a: |\nb: x\n'),
    ('literal_trailing_blank_lines', 'a: |\n  x\n\n\nb: y\n'),
    ('folded_block', 'a: >\n  folded\n  text\n\n  para\nb: >-\n  strip\nc: >+\n  keep\n\nd: x\n'),
    ('folded_more_indented', 'a: >\n  normal\n    indented\n  normal\n'),
    ('block_bad_indicator', 'a: |0\n  x\n'),
    ('block_bad_header', 'a: | x\n  y\n'),
    ('block_header_comment', 'a: | # comment\n  text\n'),
    ('block_in_seq', '- |\n  one\n- >\n  two\n'),
    ('block_at_root', '|\n  text\n'),
    ('block_tab_content', 'a: |\n  x\ty\n  \tz\n'),
    ('unicode_strings', 'a: héllo\nb: 日本語\nc: "emoji 😀"\nd: ünïcödé: x\n'),
    ('unicode_keys', 'ключ: значение\n日本: 語\n'),
    ('bom', '\ufeffa: 1\n'),
    ('crlf', 'a: 1\r\nb:\r\n  - x\r\n  - y\r\nc: |\r\n  l1\r\n  l2\r\n'),
    ('cr_only', 'a: 1\rb: 2\r'),
    ('line_sep_chars', 'a: x\u2028y\nb: "p\u2029q"\n'),
    ('nel_char', 'a: x\x85y\n'),
    ('trailing_spaces', 'a: 1   \nb: x  \n   \n'),
    ('comments_everywhere', '# top\na: 1 # trailing\n# between\nb:\n  # inside\n  - x # item\n  # end\n'),
    ('comment_in_flow', 'a: [1, # one\n  2]\n'),
    ('windows_path_quoted', 'a: "C:\\\\Users\\\\x"\n'),
    ('long_string', 'a: ' + 'word ' * 60 + '\n'),
    ('long_key', ('k' * 100) + ': 1\n'),
    ('very_long_simple_key', ('k' * 1100) + ': 1\n'),
    # --- directives ---
    ('yaml_directive', '%YAML 1.1\n---\na: 1\n'),
    ('yaml_directive_12', '%YAML 1.2\n---\na: 1\n'),
    ('yaml_directive_major2', '%YAML 2.0\n---\na: 1\n'),
    ('yaml_directive_dup', '%YAML 1.1\n%YAML 1.1\n---\na: 1\n'),
    ('yaml_directive_bad', '%YAML x\n---\na: 1\n'),
    ('tag_directive_dup', '%TAG !a! tag:x:\n%TAG !a! tag:y:\n---\na: 1\n'),
    ('tag_directive_secondary', '%TAG !! tag:example.com,2000:\n---\na: !!foo 1\n'),
    ('directive_no_doc', '%YAML 1.1\na: 1\n'),
    # --- malformed ---
    ('mapping_values_not_allowed', 'a: b: c\n'),
    ('bad_indent', 'a:\n  b: 1\n c: 2\n'),
    ('bad_indent_seq', '- a\n - b\n'),
    ('seq_then_map', '- a\nb: c\n'),
    ('map_then_seq', 'a: 1\n- b\n'),
    ('tab_indent', 'a:\n\tb: 1\n'),
    ('tab_after_colon', 'a:\tb\n'),
    ('tab_before_value', 'a: \tb\n'),
    ('tab_inside_value', 'a: x\ty\n'),
    ('leading_tab_line', '\ta: 1\n'),
    ('tab_in_flow', 'a: [1,\t2]\n'),
    ('colon_no_space', 'key:value\n'),
    ('colon_eof', 'key:'),
    ('dash_no_space', '-a\n- b\n'),
    ('stray_close_bracket', ']\n'),
    ('stray_close_brace', '}\n'),
    ('stray_comma', ',\n'),
    ('invalid_start_char', '@\n'),
    ('invalid_start_backtick', '`\n'),
    ('bare_question', '?\n'),
    ('question_value', '? a\n'),
    ('key_without_colon', 'a: 1\nb\n'),
    ('simple_key_multiline', 'a\nb: 1\n'),
    ('seq_not_allowed_here', 'a: - b\n'),
    ('seq_after_key_same_line', 'a: b\n- c\n'),
    ('anchor_without_node', '&a\n'),
    ('tag_only', '!!str\n'),
    ('tag_bad', '!\n'),
    ('tag_bad_uri', 'a: !<foo\n'),
    ('tag_bad_escape', 'a: !foo%zz bar\n'),
    ('unclosed_flow_doc', '[a, b\n---\nc\n'),
    ('doc_marker_inside', 'a: 1\n--- b\n'),
    ('control_char_nul', 'a: \x00\n'),
    ('control_char_bel', 'a: \x07\n'),
    ('control_char_esc', 'a: \x1b[0m\n'),
    ('control_char_del', 'a: \x7f\n'),
    ('control_char_c1', 'a: \x80\n'),
    ('noncharacter_fffe', 'a: \ufffe\n'),
    ('bom_inside', 'a: 1\n\ufeffb: 2\n'),
    ('windows_1252_mojibake', 'a: caf\u00e9\n'),
    ('colon_in_flow_key', '{a:b: c}\n'),
    ('number_key_colon', '1: 2: 3\n'),
    ('nested_bad_seq', 'a:\n  - b\n  c: d\n'),
    ('map_in_seq_bad_indent', '- a: 1\n b: 2\n'),
    ('quoted_key_no_value', '"a"\n'),
    ('complex_key_no_value', '? - a\n  - b\n: c\n'),
    ('anchor_before_seq_entry', '- &a x\n- *a\n'),
    ('simple_key_over_1024', 'a: ' + 'x' * 10 + '\n' + 'k' * 1025 + ': v\n'),
]

# Config-shaped YAML Meerschaum actually reads (a sample of real-world layouts).
LOAD_CASES += [
    ('config_connectors', 'meerschaum:\n  instance: sql:main\n  connectors:\n    sql:\n      main:\n        flavor: sqlite\n        database: /tmp/x.db\n      other: MRSM{meerschaum:connectors:sql:main}\n'),
    ('config_pipe_parameters', 'columns:\n  datetime: dt\n  id: id\nfetch:\n  backtrack_minutes: 1440\n  definition: |-\n    SELECT *\n    FROM t\n    WHERE x = \'y\'\ndtypes:\n  dt: datetime\n  v: "numeric"\ntags: [a, b]\nupsert: true\nverify:\n  chunk_minutes: 43200\n'),
    ('config_header_comment', '\n#####################\n#  Edit credentials  #\n#####################\n\nmeerschaum:\n  instance: sql:main\n'),
    ('config_with_dates', 'schedule:\n  start: 2024-01-01\n  at: 2024-01-01T08:00:00Z\n  days: [mon, tue]\n'),
    ('config_numeric_strings', 'ports:\n  - 5432\n  - "5432"\n  - 0555\n  - 5e3\n  - 1_000\nflags: {a: yes, b: no, c: on}\n'),
    ('config_env_like', 'env:\n  PATH: /usr/bin:/bin\n  EMPTY: ""\n  NUM: "123"\n  URI: postgresql://u:p@h:5432/db?x=y#frag\n'),
]


# ---------------------------------------------------------------------------------------------
# Dump cases
# ---------------------------------------------------------------------------------------------

def S(v):
    return {'t': 'str', 'v': v}


def I(v):
    return {'t': 'int', 'v': str(v)}


def F(v):
    return {'t': 'float', 'v': repr(float(v))}


def B(v):
    return {'t': 'bool', 'v': v}


NONE = {'t': 'none'}


def L(*items):
    return {'t': 'list', 'v': list(items)}


def T(*items):
    return {'t': 'tuple', 'v': list(items)}


def D(*pairs):
    return {'t': 'dict', 'v': [[k, v] for k, v in pairs]}


def SET(*items):
    return {'t': 'set', 'v': list(items)}


def DATE(iso):
    return {'t': 'date', 'v': iso}


def DT(iso):
    return {'t': 'datetime', 'v': iso}


def BYTES(hexstr):
    return {'t': 'bytes', 'v': hexstr}


def SH(id, v=None):
    spec = {'t': 'shared', 'id': id}
    if v is not None:
        spec['v'] = v
    return spec


def SD(**kw):
    """A dict with string keys."""
    return D(*[(S(k), v) for k, v in kw.items()])


STRINGS = [
    '', ' ', '  ', 'a', 'hello world', 'multi\nline', 'trailing\n', 'two\n\n', '\nleading', 'a\n\nb',
    'x\ny\n', ' leading space', 'trailing space ', 'tab\there', '\t', 'quote\'s', 'dq"x', 'both\'"',
    'colon: here', 'a:b', ':', '::', 'dash - inside', '- item', '-', '--', '---', '--- x', '...',
    '... x', '? q', '?', '?x', '[a]', '[', ']', '{a}', '{', '}', ',', 'a, b', '*a', '&a', '!a', '!',
    '|', '>', '|x', '>x', '%', '%x', '@x', '`x', '#', '# c', 'a #b', 'a# b', 'x #', 'yes', 'No', 'ON',
    'off', 'true', 'False', 'y', 'n', 'null', 'Null', '~', 'NULL', 'nil', '123', '-5', '+5', '1.5',
    '1e5', '1.0e+5', '.5', '.inf', '-.inf', '.nan', '0x1F', '0b101', '017', '1_000', '190:20:30',
    '2024-01-01', '2024-01-01 10:00:00', '2024-01-01T10:00:00Z', '<<', '=', '!!', 'é', 'üñï',
    '日本語', '😀', 'emoji 😀 mix', '\u00a0', '\u2028', 'a\u2028b', 'a\u2029b', '\x85', 'a\x85b',
    '\ufeff', 'x\ufeffy', '\x00', '\x01', '\x07', '\x1b', '\x7f', '\x80', '\x9f', 'bell\x07x',
    'a\rb', 'a\r\nb', '\r', 'win\r\nline\r\n', 'a b  c', 'a  b', ' \n', 'a \nb', 'a\n b', 'a\n  b',
    'a\n\tb', 'x' * 80, 'x' * 81, 'x' * 200, ('word ' * 40).strip(), ('word ' * 40), 'a ' * 41,
    'é' * 100, ('日本 ' * 30).strip(), 'long line ' * 20 + '\nsecond line', 'quoted "x" and \'y\' ' * 6,
    'C:\\path\\to', '\\', 'a\\b', 'http://example.com:80/x?y=1#z', 'postgresql://u:p@h:5432/db',
    'key: value\nother: thing', 'SELECT *\nFROM t\nWHERE x = \'y\'', '  indented\nmore', '\n\n',
    'text\n\n\n', 'a\u2028', '\u2028a', 'x\x0by', 'x\x0cy', 'x\x1cy',
]

DUMP_CASES = []


def add(name, data, **kwargs):
    DUMP_CASES.append({'name': name, 'data': data, 'kwargs': kwargs})


for _i, _s in enumerate(STRINGS):
    add('str_%03d' % _i, S(_s), sort_keys=False)
    add('str_value_%03d' % _i, SD(key=S(_s), other=I(1)), sort_keys=False)
    add('str_in_list_%03d' % _i, L(S(_s), S('x')), sort_keys=False)
    add('str_key_%03d' % _i, D((S(_s), I(1)), (S('b'), I(2))), sort_keys=False)

for _name, _data in [
    ('none', NONE), ('true', B(True)), ('false', B(False)), ('int0', I(0)), ('int_neg', I(-42)),
    ('int_big', I(123456789012345678901234567890)), ('int_big_neg', I(-(10 ** 40))),
    ('float_1', F(1.0)), ('float_01', F(0.1)), ('float_1e16', F(1e16)), ('float_1e17', F(1e17)),
    ('float_1e22', F(1e22)), ('float_1e_5', F(1e-5)), ('float_1e_7', F(1.5e-7)), ('float_inf', F(float('inf'))),
    ('float_ninf', F(float('-inf'))), ('float_nan', F(float('nan'))), ('float_1e300', F(1e300)),
    ('float_neg0', F(-0.0)), ('float_tiny', F(5e-324)), ('float_pi', F(3.141592653589793)),
    ('float_large', F(123456789.123456789)), ('float_round', F(100.0)), ('float_1e15', F(1e15)),
    ('bytes_empty', BYTES('')), ('bytes_hello', BYTES(b'hello'.hex())),
    ('bytes_long', BYTES(bytes(range(256)).hex())), ('bytes_nul', BYTES('00')),
    ('date', DATE('2024-01-01')), ('date_min', DATE('0001-01-01')), ('date_max', DATE('9999-12-31')),
    ('datetime_naive', DT('2024-01-01T12:30:00')), ('datetime_micro', DT('2024-01-01T12:30:00.123456')),
    ('datetime_utc', DT('2024-01-01T12:30:00+00:00')), ('datetime_offset', DT('2024-01-01T12:30:00+05:30')),
    ('datetime_neg_offset', DT('2024-01-01T12:30:00-08:00')),
    ('datetime_offset_micro', DT('2024-01-01T12:30:00.5+05:30')),
    ('empty_list', L()), ('empty_dict', D()), ('empty_tuple', T()), ('empty_set', SET()),
    ('tuple', T(I(1), I(2))), ('tuple_nested', T(I(1), T(I(2), I(3)))), ('set', SET(I(1), I(2), I(3))),
    ('set_str', SET(S('only'))), ('list_of_lists', L(L(I(1), I(2)), L(I(3)), L())),
    ('list_in_list_in_list', L(L(L(I(1))))),
    ('list_of_dicts', L(SD(a=I(1), b=I(2)), SD(c=I(3)), D())),
    ('dict_of_lists', SD(a=L(I(1), I(2)), b=L(), c=L(L(I(1))))),
    ('dict_of_dicts', SD(a=SD(b=SD(c=I(1)), d=I(2)), e=D())),
    ('dict_non_str_keys', D((I(1), S('a')), (B(True), S('b')), (NONE, S('c')), (F(2.5), S('d')),
                            (DATE('2024-01-01'), S('e')), (S('x'), S('f')))),
    ('dict_int_keys', D((I(3), S('c')), (I(1), S('a')), (I(2), S('b')))),
    ('dict_mixed_keys_unsortable', D((I(1), S('a')), (S('b'), S('b')), (NONE, S('c')))),
    ('dict_tuple_keys', D((T(I(2), S('x')), S('b')), (T(I(1), S('y')), S('a')))),
    ('dict_float_int_keys', D((F(2.5), S('a')), (I(1), S('b')), (B(True), S('dup')))),
    ('dict_date_keys', D((DATE('2024-03-01'), S('b')), (DATE('2024-01-01'), S('a')))),
    ('dict_datetime_keys_naive', D((DT('2024-03-01T00:00:00'), S('b')), (DT('2024-01-01T00:00:00'), S('a')))),
    ('dict_datetime_keys_aware', D((DT('2024-03-01T00:00:00+00:00'), S('b')), (DT('2024-01-01T00:00:00+00:00'), S('a')))),
    ('dict_datetime_keys_mixed', D((DT('2024-03-01T00:00:00+00:00'), S('b')), (DT('2024-01-01T00:00:00'), S('a')))),
    ('dict_bytes_keys', D((BYTES('6263'), S('b')), (BYTES('6162'), S('a')))),
    ('dict_none_key_only', D((NONE, S('a')))),
    ('dict_bool_keys', D((B(True), S('t')), (B(False), S('f')))),
    ('dict_unicode_keys', D((S('é'), I(1)), (S('a'), I(2)), (S('z'), I(3)), (S('日'), I(4)))),
    ('dict_upper_lower', D((S('b'), I(1)), (S('B'), I(2)), (S('a'), I(3)), (S('A'), I(4)))),
    ('dict_key_prefixes', D((S('ab'), I(1)), (S('a'), I(2)), (S('abc'), I(3)))),
    ('dict_key_numbers_as_str', D((S('10'), I(1)), (S('9'), I(2)), (S('1'), I(3)))),
    ('dict_long_key', D((S('k' * 129), I(1)), (S('short'), I(2)))),
    ('dict_key_127', D((S('k' * 127), I(1)))),
    ('dict_key_multiline', D((S('a\nb'), I(1)), (S('c'), I(2)))),
    ('dict_key_empty_string', D((S(''), I(1)), (S('a'), I(2)))),
    ('dict_nested_empty', SD(a=SD(b=D(), c=L()), d=NONE)),
    ('list_with_none', L(NONE, NONE, S('x'))),
    ('list_of_empty', L(L(), D(), S(''))),
    ('list_with_multiline', L(S('a\nb'), S('c'))),
    ('list_in_dict_in_list', L(SD(a=L(SD(b=I(1)))))),
    ('dict_in_list_multi_key', L(SD(a=I(1), b=I(2), c=SD(d=I(3), e=L(I(4)))))),
    ('config_like', SD(meerschaum=SD(instance=S('sql:main'), connectors=SD(sql=SD(main=SD(
        flavor=S('sqlite'), database=S('/tmp/x.db'))))), pipes=SD(columns=SD(datetime=S('dt'), id=S('id'))))),
    ('pipe_parameters_like', SD(columns=SD(datetime=S('dt'), id=S('id')), fetch=SD(backtrack_minutes=I(1440),
        definition=S('SELECT *\nFROM t\nWHERE x = \'y\'')), dtypes=SD(dt=S('datetime'), v=S('numeric')),
        tags=L(S('a'), S('b')), upsert=B(True), verify=SD(chunk_minutes=I(43200)))),
    ('symlink_strings', SD(a=S('MRSM{meerschaum:connectors:sql:main}'), b=S('x MRSM{a} y'))),
    ('scalar_root_str', S('abc')),
    ('scalar_root_int', I(5)),
    ('scalar_root_none', NONE),
    ('scalar_root_empty_str', S('')),
    ('scalar_root_multiline', S('a\nb')),
    ('scalar_root_list', L(I(1), I(2))),
    ('scalar_root_nested_list', L(L(I(1)), L(I(2)))),
    ('shared_list', SD(a=SH('x', L(I(1), I(2))), b=SH('x'))),
    ('shared_dict', SD(a=SH('x', SD(k=S('v'))), b=SH('x'), c=SH('x'))),
    ('shared_empty_list', SD(a=SH('x', L()), b=SH('x'))),
    ('shared_empty_dict', SD(a=SH('x', D()), b=SH('x'))),
    ('shared_date', SD(a=SH('x', DATE('2024-01-01')), b=SH('x'))),
    ('shared_datetime', SD(a=SH('x', DT('2024-01-01T10:00:00')), b=SH('x'))),
    ('shared_nested', SD(a=SH('x', L(SH('y', L(I(1))), SH('y'))), b=SH('x'))),
    ('shared_two_anchors', SD(a=SH('x', L(I(1))), b=SH('y', L(I(2))), c=SH('x'), d=SH('y'))),
    ('shared_in_list', L(SH('x', SD(a=I(1))), SH('x'), SH('x'))),
    ('shared_tuple', SD(a=SH('x', T(I(1), I(2))), b=SH('x'))),
    ('shared_empty_tuple', SD(a=SH('x', T()), b=SH('x'))),
    ('shared_set', SD(a=SH('x', SET(I(1))), b=SH('x'))),
    ('shared_dict_key_value', D((S('k'), SH('x', L(I(1)))), (S('j'), SH('x')))),
    ('shared_scalars_are_not_anchored', SD(a=S('same'), b=S('same'), c=I(1), d=I(1))),
    ('shared_root_twice', L(SH('x', SD(a=I(1))), SH('x'))),
    ('shared_date_key_and_value', D((SH('x', DATE('2024-01-01')), SH('x')))),
]:
    add(_name, _data, sort_keys=False)

KWARG_VARIANTS = [
    ('sort_default', {}),
    ('sort_false', {'sort_keys': False}),
    ('flow_none', {'sort_keys': False, 'default_flow_style': None}),
    ('flow_true', {'sort_keys': False, 'default_flow_style': True}),
    ('width_20', {'sort_keys': False, 'width': 20}),
    ('width_40', {'sort_keys': False, 'width': 40}),
    ('indent_4', {'sort_keys': False, 'indent': 4}),
    ('indent_3', {'sort_keys': False, 'indent': 3}),
    ('unicode', {'sort_keys': False, 'allow_unicode': True}),
    ('style_dq', {'sort_keys': False, 'default_style': '"'}),
    ('style_sq', {'sort_keys': False, 'default_style': "'"}),
    ('style_literal', {'sort_keys': False, 'default_style': '|'}),
    ('style_folded', {'sort_keys': False, 'default_style': '>'}),
    ('explicit_start', {'sort_keys': False, 'explicit_start': True}),
    ('explicit_end', {'sort_keys': False, 'explicit_end': True}),
    ('crlf', {'sort_keys': False, 'line_break': '\r\n'}),
    ('cr', {'sort_keys': False, 'line_break': '\r'}),
    ('flow_none_unicode_w30', {'sort_keys': False, 'default_flow_style': None, 'allow_unicode': True, 'width': 30}),
]

_VARIANT_SOURCES = [
    'str_%03d' % i for i in range(0, len(STRINGS), 3)
] + [c['name'] for c in DUMP_CASES if not c['name'].startswith('str_')]
_BY_NAME = {c['name']: c for c in DUMP_CASES}
for _vname, _kw in KWARG_VARIANTS:
    for _src in _VARIANT_SOURCES:
        _base = _BY_NAME[_src]
        add('%s__%s' % (_src, _vname), _base['data'], **_kw)
# Every plain case also with the default (sorting) keywords.
for _c in list(DUMP_CASES):
    if '__' not in _c['name'] and _c['kwargs'].get('sort_keys') is False and not _c['name'].startswith('str_'):
        add(_c['name'] + '__sorted', _c['data'])
    elif not _c['name'].startswith('str_') and '__' not in _c['name'] and _c['kwargs'] == {}:
        pass




# ---------------------------------------------------------------------------------------------
# Config cases: `read_config` / `write_config` on a throwaway directory
# ---------------------------------------------------------------------------------------------

def _read(keys=None, **kw):
    return {'op': 'read', 'keys': keys, 'kw': kw}


def _write(config, **kw):
    return {'op': 'write', 'config': config, 'kw': kw}


CONFIG_CASES = [
    {'name': 'preference_yaml_over_yml_json',
     'setup': {'foo.yaml': 'src: yaml\n', 'foo.yml': 'src: yml\n', 'foo.json': '{"src": "json"}'},
     'steps': [_read(['foo'], write_missing=False, with_filenames=True)]},
    {'name': 'preference_yml_over_json',
     'setup': {'foo.yml': 'src: yml\n', 'foo.json': '{"src": "json"}'},
     'steps': [_read(['foo'], write_missing=False, with_filenames=True)]},
    {'name': 'preference_all_keys',
     'setup': {'foo.yaml': 'src: yaml\n', 'foo.json': '{"src": "json"}', 'bar.json': '{"n": 1}', 'baz.yml': 'm: 2\n'},
     'steps': [_read(None, write_missing=False, with_filenames=True)]},
    {'name': 'json_only', 'setup': {'foo.json': '{"a": 1, "b": [1, 2]}'},
     'steps': [_read(['foo'], write_missing=False, with_filenames=True)]},
    {'name': 'unknown_extension', 'setup': {'foo.txt': 'hello'},
     'steps': [_read(None, write_missing=False, with_filenames=True)]},
    {'name': 'typed_values',
     'setup': {'typed.yaml': 'day: 2024-01-01\nstamp: 2024-01-01T12:30:00Z\nkeys:\n  1: number\n  "1": text\nflag: yes\nquoted: "yes"\nbig: 123456789012345678901234567890\n'},
     'steps': [_read(['typed'], substitute=False, write_missing=False)]},
    {'name': 'substitute_true',
     'setup': {'meerschaum.yaml': 'instance: sql:main\nref: MRSM{meerschaum:instance}\n',
               'other.yaml': 'x: MRSM{meerschaum:instance}\n'},
     'steps': [_read(['other'], write_missing=False)]},
    {'name': 'substitute_false', 'setup': {'other.yaml': 'x: MRSM{meerschaum:instance}\n'},
     'steps': [_read(['other'], substitute=False, write_missing=False)]},
    {'name': 'malformed_preserved', 'setup': {'bad.yaml': 'a: b: c\n'},
     'steps': [_read(['bad'], write_missing=False, with_filenames=True)]},
    {'name': 'malformed_raise', 'setup': {'bad.yaml': 'a: [1, 2\n'},
     'steps': [_read(['bad'], write_missing=False, raise_parsing_errors=True)]},
    {'name': 'malformed_json_preserved', 'setup': {'bad.json': '{"a": '},
     'steps': [_read(['bad'], write_missing=False, with_filenames=True)]},
    {'name': 'empty_file', 'setup': {'empty.yaml': ''},
     'steps': [_read(['empty'], write_missing=False, with_filenames=True)]},
    {'name': 'write_missing_true', 'setup': {},
     'steps': [_read(['stack'], write_missing=True, with_filenames=True)]},
    {'name': 'write_missing_false', 'setup': {},
     'steps': [_read(['stack'], write_missing=False, with_filenames=True)]},
    {'name': 'write_missing_meerschaum_header', 'setup': {},
     'steps': [_read(['meerschaum'], write_missing=True, with_filenames=True)]},
    {'name': 'write_header_and_order', 'setup': {},
     'steps': [_write({'meerschaum': {'instance': 'sql:main', 'zeta': 1, 'alpha': [1, 2]}}),
               _read(['meerschaum'], write_missing=False, substitute=False, with_filenames=True)]},
    {'name': 'write_key_order_kept', 'setup': {},
     'steps': [_write({'plain': {'z': 1, 'a': 2, 'm': {'y': 1, 'b': 2}}}),
               _read(['plain'], write_missing=False, substitute=False, with_filenames=True)]},
    {'name': 'write_multiline_string', 'setup': {},
     'steps': [_write({'plain': {'sql': 'SELECT *\nFROM t', 'x': 'tail\n'}}),
               _read(['plain'], write_missing=False, substitute=False, with_filenames=True)]},
    {'name': 'write_filetype_json', 'setup': {},
     'steps': [_write({'plain': {'filetype': 'json', 'a': 1, 'b': [1, 2]}}),
               _read(['plain'], write_missing=False, substitute=False, with_filenames=True)]},
    {'name': 'write_keeps_existing_yml', 'setup': {'plain.yml': 'old: 1\n'},
     'steps': [_write({'plain': {'new': 2}}),
               _read(['plain'], write_missing=False, substitute=False, with_filenames=True)]},
    {'name': 'write_keeps_existing_json', 'setup': {'plain.json': '{"old": 1}'},
     'steps': [_write({'plain': {'new': 2}}),
               _read(['plain'], write_missing=False, substitute=False, with_filenames=True)]},
    {'name': 'write_leaves_other_keys', 'setup': {'other.yaml': 'keep: me\n'},
     'steps': [_write({'plain': {'new': 2}}),
               _read(['plain'], write_missing=False, substitute=False, with_filenames=True)]},
    {'name': 'write_symlinks_restored', 'setup': {},
     'steps': [_write({'plain': {'host': 'localhost', 'copy': 'localhost'},
                       '_symlinks': {'plain': {'copy': 'MRSM{plain:host}'}}}),
               _read(['plain'], write_missing=False, substitute=False, with_filenames=True)]},
    {'name': 'write_nested_symlinks_restored', 'setup': {},
     'steps': [_write({'plain': {'a': {'b': 1, 'c': 1}, 'd': 2},
                       '_symlinks': {'plain': {'a': {'c': 'MRSM{plain:a:b}'}}}}),
               _read(['plain'], write_missing=False, substitute=False, with_filenames=True)]},
    {'name': 'write_typed_values', 'setup': {},
     'steps': [_write(D((S('plain'), D((S('filetype'), S('yaml')), (S('when'), DATE('2024-01-01')),
                                       (S('at'), DT('2024-01-01T12:30:00+00:00')),
                                       (S('raw'), BYTES('78797a')), (S('tup'), T(I(1), I(2))),
                                       (S('big'), I(10 ** 30))))), typed=True),
               _read(['plain'], write_missing=False, substitute=False, with_filenames=True)]},
    {'name': 'write_unwritable_json_date', 'setup': {},
     'steps': [_write(D((S('plain'), D((S('when'), DATE('2024-01-01'))))), typed=True),
               _read(['plain'], write_missing=False, substitute=False, with_filenames=True)]},
    {'name': 'write_not_allowed', 'setup': {},
     'steps': [_write({'plain': {'a': 1}}, allow_write_missing=False)]},
    {'name': 'keyfile_missing', 'setup': {}, 'steps': [{'op': 'keyfile', 'key': 'nothing'}]},
    {'name': 'keyfile_create_new', 'setup': {}, 'steps': [{'op': 'keyfile', 'key': 'nothing', 'create_new': True}]},
]


def run_config_case(case, directory):
    """
    Run one config case in `directory` (existing, empty). Returns the observable outcome: each
    step's result, the files left behind, captured stdout and warnings. Paths are scrubbed.
    """
    import contextlib
    import io
    import pathlib
    import re
    import warnings
    import meerschaum.config as mcfg
    from meerschaum.config._read_config import read_config, get_keyfile_path
    from meerschaum.config._edit import write_config

    directory = pathlib.Path(directory)
    for fname, text in case['setup'].items():
        (directory / fname).write_text(text, encoding='utf-8')

    results = []
    stdout = io.StringIO()
    stderr = io.StringIO()
    with contextlib.redirect_stdout(stdout), contextlib.redirect_stderr(stderr), \
            warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter('always')
        for step in case['steps']:
            kw = dict(step.get('kw', {}))
            try:
                if step['op'] == 'read':
                    value = read_config(directory=directory, keys=step['keys'], **kw)
                    if isinstance(value, tuple):
                        results.append({'config': tag(value[0]), 'filenames': value[1]})
                    else:
                        results.append({'config': tag(value)})
                elif step['op'] == 'write':
                    config = step['config']
                    if kw.pop('typed', False):
                        config = untag(config)
                    previous = mcfg._allow_write_missing
                    mcfg._allow_write_missing = kw.pop('allow_write_missing', True)
                    try:
                        results.append({'ok': write_config(config, directory)})
                    finally:
                        mcfg._allow_write_missing = previous
                elif step['op'] == 'keyfile':
                    path = get_keyfile_path(
                        step['key'], directory=directory, create_new=step.get('create_new', False)
                    )
                    results.append({'path': None if path is None else path.name})
            except Exception as e:
                results.append({'error': type(e).__name__, 'message': str(e)})
    files = {}
    for path in sorted(directory.iterdir()):
        if path.is_file():
            files[path.name] = path.read_text(encoding='utf-8')
    ansi = re.compile(r'\x1b\[[0-9;]*m')
    root = str(pathlib.Path(mcfg.paths.ROOT_DIR_PATH))
    scrub = lambda text: ansi.sub('', text).replace(root, '{ROOT}')
    results = json.loads(scrub(json.dumps(results)))
    return {
        'results': results,
        'files': {name: scrub(text) for name, text in files.items()},
        'stdout': scrub(stdout.getvalue()),
        'stderr': scrub(stderr.getvalue()),
        'warnings': [scrub(str(w.message)) for w in caught],
    }
