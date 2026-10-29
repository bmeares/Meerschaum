#! /usr/bin/env python
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

"""
`meerschaum.plugins.figure()`: the page-contract chart helper (design §8.3, D19).

The figure JSON, not a library, is the charting contract: a Python plugin and a Rust plugin must
emit the same JSON for the same chart (see `tests/console/` and `tests/plugins/w7_pyfig`,
`tests/plugins/w7_native_fig`). All data is synthetic.
"""

import re
import json
import pathlib
from html.parser import HTMLParser

import pytest
import meerschaum as mrsm
from meerschaum.plugins import figure, figure_json

REPO = pathlib.Path(__file__).resolve().parents[1]

#: The figure both console test plugins emit.
FIGURE = {
    'data': [{'type': 'bar', 'x': ['a', 'b', 'c'], 'y': [3, 1, 2], 'name': 'w7'}],
    'layout': {'title': {'text': 'W7 figure'}},
}


class _Attributes(HTMLParser):
    """Collect the attributes of every start tag, like a browser's `dataset`."""

    def __init__(self):
        super().__init__()
        self.tags = []

    def handle_starttag(self, tag, attrs):
        self.tags.append((tag, dict(attrs)))


def _attributes(fragment: str):
    parser = _Attributes()
    parser.feed(fragment)
    return parser.tags


def _rust_plugin_figure_json() -> str:
    source = (REPO / 'tests' / 'plugins' / 'w7_native_fig' / 'plugin.rs').read_text()
    match = re.search(r'const FIGURE_JSON: &str = r#"(.*?)"#;', source)
    assert match, "The Rust plugin no longer declares FIGURE_JSON."
    return match.group(1)


def test_figure_json_is_byte_identical_to_the_rust_plugin():
    rust = _rust_plugin_figure_json()
    assert figure_json(FIGURE) == rust
    assert figure_json(json.dumps(FIGURE, indent=2)) == rust


def test_figure_json_matches_the_python_console_plugin():
    namespace = {}
    path = REPO / 'tests' / 'plugins' / 'w7_pyfig' / '__init__.py'
    exec(compile(path.read_text(), str(path), 'exec'), namespace)
    assert figure_json(namespace['FIGURE']) == namespace['FIGURE_JSON']
    assert figure_json(FIGURE) == namespace['FIGURE_JSON']


def test_figure_fragment_carries_the_json_in_an_attribute():
    fragment = figure(FIGURE)
    assert fragment.startswith('<div ') and fragment.endswith('></div>')
    ((tag, attrs),) = _attributes(fragment)
    assert tag == 'div'
    ### What the console's JS reads back (`el.dataset.mrsmFigure`).
    assert attrs['data-mrsm-figure'] == _rust_plugin_figure_json()


def test_figure_options():
    ((_, attrs),) = _attributes(figure(FIGURE, id='counts', aria_label='Counts "by" site'))
    assert attrs['id'] == 'counts'
    assert attrs['role'] == 'img'
    assert attrs['aria-label'] == 'Counts "by" site'
    assert json.loads(attrs['data-mrsm-figure']) == FIGURE


@pytest.mark.parametrize('text', [
    "O'Brien & <Sons> \"quoted\"",
    "</div><script>alert(1)</script>",
    "emoji ☃ and 'single' quotes",
])
def test_figure_escapes_attribute_breakers(text):
    fig = {'data': [{'type': 'bar', 'x': [text], 'y': [1]}], 'layout': {'title': {'text': text}}}
    fragment = figure(fig)
    ((tag, attrs),) = _attributes(fragment)
    assert '<script>' not in fragment
    assert json.loads(attrs['data-mrsm-figure']) == fig


def test_plotly_figure_object():
    go = mrsm.attempt_import('plotly.graph_objects')
    fig = go.Figure(
        data=[go.Bar(x=['a', 'b', 'c'], y=[3, 1, 2], name='w7')],
        layout={'title': {'text': 'W7 figure'}},
    )
    document = json.loads(figure_json(fig))
    ### plotly.py adds its default template; strip it to compare with the plugin figure.
    document['layout'].pop('template', None)
    assert document == FIGURE
    ((_, attrs),) = _attributes(figure(fig))
    assert json.loads(attrs['data-mrsm-figure']) == json.loads(figure_json(fig))


def test_figure_dict_with_numpy_values():
    np = mrsm.attempt_import('numpy')
    fig = {'data': [{'type': 'bar', 'x': ['a', 'b'], 'y': np.array([1, 2])}], 'layout': {}}
    assert json.loads(figure_json(fig))['data'][0]['y'] == [1, 2]


def test_figure_is_deterministic_and_compact():
    assert figure_json(FIGURE) == figure_json(dict(FIGURE))
    assert ' ' not in figure_json({'data': [], 'layout': {}})


@pytest.mark.parametrize('bad', [42, None, [1, 2], '[1, 2]'])
def test_figure_rejects_non_figures(bad):
    with pytest.raises((TypeError, ValueError)):
        figure_json(bad)
