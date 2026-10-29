"""Website output changes punctuation without changing runnable examples or API source."""
from importlib.util import module_from_spec, spec_from_file_location
from pathlib import Path

SPEC = spec_from_file_location(
    'normalize_generated_docs', Path(__file__).resolve().parents[1] / 'scripts/normalize_generated_docs.py',
)
GEN = module_from_spec(SPEC)
SPEC.loader.exec_module(GEN)


def test_normalizes_literal_and_entity_prose_while_preserving_markup():
    source = '<p title="literal—attribute">A — B &mdash; C &#8212; D &#x2014; E.</p>'
    expected = '<p title="literal—attribute">A, B, C, D, E.</p>'
    assert GEN.normalize_prose(source) == expected
    assert GEN.normalize_prose(expected) == expected


def test_preserves_code_javascript_styles_and_comments_exactly():
    source = '''<!doctype html>
<!-- — untouched -->
<style>.x:after { content: '—'; }</style>
<script>const data = {"value":"—"};</script>
<pre><code><span>source — literal &mdash; &#8212;</span></code></pre>
<p>Read <code>pipe — source</code> — then sync.</p>
<textarea>— &mdash;</textarea>
'''
    expected = source.replace('</code> — then sync.', '</code>, then sync.')
    assert GEN.normalize_prose(source) == expected
