#! /usr/bin/env python3
# vim:fenc=utf-8

"""
Test miscellaneous utilities.
"""

from typing import Dict, Any

import pytest


@pytest.mark.parametrize(
    "doc_str,doc",
    [
        ('', {}),
        ('a:1', {'a': 1}),
        ('a:"1"', {'a': '1'}),
        ('a:foo', {'a': 'foo'}),
        ('a:1,b:2,c:3.3', {'a': 1, 'b': 2, 'c': 3.3}),
        ('a:1,b:["foo","bar"]', {'a': 1, 'b': ['foo', 'bar']}),
        ('a:{"b":{"c":[1,2,3]}},d:[456],e:"7"', {'a': {'b': {'c': [1, 2, 3]}}, 'd': [456], 'e': '7'}),
    ]
)
def test_string_to_dict(doc_str: str, doc: Dict[str, Any]):
    """
    Test that `string_to_dict()` function correctly parses the simple-dict input.
    """
    from meerschaum.utils.misc import string_to_dict
    parsed_doc = string_to_dict(doc_str)
    assert parsed_doc == doc


@pytest.mark.parametrize(
    "doc_str,doc",
    [
        ('', {}),
        ('a:1', {'a': 1}),
        ('a:"1"', {'a': '1'}),
        ('a:foo', {'a': 'foo'}),
        ('a:1,b:2,c:3.3', {'a': 1, 'b': 2, 'c': 3.3}),
        ('a:1,b:["foo","bar"]', {'a': 1, 'b': ['foo', 'bar']}),
        ('a:{"b":{"c":[1,2,3]}},d:[456],e:"7"', {'a': {'b': {'c': [1, 2, 3]}}, 'd': [456], 'e': '7'}),
    ]
)
def test_to_simple_dict(doc_str: str, doc: Dict[str, Any]):
    """
    Test that `to_simple_dict()` correctly serializes the input dictionary.
    """
    from meerschaum.utils.misc import to_simple_dict
    parsed_doc_str = to_simple_dict(doc)
    assert parsed_doc_str == doc_str
