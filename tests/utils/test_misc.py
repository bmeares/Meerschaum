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

@pytest.mark.parametrize(
    "input_str, expected_output",
    [
        ("HelloWorld!", "hello_world"),
        ("This has spaces in it.", "this_has_spaces_in_it"),
        ("already_in_snake_case", "already_in_snake_case"),
        ("getHTTPResponseCode", "get_http_response_code"),
        ("HTTPStatus", "http_status"),
    ]
)
def test_to_snake_case(input_str: str, expected_output: str):
    """
    Test that `to_snake_case()` correctly converts strings to snake case.
    """
    from meerschaum.utils.misc import to_snake_case
    assert to_snake_case(input_str) == expected_output