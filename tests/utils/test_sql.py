#! /usr/bin/env python3
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

"""
Test SQL utility functions.
"""

from typing import Dict, List, Any
import pytest
from meerschaum.utils.sql import (
    build_where,
    get_pd_type,
)
import meerschaum as mrsm

@pytest.mark.parametrize(
    'params,expected_subqueries',
    [
        (
            {'a': 1},
            ["\"a\" = '1'"]
        ),
        (
            {'a': 1, 'b': 2},
            ["\"a\" = '1'", "\"b\" = '2'"]
        ),
        (
            {'a': [1], 'b': 2},
            ["\"a\" IN ('1')", "\"b\" = '2'"]
        ),
        (
            {'a': [1], 'b': '_2'},
            ["\"a\" IN ('1')", "\"b\" != '2'"]
        ),
        (
            {'a': ['_1'], 'b': '_2'},
            ["\"a\" NOT IN ('1')", "\"b\" != '2'"]
        ),
        (
            {'a': ['_1', 10, '_2', 20], 'b': ['_2', '_3']},
            [
                "\"a\" NOT IN ('1', '2')",
                "\"a\" IN ('10', '20')",
                "\"b\" NOT IN ('2', '3')",
            ]
        ),
    ]
)
def test_build_where(params: Dict[str, Any], expected_subqueries: List[str]):
    """
    Test that build_where() correctly produces the expected query.
    """
    where_subquery = build_where(
        params,
        mrsm.get_connector('sql', 'build_where_test', uri='postgresql://foo:bar@localhost:5432/baz')
    )
    for subquery in expected_subqueries:
        assert subquery in where_subquery


@pytest.mark.parametrize(
    'db_type,pd_type',
    [
        ('TEXT', 'string[pyarrow]'),
        ('DATETIME', 'datetime64[ns]'),
        ('NVARCHAR(2000)', 'string[pyarrow]'),
        ('JSON', 'object'),
        ('DATE', 'datetime64[ns]'),
        ('TIMESTAMP', 'datetime64[ns]'),
        ('BOOL', 'bool[pyarrow]'),
        ('BOOLEAN', 'bool[pyarrow]'),
        ('FLOAT', 'float64[pyarrow]'),
        ('DOUBLE', 'float64[pyarrow]'),
        ('TIMESTAMPTZ', 'datetime64[ns, UTC]'),
        ('TIMESTAMP WITH TIMEZONE', 'datetime64[ns, UTC]'),
        ('CLOB', 'string[pyarrow]'),
        ('NUMBER', 'float64[pyarrow]'),
        ('INT', 'int64[pyarrow]'),
        ('BIGINT', 'int64[pyarrow]'),
        ('VARCHAR', 'string[pyarrow]'),
        ('CHAR', 'string[pyarrow]'),
        ('not a type', 'object'),
    ]
)
def test_get_pd_type(db_type: str, pd_type: str):
    """
    Verify that various database types are mapped to Pandas types.
    """
    assert get_pd_type(db_type) == pd_type
