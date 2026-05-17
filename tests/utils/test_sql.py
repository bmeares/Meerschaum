#! /usr/bin/env python3
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

"""
Test SQL utility functions.
"""

from typing import Dict, List, Any, Tuple
import pytest
from meerschaum.utils.sql import (
    build_where,
    clean,
    dateadd_str,
    get_pd_type,
)
from meerschaum.utils.dtypes.sql import get_numeric_precision_scale
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
        (
            {'a': [None, 1], 'b': ['_']},
            [
                "\"a\" IN ('1')",
                "\"a\" IS NULL",
                "\"b\" IS NOT NULL",
            ],
        ),
        (
            {'a': ['NaN'], 'b': ['_1', '_none']},
            [
                "\"a\" IS NULL",
                "\"b\" NOT IN ('1')",
                "\"b\" IS NOT NULL",
            ],
        ),
    ]
)
def test_build_where(params: Dict[str, Any], expected_subqueries: List[str]):
    """
    Test that build_where() correctly produces the expected query.
    """
    where_subquery = build_where(
        params,
        mrsm.get_connector(
            'sql', 'build_where_test',
            uri='postgresql+psycopg2://foo:bar@localhost:5432/baz'
        )
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
        ('DATE', 'date'),
        ('TIMESTAMP', 'datetime64[ns]'),
        ('BOOL', 'bool[pyarrow]'),
        ('BOOLEAN', 'bool[pyarrow]'),
        ('FLOAT', 'float64[pyarrow]'),
        ('DOUBLE', 'float64[pyarrow]'),
        ('REAL', 'float64[pyarrow]'),
        ('TIMESTAMPTZ', 'datetime64[ns, UTC]'),
        ('TIMESTAMP WITH TIMEZONE', 'datetime64[ns, UTC]'),
        ('TIMESTAMP WITHOUT TIMEZONE', 'datetime64[ns]'),
        ('CLOB', 'string[pyarrow]'),
        ('NUMERIC', 'numeric'),
        ('NUMERIC(12, 10)', 'numeric[12,10]'),
        ('DECIMAL', 'numeric'),
        ('NUMBER', 'numeric'),
        ('INT', 'int64[pyarrow]'),
        ('BIGINT', 'int64[pyarrow]'),
        ('VARCHAR', 'string[pyarrow]'),
        ('CHAR', 'string[pyarrow]'),
        ('BYTEA', 'bytes'),
        ('VARBINARY', 'bytes'),
        ('not a type', 'object'),
    ]
)
def test_get_pd_type(db_type: str, pd_type: str):
    """
    Verify that various database types are mapped to Pandas types.
    """
    from meerschaum.utils.dtypes import are_dtypes_equal
    assert are_dtypes_equal(get_pd_type(db_type), pd_type)


@pytest.mark.parametrize(
    'flavor,dtype,expected_precision_and_scale',
    [
        (None, 'numeric[28,10]', (28, 10)),
        (None, 'numeric', (None, None)),
        ('postgresql', 'numeric', (None, None)),
        ('mssql', 'numeric', (28, 10)),
        ('timescaledb', 'numeric[28,10]', (28, 10)),
        ('not_a_flavor', 'numeric', (None, None)),
        (None, 'DECIMAL(5, 2)', (5, 2)),
    ]
)
def test_get_numeric_precision_scale(flavor: str, dtype: str, expected_precision_and_scale: Tuple[Any, Any]):
    """
    Test that dtypes may be parsed into precision and scale.
    """
    expected_precision, expected_scale = expected_precision_and_scale
    precision, scale = get_numeric_precision_scale(flavor, dtype=dtype)
    assert precision == expected_precision
    assert scale == expected_scale


@pytest.mark.parametrize('bad_string', [
    '; DROP TABLE users',
    '-- comment',
    'drop table foo',
    '/* block comment',
    'union select password from users',
    'exec xp_cmdshell',
    'execute sp_executesql',
    'insert into foo',
    'update users set',
    'delete from foo',
    'create table evil',
    'alter table foo',
    'truncate table foo',
])
def test_clean_rejects_injection(bad_string: str):
    """
    clean() must raise on strings containing banned SQL patterns.
    """
    with pytest.raises(Exception):
        clean(bad_string)


@pytest.mark.parametrize('safe_string', [
    'normal_value',
    '2024-01-01',
    'some text without keywords',
    '12345',
    'hello world',
])
def test_clean_allows_safe_strings(safe_string: str):
    """
    clean() must not raise on benign strings.
    """
    clean(safe_string)


@pytest.mark.parametrize('bad_datepart', [
    "day'; DROP TABLE users; --",
    'week',
    'quarter',
    'epoch',
    'microsecond',
    '',
    'DAY',
    "day' UNION SELECT 1--",
])
def test_dateadd_str_rejects_invalid_datepart(bad_datepart: str):
    """
    dateadd_str() must raise ValueError for non-whitelisted datepart values.
    """
    with pytest.raises(ValueError):
        dateadd_str(flavor='postgresql', datepart=bad_datepart, number=1, begin='now')


def test_dateadd_str_none_datepart_allowed_for_int_begin():
    """
    datepart=None is valid when begin is an integer (int path returns before datepart is used).
    """
    result = dateadd_str(flavor='postgresql', datepart=None, number=5, begin=1000)
    assert result == '1000 + 5'


@pytest.mark.parametrize('good_datepart', ['year', 'month', 'day', 'hour', 'minute', 'second'])
def test_dateadd_str_accepts_valid_dateparts(good_datepart: str):
    """
    dateadd_str() must accept the six standard datepart values.
    """
    result = dateadd_str(flavor='postgresql', datepart=good_datepart, number=1, begin='now')
    assert isinstance(result, str)
    assert len(result) > 0


@pytest.mark.parametrize('injection_params', [
    {'col': "1'; DROP TABLE users; --"},
    {'col': "val' OR '1'='1"},
    {'col': "x' UNION SELECT password FROM users --"},
])
def test_build_where_escapes_values(injection_params: Dict[str, Any]):
    """
    build_where() must either abort (return '') or escape quotes so the raw
    injection payload does not appear verbatim in the output.
    """
    import warnings
    conn = mrsm.get_connector(
        'sql', 'build_where_injection_test',
        uri='postgresql+psycopg2://foo:bar@localhost:5432/baz',
    )
    with warnings.catch_warnings():
        warnings.filterwarnings('ignore', message='.*SQL injection.*', category=UserWarning)
        result = build_where(injection_params, conn)
    raw_value = list(injection_params.values())[0]
    # Either aborted entirely or the raw unescaped value is absent
    assert result == '' or raw_value not in result
