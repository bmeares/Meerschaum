#! /usr/bin/env python3
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

"""
Test functions from `meerschaum.utils.misc`.
"""

import pytest
from meerschaum.utils.packages import import_pandas
DEBUG: bool = True
pd = import_pandas(debug=DEBUG)


@pytest.mark.parametrize(
    'ldtype,rdtype,are_equal',
    [
        ('string', 'string', True),
        ('str', 'string', True),
        ('str', 'object', True),
        ('int', 'int32', True),
        ('datetime64[ns, UTC]', 'Timestamp', True),
        ('float', 'float64', True),
        ('bool', 'bool[pyarrow]', True),
        ('Int64', 'int', True),
        ('json', 'object', True),
        ('float', 'object', False),
        ('datetime', 'object', False),
        ('uuid', 'object', True),
    ]
)
def test_are_dtypes_equal(ldtype: str, rdtype: str, are_equal: bool):
    """
    Test that different combinations of dtypes are equal (or inequal).
    """
    from meerschaum.utils.dtypes import are_dtypes_equal
    assert are_dtypes_equal(ldtype, rdtype) == are_equal
