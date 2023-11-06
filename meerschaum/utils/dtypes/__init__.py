#! /usr/bin/env python3
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

"""
Utility functions for working with data types.
"""

from decimal import Decimal, Context, InvalidOperation
from meerschaum.utils.typing import Dict, Union, Any

MRSM_PD_DTYPES: Dict[str, str] = {
    'json': 'object',
    'numeric': 'object',
    'datetime': 'datetime64[ns]',
    'bool': 'bool[pyarrow]',
    'int': 'Int64',
    'int8': 'Int8',
    'int16': 'Int16',
    'int32': 'Int32',
    'int64': 'Int64',
    'str': 'string[python]',
}


def to_pandas_dtype(dtype: str) -> str:
    """
    Cast a supported Meerschaum dtype to a Pandas dtype.
    """
    known_dtype = MRSM_PD_DTYPES.get(dtype, None)
    if known_dtype is not None:
        return known_dtype

    ### NOTE: Kind of a hack, but if the first word of the given dtype is in all caps,
    ### treat it as a SQL db type.
    if dtype.split(' ')[0].isupper():
        from meerschaum.utils.dtypes.sql import get_pd_type_from_db_type
        return get_pd_type_from_db_type(dtype)

    import traceback
    from meerschaum.utils.packages import attempt_import
    from meerschaum.utils.warnings import warn
    pandas = attempt_import('pandas', lazy=False)

    try:
        return str(pandas.api.types.pandas_dtype(dtype))
    except Exception as e:
        warn(
            f"Invalid dtype '{dtype}', will use 'object' instead:\n"
            + f"{traceback.format_exc()}",
            stack = False,
        )
    
    return 'object'


def are_dtypes_equal(
        ldtype: Union[str, Dict[str, str]],
        rdtype: Union[str, Dict[str, str]],
    ) -> bool:
    """
    Determine whether two dtype strings may be considered
    equivalent to avoid unnecessary conversions.

    Parameters
    ----------
    ldtype: Union[str, Dict[str, str]]
        The left dtype to compare.
        May also provide a dtypes dictionary.

    rdtype: Union[str, Dict[str, str]]
        The right dtype to compare.
        May also provide a dtypes dictionary.

    Returns
    -------
    A `bool` indicating whether the two dtypes are to be considered equivalent.
    """
    if isinstance(ldtype, dict) and isinstance(rdtype, dict):
        lkeys = sorted([str(k) for k in ldtype.keys()])
        rkeys = sorted([str(k) for k in rdtype.keys()])
        for lkey, rkey in zip(lkeys, rkeys):
            if lkey != rkey:
                return False
            ltype = ldtype[lkey]
            rtype = rdtype[rkey]
            if not are_dtypes_equal(ltype, rtype):
                return False
        return True

    if ldtype == rdtype:
        return True

    ### Sometimes pandas dtype objects are passed.
    ldtype = str(ldtype)
    rdtype = str(rdtype)

    json_dtypes = ('json', 'object')
    if ldtype in json_dtypes and rdtype in json_dtypes:
        return True

    numeric_dtypes = ('numeric', 'object')
    if ldtype in numeric_dtypes and rdtype in numeric_dtypes:
        return True

    ldtype_clean = ldtype.split('[')[0]
    rdtype_clean = rdtype.split('[')[0]

    if ldtype_clean.lower() == rdtype_clean.lower():
        return True

    datetime_dtypes = ('datetime', 'timestamp')
    ldtype_found_dt_prefix = False
    rdtype_found_dt_prefix = False
    for dt_prefix in datetime_dtypes:
        ldtype_found_dt_prefix = (dt_prefix in ldtype.lower()) or ldtype_found_dt_prefix
        rdtype_found_dt_prefix = (dt_prefix in rdtype.lower()) or rdtype_found_dt_prefix
    if ldtype_found_dt_prefix and rdtype_found_dt_prefix:
        return True

    string_dtypes = ('str', 'string', 'object')
    if ldtype_clean in string_dtypes and rdtype_clean in string_dtypes:
        return True

    int_dtypes = ('int', 'int64', 'int32', 'int16', 'int8')
    if ldtype_clean.lower() in int_dtypes and rdtype_clean.lower() in int_dtypes:
        return True

    float_dtypes = ('float', 'float64', 'float32', 'float16', 'float128', 'double')
    if ldtype_clean.lower() in float_dtypes and rdtype_clean.lower() in float_dtypes:
        return True

    bool_dtypes = ('bool', 'boolean')
    if ldtype_clean in bool_dtypes and rdtype_clean in bool_dtypes:
        return True

    return False


def is_dtype_numeric(dtype: str) -> bool:
    """
    Determine whether a given `dtype` string
    should be considered compatible with the Meerschaum dtype `numeric`.

    Parameters
    ----------
    dtype: str
        The pandas-like dtype string.

    Returns
    -------
    A bool indicating the dtype is compatible with `numeric`.
    """
    dtype_lower = dtype.lower()

    acceptable_substrings = ('numeric', 'float', 'double', 'int')
    for substring in acceptable_substrings:
        if substring in dtype_lower:
            return True

    return False


def attempt_cast_to_numeric(value: Any) -> Any:
    """
    Given a value, attempt to coerce it into a numeric (Decimal).
    """
    if isinstance(value, Decimal):
        return value
    try:
        return (
            Decimal(str(value))
            if not value_is_null(value)
            else Decimal('NaN')
        )
    except Exception as e:
        return value


def value_is_null(value: Any) -> Any:
    """
    Determine if a value is a null-like string.
    """
    return str(value).lower() in ('none', 'nan', 'na', 'nat', '', '<na>')


def none_if_null(value: Any) -> Any:
    """
    Return `None` if a value is a null-like string.
    """
    return (None if value_is_null(value) else value)


def quantize_decimal(x: Decimal, scale: int, precision: int) -> Decimal:
    """
    Quantize a given `Decimal` to a known scale and precision.

    Parameters
    ----------
    x: Decimal
        The `Decimal` to be quantized.

    scale: int
        The total number of significant digits.

    precision: int
        The number of significant digits after the decimal point.

    Returns
    -------
    A `Decimal` quantized to the specified scale and precision.
    """
    precision_decimal = Decimal((('1' * scale) + '.' + ('1' * precision)))
    try:
        return x.quantize(precision_decimal, context=Context(prec=scale))
    except InvalidOperation:
        return x
