#! /usr/bin/env python3
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

"""
Utility functions for working with data types.
"""

from meerschaum.utils.typing import Dict, Union

MRSM_PD_DTYPES: Dict[str, str] = {
    'json': 'object',
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
        lkeys = sorted(list(ldtype.keys()))
        rkeys = sorted(list(rdtype.keys()))
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
