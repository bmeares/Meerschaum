#! /usr/bin/env python3
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

"""
Create a new SQLConnector from a URI.
"""

import os
from meerschaum.utils.typing import Optional, Dict, Any, Union
from meerschaum.utils.warnings import warn, error
from meerschaum.utils.packages import attempt_import

@classmethod
def from_uri(
        cls,
        uri: str,
        label: Optional[str] = None,
        as_dict: bool = False,
    ) -> Union[
        'meerschaum.connectors.SQLConnector',
        Dict[str, Union[str, int]],
    ]:
    """
    Create a new SQLConnector from a URI string.

    Parameters
    ----------
    uri: str
        The URI connection string.

    label: Optional[str], default None
        If provided, use this as the connector label.
        Otherwise use the determined database name.

    as_dict: bool, default False
        If `True`, return a dictionary of the keyword arguments
        necessary to create a new `SQLConnector`, otherwise create a new object.

    Returns
    -------
    A new SQLConnector object or a dictionary of attributes (if `as_dict` is `True`).
    """

    params = cls.parse_uri(uri)
    params['uri'] = uri
    flavor = params.get('flavor', None)
    if not flavor or flavor not in cls.flavor_configs:
        error(f"Invalid flavor '{flavor}' detected from the provided URI.")

    if 'database' not in params:
        error("Unable to determine the database from the provided URI.")

    if flavor in ('sqlite', 'duckdb'):
        if params['database'] == ':memory:':
            params['label'] = label or f'memory_{flavor}'
        else:
            params['label'] = label or params['database'].split(os.path.sep)[-1].lower()
    else:
        params['label'] = label or (
            (
                (params['username'] + '@' if 'username' in params else '')
                + params.get('host', '')
                + ('/' if 'host' in params else '')
                + params.get('database', '')
            ).lower()
        )

    return cls(**params) if not as_dict else params


@staticmethod
def parse_uri(uri: str) -> Dict[str, Any]:
    """
    Parse a URI string into a dictionary of parameters.

    Parameters
    ----------
    uri: str
        The database connection URI.

    Returns
    -------
    A dictionary of attributes.

    Examples
    --------
    >>> parse_uri('sqlite:////home/foo/bar.db')
    {'database': '/home/foo/bar.db', 'flavor': 'sqlite'}
    >>> parse_uri(
    ...     'mssql+pyodbc://sa:supersecureSECRETPASSWORD123!@localhost:1439'
    ...     + '/master?driver=ODBC+Driver+17+for+SQL+Server'
    ... )
    {'host': 'localhost', 'database': 'master', 'username': 'sa',
    'password': 'supersecureSECRETPASSWORD123!', 'port': 1439, 'flavor': 'mssql',
    'driver': 'ODBC Driver 17 for SQL Server'}
    >>> 
    """
    from urllib.parse import parse_qs, urlparse
    sqlalchemy = attempt_import('sqlalchemy')
    parser = sqlalchemy.engine.url.make_url
    params = parser(uri).translate_connect_args()
    params['flavor'] = uri.split(':')[0].split('+')[0]
    if params['flavor'] == 'postgres':
        params['flavor'] = 'postgresql'
    if '?' in uri:
        parsed_uri = urlparse(uri)
        for key, value in parse_qs(parsed_uri.query).items():
            params.update({key: value[0]})

        if '--search_path' in params.get('options', ''):
            params.update({'schema': params['options'].replace('--search_path=', '', 1)})
    return params
