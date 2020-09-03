#! /usr/bin/env python
# -*- coding: utf-8 -*-
# vim:fenc=utf-8
"""
This module contains SQLConnector functions for executing SQL queries.
"""

import pandas as pd
import sqlalchemy

def read(self, query : str, debug=False) -> pd.DataFrame:
    """
    Read a SQL query into a pandas dataframe.
    """
    if debug: print(query)
    try:
        df = pd.read_sql_query(query, self.engine)
    except Exception as e:
        print(f"Failed to execute query:\n\n{query}\n\n")
        print(e)
        return False
    return df

def exec(self, query, debug=False) -> bool:
    """
    Execute SQL code and return success status. e.g. calling stored procedures
    """
    try:
        with self.engine.connect() as connection:
            result = connection.execute(
                sqlalchemy.text(query).execution_options(autocommit=True)
            )
    except Exception as e:
        print(f"Failed to execute query:\n\n{query}\n\n")
        print(e)
        result = False
    return result

def to_sql(
        self,
        df : pd.DataFrame,
        name : str = None,
        index : bool = False,
        if_exists : str = 'replace',
        dtype : 'dictionary/scalar' = {},
        **kw
    ):
    """
    """
    if name is None:
        raise Exception("Name must not be None to submit to the SQL server")
    try:
        df.to_sql(
            name=name,
            con=self.engine,
            index=index,
            if_exists=if_exists,
            dtype=dtype,
            **kw
        )
    except Exception as e:
        print(f'Failed to commit dataframe with name: {name}')
        print(e)
        return False
    return True
