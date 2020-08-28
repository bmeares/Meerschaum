#! /usr/bin/env python
# -*- coding: utf-8 -*-
# vim:fenc=utf-8
"""
mrsm CLI entrypoint
"""

def main(sysargs=None):
    #  print('sysargs:', sysargs)
    from meerschaum.connectors import SQLConnector
    connector = SQLConnector()
    import pandas as pd
    df = pd.DataFrame({})
    connector.to_sql(df, name="test_table")
    pass
