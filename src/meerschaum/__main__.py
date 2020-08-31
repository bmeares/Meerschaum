#! /usr/bin/env python
# -*- coding: utf-8 -*-
# vim:fenc=utf-8
"""
mrsm CLI entrypoint
"""

def main(sysargs=None):
    from meerschaum.connectors import SQLConnector
    connector = SQLConnector()
    df = connector.read("SELECT * FROM m_home_temp_hist_raw")
    print(df)


if __name__ == "__main__":
    import sys
    main(sys.argv[1:])
