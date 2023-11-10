#! /usr/bin/env python3
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

"""
Define utilities for instance connectors.
"""

import time
from datetime import datetime, timezone, timedelta
import meerschaum as mrsm
from meerschaum.utils.typing import Dict, SuccessTuple, Optional, Union, List
from meerschaum.utils.warnings import warn

def _get_temporary_tables_pipe(self) -> mrsm.Pipe:
    """
    Return a pipe for logging temporary tables.
    """
    return mrsm.Pipe(
        "temporary", "tables",
        target = "mrsm_temp_tables",
        temporary = True,
        instance = self,
        columns = {
            'datetime': 'date_created',
            'table': 'table',
            'ready_to_drop': 'ready_to_drop',
        },
        dtypes = {
            'ready_to_drop': 'datetime',
        },
        parameters = {
            'schema': self.instance_schema,
        },
    )


def _log_temporary_tables_creation(
        self,
        tables: Union[str, List[str]],
        ready_to_drop: bool = False,
        debug: bool = False,
    ) -> SuccessTuple:
    """
    Log a temporary table's creation for later deletion.
    """
    if isinstance(tables, str):
        tables = [tables]
    temporary_tables_pipe = self._get_temporary_tables_pipe()
    now = datetime.now(timezone.utc).replace(tzinfo=None)
    return temporary_tables_pipe.sync(
        [
            {
                'date_created': now,
                'table': table,
                'ready_to_drop': (now if ready_to_drop else None),
            }
            for table in tables
        ],
        check_existing = False,
        debug = debug,
    )


def _drop_temporary_table(
        self,
        table: str,
        debug: bool = False,
    ) -> SuccessTuple:
    """
    Drop a temporary table and clear it from the internal table.
    """
    from meerschaum.utils.sql import sql_item_name, table_exists
    temporary_tables_pipe = self._get_temporary_tables_pipe()
    if table_exists(table, self, self.internal_schema, debug=debug):
        return True, "Success"
    drop_query = f"DROP TABLE IF EXISTS " + sql_item_name(
        table, self.flavor, schema=self.internal_schema
    )
    drop_success = self.exec(drop_query, silent=True, debug=debug) is not None
    drop_msg = "Success" if drop_success else f"Failed to drop temporary table '{table}'."
    return drop_success, drop_msg


def _drop_temporary_tables(self, debug: bool = False) -> SuccessTuple:
    """
    Drop all tables in the internal schema that are marked as ready to be dropped.
    """
    from meerschaum.utils.sql import sql_item_name, table_exists
    from meerschaum.utils.misc import items_str

    temporary_tables_pipe = self._get_temporary_tables_pipe()
    df = temporary_tables_pipe.get_data(['table'], params={'ready_to_drop': '_None'}, debug=debug)
    if df is None:
        return True, "Success"

    dropped_tables = []
    failed_tables = []
    for table in df['table']:
        drop_success, drop_msg = self._drop_temporary_table(table, debug=debug)
        if not drop_success:
            failed_tables.append(table)
            continue
        dropped_tables.append(table)

    if dropped_tables:
        temporary_tables_pipe.clear(params={'table': dropped_tables}, debug=debug)

    success = len(failed_tables) == 0
    msg = (
        "Success"
        if success
        else (
            "Failed to drop stale temporary tables "
            + f"{items_str(failed_tables)}."
        )
    )
    return success, msg


def _drop_old_temporary_tables(
        self,
        refresh: bool = True,
        debug: bool = False,
    ) -> SuccessTuple:
    """
    Drop temporary tables older than the configured interval (24 hours by default).
    """
    from meerschaum.config import get_config
    last_check = getattr(self, '_stale_temporary_tables_check_timestamp', 0)
    now_ts = time.perf_counter()
    if refresh or not last_check or (now_ts - last_check) > 60:
        self._stale_temporary_tables_check_timestamp = now_ts
        return self._drop_temporary_tables(debug=debug)

    stale_temporary_tables_minutes = get_config(
        'system', 'connectors', 'sql', 'instance', 'stale_temporary_tables_minutes'
    )
    temporary_tables_pipe = self._get_temporary_tables_pipe()
    now = datetime.now(timezone.utc).replace(tzinfo=None)
    end = now - timedelta(minutes=stale_temporary_tables_minutes)
    df = temporary_tables_pipe.get_data(end=end, debug=debug)
    if df is None:
        return True, "Success"

    ### Insert new records with the current time (skipping updates to avoid recursion).
    docs = [
        {
            'date_created': now,
            'table': doc['table'],
            'ready_to_drop': now,
        }
        for doc in df.to_dict(orient='records')
    ]
    if docs:
        update_temporary_success, update_temporary_msg = temporary_tables_pipe.sync(
            docs,
            check_existing = False,
            debug = debug,
        )
        if not update_temporary_success:
            warn(update_temporary_msg)

    return self._drop_temporary_tables(debug=debug)
