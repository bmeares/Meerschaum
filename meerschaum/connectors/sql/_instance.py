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


_in_memory_temp_tables: Dict[str, bool] = {}
def _log_temporary_tables_creation(
        self,
        tables: Union[str, List[str]],
        ready_to_drop: bool = False,
        create: bool = True,
        debug: bool = False,
    ) -> SuccessTuple:
    """
    Log a temporary table's creation for later deletion.
    """
    from meerschaum.utils.misc import items_str
    from meerschaum.connectors.sql.tables import get_tables
    sqlalchemy = mrsm.attempt_import('sqlalchemy')
    temp_tables_table = get_tables(
        mrsm_instance = self,
        create = create,
        debug = debug,
    )['temp_tables']
    if isinstance(tables, str):
        tables = [tables]
    now = datetime.now(timezone.utc).replace(tzinfo=None)
    docs = [
        {
            'date_created': now,
            'table': table,
            'ready_to_drop': (now if ready_to_drop else None),
        }
        for table in tables
    ]
    ### NOTE: We may be running in a temporary context, in which we don't create instance tables.
    queries = [sqlalchemy.insert(temp_tables_table).values(**doc) for doc in docs]
    results = [self.exec(query, silent=True, debug=debug) for query in queries]
    success = all(results)
    _in_memory_temp_tables.update(
        {
            table: ready_to_drop
            for table in tables
        }
    )
    msg = "Success" if success else f"Failed to log temp tables {items_str(tables)}."
    return success, msg


def _drop_temporary_table(
        self,
        table: str,
        debug: bool = False,
    ) -> SuccessTuple:
    """
    Drop a temporary table and clear it from the internal table.
    """
    from meerschaum.utils.sql import sql_item_name, table_exists, SKIP_IF_EXISTS_FLAVORS
    if_exists = "IF EXISTS" if self.flavor not in SKIP_IF_EXISTS_FLAVORS else ""
    if not if_exists:
        if not table_exists(table, self, self.internal_schema, debug=debug):
            return True, "Success"

    drop_query = f"DROP TABLE {if_exists} " + sql_item_name(
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
    from meerschaum.connectors.sql.tables import get_tables
    sqlalchemy = mrsm.attempt_import('sqlalchemy')
    temp_tables_table = get_tables(
        mrsm_instance = self,
        create = False,
        debug = debug,
    )['temp_tables']
    query = (
        sqlalchemy.select(temp_tables_table.c.table)
        .where(temp_tables_table.c.ready_to_drop.is_not(None))
    )
    tables_to_drop = [
        table
        for table, ready_to_drop in _in_memory_temp_tables.items()
        if ready_to_drop
    ]
    if not tables_to_drop:
        df = self.read(query, silent=True, debug=debug)
        tables_to_drop = (
            list(df['table'])
            if df is not None
            else []
        )
    if not tables_to_drop:
        return True, "Success"

    dropped_tables = []
    failed_tables = []
    for table in tables_to_drop:
        drop_success, drop_msg = self._drop_temporary_table(table, debug=debug)
        if not drop_success:
            failed_tables.append(table)
            continue
        dropped_tables.append(table)
        _ = _in_memory_temp_tables.pop(table, None)

    if dropped_tables:
        delete_query = (
            sqlalchemy.delete(temp_tables_table)
            .where(temp_tables_table.c.table.in_(dropped_tables))
        )
        delete_result = self.exec(delete_query, silent=True, debug=debug)

    success = len(failed_tables) == 0
    msg = (
        "Success"
        if success
        else (
            "Failed to drop temporary tables "
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
    from meerschaum.connectors.sql.tables import get_tables
    from meerschaum.utils.misc import items_str
    sqlalchemy = mrsm.attempt_import('sqlalchemy')
    temp_tables_table = get_tables(mrsm_instance=self, create=False, debug=debug)['temp_tables']
    last_check = getattr(self, '_stale_temporary_tables_check_timestamp', 0)
    now_ts = time.perf_counter()
    if refresh or not last_check or (now_ts - last_check) > 60:
        self._stale_temporary_tables_check_timestamp = now_ts
        return self._drop_temporary_tables(debug=debug)

    stale_temporary_tables_minutes = get_config(
        'system', 'connectors', 'sql', 'instance', 'stale_temporary_tables_minutes'
    )
    now = datetime.now(timezone.utc).replace(tzinfo=None)
    end = now - timedelta(minutes=stale_temporary_tables_minutes)

    query = (
        sqlalchemy.select(temp_tables_table.c.table)
        .where(temp_tables_table.c.date_created < end)
    )

    df = self.read(query, silent=True, debug=debug)
    if df is None:
        return True, "Success"

    ### Insert new records with the current time (skipping updates to avoid recursion).
    docs = [
        {
            'date_created': now,
            'table': table,
            'ready_to_drop': now,
        }
        for table in df['table']
    ]
    if docs:
        queries = [sqlalchemy.insert(temp_tables_table).values(**doc) for doc in docs]
        results = [self.exec(query, silent=True, debug=debug) for query in queries]
        _in_memory_temp_tables.update(
            {
                table: True
                for table in df['table']
            }
        )

    return self._drop_temporary_tables(debug=debug)
