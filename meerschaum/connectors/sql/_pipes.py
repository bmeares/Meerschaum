#! /usr/bin/env python
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

"""
Interact with Pipes metadata via SQLConnector.
"""
from __future__ import annotations
from datetime import datetime, date, timedelta
import meerschaum as mrsm
from meerschaum.utils.typing import (
    Union, Any, SuccessTuple, Tuple, Dict, Optional, List
)
from meerschaum.utils.warnings import warn, error
from meerschaum.utils.debug import dprint


def register_pipe(
    self,
    pipe: mrsm.Pipe,
    debug: bool = False,
) -> SuccessTuple:
    """
    Register a new pipe.
    A pipe's attributes must be set before registering.
    """
    from meerschaum.utils.debug import dprint
    from meerschaum.utils.packages import attempt_import
    from meerschaum.utils.sql import json_flavors

    ### ensure pipes table exists
    from meerschaum.connectors.sql.tables import get_tables
    pipes_tbl = get_tables(mrsm_instance=self, create=(not pipe.temporary), debug=debug)['pipes']

    if pipe.get_id(debug=debug) is not None:
        return False, f"{pipe} is already registered."

    ### NOTE: if `parameters` is supplied in the Pipe constructor,
    ###       then `pipe.parameters` will exist and not be fetched from the database.

    ### 1. Prioritize the Pipe object's `parameters` first.
    ###    E.g. if the user manually sets the `parameters` property
    ###    or if the Pipe already exists
    ###    (which shouldn't be able to be registered anyway but that's an issue for later).
    parameters = None
    try:
        parameters = pipe.parameters
    except Exception as e:
        if debug:
            dprint(str(e))
        parameters = None

    ### ensure `parameters` is a dictionary
    if parameters is None:
        parameters = {}

    import json
    sqlalchemy = attempt_import('sqlalchemy')
    values = {
        'connector_keys' : pipe.connector_keys,
        'metric_key'     : pipe.metric_key,
        'location_key'   : pipe.location_key,
        'parameters'     : (
            json.dumps(parameters)
            if self.flavor not in json_flavors
            else parameters
        ),
    }
    query = sqlalchemy.insert(pipes_tbl).values(**values)
    result = self.exec(query, debug=debug)
    if result is None:
        return False, f"Failed to register {pipe}."
    return True, f"Successfully registered {pipe}."


def edit_pipe(
    self,
    pipe : mrsm.Pipe = None,
    patch: bool = False,
    debug: bool = False,
    **kw : Any
) -> SuccessTuple:
    """
    Persist a Pipe's parameters to its database.

    Parameters
    ----------
    pipe: mrsm.Pipe, default None
        The pipe to be edited.
    patch: bool, default False
        If patch is `True`, update the existing parameters by cascading.
        Otherwise overwrite the parameters (default).
    debug: bool, default False
        Verbosity toggle.
    """

    if pipe.id is None:
        return False, f"{pipe} is not registered and cannot be edited."

    from meerschaum.utils.debug import dprint
    from meerschaum.utils.packages import attempt_import
    from meerschaum.utils.sql import json_flavors
    if not patch:
        parameters = pipe.__dict__.get('_attributes', {}).get('parameters', {})
    else:
        from meerschaum import Pipe
        from meerschaum.config._patch import apply_patch_to_config
        original_parameters = Pipe(
            pipe.connector_keys, pipe.metric_key, pipe.location_key,
            mrsm_instance=pipe.instance_keys
        ).parameters
        parameters = apply_patch_to_config(
            original_parameters,
            pipe.parameters
        )

    ### ensure pipes table exists
    from meerschaum.connectors.sql.tables import get_tables
    pipes_tbl = get_tables(mrsm_instance=self, create=(not pipe.temporary), debug=debug)['pipes']

    import json
    sqlalchemy = attempt_import('sqlalchemy')

    values = {
        'parameters': (
            json.dumps(parameters)
            if self.flavor not in json_flavors
            else parameters
        ),
    }
    q = sqlalchemy.update(pipes_tbl).values(**values).where(
        pipes_tbl.c.pipe_id == pipe.id
    )

    result = self.exec(q, debug=debug)
    message = (
        f"Successfully edited {pipe}."
        if result is not None else f"Failed to edit {pipe}."
    )
    return (result is not None), message


def fetch_pipes_keys(
    self,
    connector_keys: Optional[List[str]] = None,
    metric_keys: Optional[List[str]] = None,
    location_keys: Optional[List[str]] = None,
    tags: Optional[List[str]] = None,
    params: Optional[Dict[str, Any]] = None,
    debug: bool = False
) -> Optional[List[Tuple[str, str, Optional[str]]]]:
    """
    Return a list of tuples corresponding to the parameters provided.

    Parameters
    ----------
    connector_keys: Optional[List[str]], default None
        List of connector_keys to search by.

    metric_keys: Optional[List[str]], default None
        List of metric_keys to search by.

    location_keys: Optional[List[str]], default None
        List of location_keys to search by.

    params: Optional[Dict[str, Any]], default None
        Dictionary of additional parameters to search by.
        E.g. `--params pipe_id:1`

    debug: bool, default False
        Verbosity toggle.
    """
    from meerschaum.utils.debug import dprint
    from meerschaum.utils.packages import attempt_import
    from meerschaum.utils.misc import separate_negation_values, flatten_list
    from meerschaum.utils.sql import OMIT_NULLSFIRST_FLAVORS, table_exists
    from meerschaum.config.static import STATIC_CONFIG
    import json
    from copy import deepcopy
    sqlalchemy, sqlalchemy_sql_functions = attempt_import('sqlalchemy', 'sqlalchemy.sql.functions')
    coalesce = sqlalchemy_sql_functions.coalesce

    if connector_keys is None:
        connector_keys = []
    if metric_keys is None:
        metric_keys = []
    if location_keys is None:
        location_keys = []
    else:
        location_keys = [
            (
                lk
                if lk not in ('[None]', 'None', 'null')
                else 'None'
            )
            for lk in location_keys
        ]
    if tags is None:
        tags = []

    if params is None:
        params = {}

    ### Add three primary keys to params dictionary
    ###   (separated for convenience of arguments).
    cols = {
        'connector_keys': [str(ck) for ck in connector_keys],
        'metric_key': [str(mk) for mk in metric_keys],
        'location_key': [str(lk) for lk in location_keys],
    }

    ### Make deep copy so we don't mutate this somewhere else.
    parameters = deepcopy(params)
    for col, vals in cols.items():
        if vals not in [[], ['*']]:
            parameters[col] = vals

    if not table_exists('mrsm_pipes', self, schema=self.instance_schema, debug=debug):
        return []

    from meerschaum.connectors.sql.tables import get_tables
    pipes_tbl = get_tables(mrsm_instance=self, create=False, debug=debug)['pipes']

    _params = {}
    for k, v in parameters.items():
        _v = json.dumps(v) if isinstance(v, dict) else v
        _params[k] = _v

    negation_prefix = STATIC_CONFIG['system']['fetch_pipes_keys']['negation_prefix']
    ### Parse regular params.
    ### If a param begins with '_', negate it instead.
    _where = [
        (
            (coalesce(pipes_tbl.c[key], 'None') == val)
            if not str(val).startswith(negation_prefix)
            else (pipes_tbl.c[key] != key)
        ) for key, val in _params.items()
        if not isinstance(val, (list, tuple)) and key in pipes_tbl.c
    ]
    select_cols = (
        [
            pipes_tbl.c.connector_keys,
            pipes_tbl.c.metric_key,
            pipes_tbl.c.location_key,
        ]
    )

    q = sqlalchemy.select(*select_cols).where(sqlalchemy.and_(True, *_where))
    for c, vals in cols.items():
        if not isinstance(vals, (list, tuple)) or not vals or not c in pipes_tbl.c:
            continue
        _in_vals, _ex_vals = separate_negation_values(vals)
        q = q.where(coalesce(pipes_tbl.c[c], 'None').in_(_in_vals)) if _in_vals else q
        q = q.where(coalesce(pipes_tbl.c[c], 'None').not_in(_ex_vals)) if _ex_vals else q

    ### Finally, parse tags.
    tag_groups = [tag.split(',') for tag in tags]
    in_ex_tag_groups = [separate_negation_values(tag_group) for tag_group in tag_groups]

    ors, nands = [], []
    for _in_tags, _ex_tags in in_ex_tag_groups:
        sub_ands = []
        for nt in _in_tags:
            sub_ands.append(
                sqlalchemy.cast(
                    pipes_tbl.c['parameters'],
                    sqlalchemy.String,
                ).like(f'%"tags":%"{nt}"%')
            )
        if sub_ands:
            ors.append(sqlalchemy.and_(*sub_ands))

        for xt in _ex_tags:
            nands.append(
                sqlalchemy.cast(
                    pipes_tbl.c['parameters'],
                    sqlalchemy.String,
                ).not_like(f'%"tags":%"{xt}"%')
            )

    q = q.where(sqlalchemy.and_(*nands)) if nands else q
    q = q.where(sqlalchemy.or_(*ors)) if ors else q
    loc_asc = sqlalchemy.asc(pipes_tbl.c['location_key'])
    if self.flavor not in OMIT_NULLSFIRST_FLAVORS:
        loc_asc = sqlalchemy.nullsfirst(loc_asc)
    q = q.order_by(
        sqlalchemy.asc(pipes_tbl.c['connector_keys']),
        sqlalchemy.asc(pipes_tbl.c['metric_key']),
        loc_asc,
    )

    ### execute the query and return a list of tuples
    if debug:
        dprint(q.compile(compile_kwargs={'literal_binds': True}))
    try:
        rows = (
            self.execute(q).fetchall()
            if self.flavor != 'duckdb'
            else [
                (row['connector_keys'], row['metric_key'], row['location_key'])
                for row in self.read(q).to_dict(orient='records')
            ]
        )
    except Exception as e:
        error(str(e))

    return [(row[0], row[1], row[2]) for row in rows]


def create_indices(
    self,
    pipe: mrsm.Pipe,
    indices: Optional[List[str]] = None,
    debug: bool = False
) -> bool:
    """
    Create a pipe's indices.
    """
    from meerschaum.utils.sql import sql_item_name, update_queries
    from meerschaum.utils.debug import dprint
    if debug:
        dprint(f"Creating indices for {pipe}...")
    if not pipe.indices:
        warn(f"{pipe} has no index columns; skipping index creation.", stack=False)
        return True

    _ = pipe.__dict__.pop('_columns_indices', None)
    ix_queries = {
        ix: queries
        for ix, queries in self.get_create_index_queries(pipe, debug=debug).items()
        if indices is None or ix in indices
    }
    success = True
    for ix, queries in ix_queries.items():
        ix_success = all(self.exec_queries(queries, debug=debug, silent=False))
        success = success and ix_success
        if not ix_success:
            warn(f"Failed to create index on column: {ix}")

    return success


def drop_indices(
    self,
    pipe: mrsm.Pipe,
    indices: Optional[List[str]] = None,
    debug: bool = False
) -> bool:
    """
    Drop a pipe's indices.
    """
    from meerschaum.utils.debug import dprint
    if debug:
        dprint(f"Dropping indices for {pipe}...")
    if not pipe.columns:
        warn(f"Unable to drop indices for {pipe} without columns.", stack=False)
        return False
    ix_queries = {
        ix: queries
        for ix, queries in self.get_drop_index_queries(pipe, debug=debug).items()
        if indices is None or ix in indices
    }
    success = True
    for ix, queries in ix_queries.items():
        ix_success = all(self.exec_queries(queries, debug=debug, silent=True))
        if not ix_success:
            success = False
            if debug:
                dprint(f"Failed to drop index on column: {ix}")
    return success


def get_create_index_queries(
    self,
    pipe: mrsm.Pipe,
    debug: bool = False,
) -> Dict[str, List[str]]:
    """
    Return a dictionary mapping columns to a `CREATE INDEX` or equivalent query.

    Parameters
    ----------
    pipe: mrsm.Pipe
        The pipe to which the queries will correspond.

    Returns
    -------
    A dictionary of index names mapping to lists of queries.
    """
    ### NOTE: Due to recent breaking changes in DuckDB, indices don't behave properly.
    if self.flavor == 'duckdb':
        return {}
    from meerschaum.utils.sql import (
        sql_item_name,
        get_distinct_col_count,
        update_queries,
        get_null_replacement,
        get_create_table_queries,
        get_rename_table_queries,
        COALESCE_UNIQUE_INDEX_FLAVORS,
    )
    from meerschaum.utils.dtypes.sql import (
        get_db_type_from_pd_type,
        get_pd_type_from_db_type,
        AUTO_INCREMENT_COLUMN_FLAVORS,
    )
    from meerschaum.config import get_config
    index_queries = {}

    upsert = pipe.parameters.get('upsert', False) and (self.flavor + '-upsert') in update_queries
    static = pipe.parameters.get('static', False)
    index_names = pipe.get_indices()
    indices = pipe.indices
    existing_cols_types = pipe.get_columns_types(debug=debug)
    existing_cols_pd_types = {
        col: get_pd_type_from_db_type(typ)
        for col, typ in existing_cols_types.items()
    }
    existing_cols_indices = self.get_pipe_columns_indices(pipe, debug=debug)
    existing_ix_names = set()
    existing_primary_keys = []
    for col, col_indices in existing_cols_indices.items():
        for col_ix_doc in col_indices:
            existing_ix_names.add(col_ix_doc.get('name', None))
            if col_ix_doc.get('type', None) == 'PRIMARY KEY':
                existing_primary_keys.append(col)

    _datetime = pipe.get_columns('datetime', error=False)
    _datetime_name = (
        sql_item_name(_datetime, self.flavor, None)
        if _datetime is not None else None
    )
    _datetime_index_name = (
        sql_item_name(index_names['datetime'], flavor=self.flavor, schema=None)
        if index_names.get('datetime', None)
        else None
    )
    _id = pipe.get_columns('id', error=False)
    _id_name = (
        sql_item_name(_id, self.flavor, None)
        if _id is not None
        else None
    )
    primary_key = pipe.columns.get('primary', None)
    primary_key_name = (
        sql_item_name(primary_key, flavor=self.flavor, schema=None)
        if primary_key
        else None
    )
    autoincrement = (
        pipe.parameters.get('autoincrement', False)
        or (
            primary_key is not None
            and primary_key not in existing_cols_pd_types
        )
    )
    primary_key_db_type = (
        get_db_type_from_pd_type(pipe.dtypes.get(primary_key, 'int'), self.flavor)
        if primary_key
        else None
    )
    primary_key_constraint_name = (
        sql_item_name(f'pk_{pipe.target}', self.flavor, None)
        if primary_key is not None
        else None
    )

    _id_index_name = (
        sql_item_name(index_names['id'], self.flavor, None)
        if index_names.get('id', None)
        else None
    )
    _pipe_name = sql_item_name(pipe.target, self.flavor, self.get_pipe_schema(pipe))
    _create_space_partition = get_config('system', 'experimental', 'space')

    ### create datetime index
    if _datetime is not None:
        if self.flavor == 'timescaledb' and pipe.parameters.get('hypertable', True):
            _id_count = (
                get_distinct_col_count(_id, f"SELECT {_id_name} FROM {_pipe_name}", self)
                if (_id is not None and _create_space_partition) else None
            )

            chunk_interval = pipe.get_chunk_interval(debug=debug)
            chunk_interval_minutes = (
                chunk_interval
                if isinstance(chunk_interval, int)
                else int(chunk_interval.total_seconds() / 60)
            )
            chunk_time_interval = (
                f"INTERVAL '{chunk_interval_minutes} MINUTES'"
                if isinstance(chunk_interval, timedelta)
                else f'{chunk_interval_minutes}'
            )

            dt_query = (
                f"SELECT public.create_hypertable('{_pipe_name}', " +
                f"'{_datetime}', "
                + (
                    f"'{_id}', {_id_count}, " if (_id is not None and _create_space_partition)
                    else ''
                )
                + f'chunk_time_interval => {chunk_time_interval}, '
                + 'if_not_exists => true, '
                + "migrate_data => true);"
            )
        elif self.flavor == 'mssql':
            dt_query = (
                "CREATE "
                + ("CLUSTERED " if not primary_key else '')
                + f"INDEX {_datetime_index_name} "
                + f"ON {_pipe_name} ({_datetime_name})"
            )
        else: ### mssql, sqlite, etc.
            dt_query = (
                f"CREATE INDEX {_datetime_index_name} "
                + f"ON {_pipe_name} ({_datetime_name})"
            )

        index_queries[_datetime] = [dt_query]

    primary_queries = []
    if (
        primary_key is not None
        and primary_key not in existing_primary_keys
        and not static
    ):
        if autoincrement and primary_key not in existing_cols_pd_types:
            autoincrement_str = AUTO_INCREMENT_COLUMN_FLAVORS.get(
                self.flavor,
                AUTO_INCREMENT_COLUMN_FLAVORS['default']
            )
            primary_queries.extend([
                (
                    f"ALTER TABLE {_pipe_name}\n"
                    f"ADD {primary_key_name} {primary_key_db_type} {autoincrement_str}"
                ),
            ])
        elif not autoincrement and primary_key in existing_cols_pd_types:
            if self.flavor == 'sqlite':
                new_table_name = sql_item_name(
                    f'_new_{pipe.target}',
                    self.flavor,
                    self.get_pipe_schema(pipe)
                )
                select_cols_str = ', '.join(
                    [
                        sql_item_name(col, self.flavor, None)
                        for col in existing_cols_types
                    ]
                )
                primary_queries.extend(
                    get_create_table_queries(
                        existing_cols_pd_types,
                        f'_new_{pipe.target}',
                        self.flavor,
                        schema=self.get_pipe_schema(pipe),
                        primary_key=primary_key,
                    ) + [
                        (
                            f"INSERT INTO {new_table_name} ({select_cols_str})\n"
                            f"SELECT {select_cols_str}\nFROM {_pipe_name}"
                        ),
                        f"DROP TABLE {_pipe_name}",
                    ] + get_rename_table_queries(
                        f'_new_{pipe.target}',
                        pipe.target,
                        self.flavor,
                        schema=self.get_pipe_schema(pipe),
                    )
                )
            elif self.flavor == 'oracle':
                primary_queries.extend([
                    (
                        f"ALTER TABLE {_pipe_name}\n"
                        f"MODIFY {primary_key_name} NOT NULL"
                    ),
                    (
                        f"ALTER TABLE {_pipe_name}\n"
                        f"ADD CONSTRAINT {primary_key_constraint_name} PRIMARY KEY ({primary_key_name})"
                    )
                ])
            elif self.flavor in ('mysql', 'mariadb'):
                primary_queries.extend([
                    (
                        f"ALTER TABLE {_pipe_name}\n"
                        f"MODIFY {primary_key_name} {primary_key_db_type} NOT NULL"
                    ),
                    (
                        f"ALTER TABLE {_pipe_name}\n"
                        f"ADD CONSTRAINT {primary_key_constraint_name} PRIMARY KEY ({primary_key_name})"
                    )
                ])
            elif self.flavor == 'timescaledb':
                primary_queries.extend([
                    (
                        f"ALTER TABLE {_pipe_name}\n"
                        f"ALTER COLUMN {primary_key_name} SET NOT NULL"
                    ),
                    (
                        f"ALTER TABLE {_pipe_name}\n"
                        f"ADD CONSTRAINT {primary_key_constraint_name} PRIMARY KEY (" + (
                            f"{_datetime_name}, " if _datetime_name else ""
                        ) + f"{primary_key_name})"
                    ),
                ])
            elif self.flavor in ('citus', 'postgresql', 'duckdb'):
                primary_queries.extend([
                    (
                        f"ALTER TABLE {_pipe_name}\n"
                        f"ALTER COLUMN {primary_key_name} SET NOT NULL"
                    ),
                    (
                        f"ALTER TABLE {_pipe_name}\n"
                        f"ADD CONSTRAINT {primary_key_constraint_name} PRIMARY KEY ({primary_key_name})"
                    ),
                ])
            else:
                primary_queries.extend([
                    (
                        f"ALTER TABLE {_pipe_name}\n"
                        f"ALTER COLUMN {primary_key_name} {primary_key_db_type} NOT NULL"
                    ),
                    (
                        f"ALTER TABLE {_pipe_name}\n"
                        f"ADD CONSTRAINT {primary_key_constraint_name} PRIMARY KEY ({primary_key_name})"
                    ),
                ])
        index_queries[primary_key] = primary_queries

    ### create id index
    if _id_name is not None:
        if self.flavor == 'timescaledb':
            ### Already created indices via create_hypertable.
            id_query = (
                None if (_id is not None and _create_space_partition)
                else (
                    f"CREATE INDEX IF NOT EXISTS {_id_index_name} ON {_pipe_name} ({_id_name})"
                    if _id is not None
                    else None
                )
            )
            pass
        else: ### mssql, sqlite, etc.
            id_query = f"CREATE INDEX {_id_index_name} ON {_pipe_name} ({_id_name})"

        if id_query is not None:
            index_queries[_id] = id_query if isinstance(id_query, list) else [id_query]

    ### Create indices for other labels in `pipe.columns`.
    other_index_names = {
        ix_key: ix_unquoted
        for ix_key, ix_unquoted in index_names.items()
        if ix_key not in ('datetime', 'id', 'primary') and ix_unquoted not in existing_ix_names
    }
    for ix_key, ix_unquoted in other_index_names.items():
        ix_name = sql_item_name(ix_unquoted, self.flavor, None)
        cols = indices[ix_key]
        if not isinstance(cols, (list, tuple)):
            cols = [cols]
        cols_names = [sql_item_name(col, self.flavor, None) for col in cols if col]
        if not cols_names:
            continue
        cols_names_str = ", ".join(cols_names)
        index_queries[ix_key] = [f"CREATE INDEX {ix_name} ON {_pipe_name} ({cols_names_str})"]

    indices_cols_str = ', '.join(
        list({
            sql_item_name(ix, self.flavor)
            for ix_key, ix in pipe.columns.items()
            if ix and ix in existing_cols_types
        })
    )
    coalesce_indices_cols_str = ', '.join(
        [
            (
                "COALESCE("
                + sql_item_name(ix, self.flavor)
                + ", "
                + get_null_replacement(existing_cols_types[ix], self.flavor)
                + ") "
            ) if ix_key != 'datetime' else (sql_item_name(ix, self.flavor))
            for ix_key, ix in pipe.columns.items()
            if ix and ix in existing_cols_types
        ]
    )
    unique_index_name = sql_item_name(pipe.target + '_unique_index', self.flavor)
    constraint_name = sql_item_name(pipe.target + '_constraint', self.flavor)
    add_constraint_query = (
        f"ALTER TABLE {_pipe_name} ADD CONSTRAINT {constraint_name} UNIQUE ({indices_cols_str})"
    )
    unique_index_cols_str = (
        indices_cols_str
        if self.flavor not in COALESCE_UNIQUE_INDEX_FLAVORS
        else coalesce_indices_cols_str
    )
    create_unique_index_query = (
        f"CREATE UNIQUE INDEX {unique_index_name} ON {_pipe_name} ({unique_index_cols_str})"
    )
    constraint_queries = [create_unique_index_query]
    if self.flavor != 'sqlite':
        constraint_queries.append(add_constraint_query)
    if upsert and indices_cols_str:
        index_queries[unique_index_name] = constraint_queries
    return index_queries


def get_drop_index_queries(
    self,
    pipe: mrsm.Pipe,
    debug: bool = False,
) -> Dict[str, List[str]]:
    """
    Return a dictionary mapping columns to a `DROP INDEX` or equivalent query.

    Parameters
    ----------
    pipe: mrsm.Pipe
        The pipe to which the queries will correspond.

    Returns
    -------
    A dictionary of column names mapping to lists of queries.
    """
    ### NOTE: Due to breaking changes within DuckDB, indices must be skipped.
    if self.flavor == 'duckdb':
        return {}
    if not pipe.exists(debug=debug):
        return {}
    from meerschaum.utils.sql import (
        sql_item_name,
        table_exists,
        hypertable_queries,
        DROP_IF_EXISTS_FLAVORS,
    )
    drop_queries = {}
    schema = self.get_pipe_schema(pipe)
    schema_prefix = (schema + '_') if schema else ''
    indices = {
        col: schema_prefix + ix
        for col, ix in pipe.get_indices().items()
    }
    pipe_name = sql_item_name(pipe.target, self.flavor, self.get_pipe_schema(pipe))
    pipe_name_no_schema = sql_item_name(pipe.target, self.flavor, None)

    if self.flavor not in hypertable_queries:
        is_hypertable = False
    else:
        is_hypertable_query = hypertable_queries[self.flavor].format(table_name=pipe_name)
        is_hypertable = self.value(is_hypertable_query, silent=True, debug=debug) is not None

    if_exists_str = "IF EXISTS" if self.flavor in DROP_IF_EXISTS_FLAVORS else ""
    if is_hypertable:
        nuke_queries = []
        temp_table = '_' + pipe.target + '_temp_migration'
        temp_table_name = sql_item_name(temp_table, self.flavor, self.get_pipe_schema(pipe))

        if table_exists(temp_table, self, schema=self.get_pipe_schema(pipe), debug=debug):
            nuke_queries.append(f"DROP TABLE {if_exists_str} {temp_table_name}")
        nuke_queries += [
            f"SELECT * INTO {temp_table_name} FROM {pipe_name}",
            f"DROP TABLE {if_exists_str} {pipe_name}",
            f"ALTER TABLE {temp_table_name} RENAME TO {pipe_name_no_schema}",
        ]
        nuke_ix_keys = ('datetime', 'id')
        nuked = False
        for ix_key in nuke_ix_keys:
            if ix_key in indices and not nuked:
                drop_queries[ix_key] = nuke_queries
                nuked = True

    drop_queries.update({
        ix_key: ["DROP INDEX " + sql_item_name(ix_unquoted, self.flavor, None)]
        for ix_key, ix_unquoted in indices.items()
        if ix_key not in drop_queries
    })
    return drop_queries


def delete_pipe(
    self,
    pipe: mrsm.Pipe,
    debug: bool = False,
) -> SuccessTuple:
    """
    Delete a Pipe's registration.
    """
    from meerschaum.utils.sql import sql_item_name
    from meerschaum.utils.debug import dprint
    from meerschaum.utils.packages import attempt_import
    sqlalchemy = attempt_import('sqlalchemy')

    if not pipe.id:
        return False, f"{pipe} is not registered."

    ### ensure pipes table exists
    from meerschaum.connectors.sql.tables import get_tables
    pipes_tbl = get_tables(mrsm_instance=self, create=(not pipe.temporary), debug=debug)['pipes']

    q = sqlalchemy.delete(pipes_tbl).where(pipes_tbl.c.pipe_id == pipe.id)
    if not self.exec(q, debug=debug):
        return False, f"Failed to delete registration for {pipe}."

    return True, "Success"


def get_pipe_data(
    self,
    pipe: mrsm.Pipe,
    select_columns: Optional[List[str]] = None,
    omit_columns: Optional[List[str]] = None,
    begin: Union[datetime, str, None] = None,
    end: Union[datetime, str, None] = None,
    params: Optional[Dict[str, Any]] = None,
    order: str = 'asc',
    limit: Optional[int] = None,
    begin_add_minutes: int = 0,
    end_add_minutes: int = 0,
    debug: bool = False,
    **kw: Any
) -> Union[pd.DataFrame, None]:
    """
    Access a pipe's data from the SQL instance.

    Parameters
    ----------
    pipe: mrsm.Pipe:
        The pipe to get data from.

    select_columns: Optional[List[str]], default None
        If provided, only select these given columns.
        Otherwise select all available columns (i.e. `SELECT *`).

    omit_columns: Optional[List[str]], default None
        If provided, remove these columns from the selection.

    begin: Union[datetime, str, None], default None
        If provided, get rows newer than or equal to this value.

    end: Union[datetime, str, None], default None
        If provided, get rows older than or equal to this value.

    params: Optional[Dict[str, Any]], default None
        Additional parameters to filter by.
        See `meerschaum.connectors.sql.build_where`.

    order: Optional[str], default 'asc'
        The selection order for all of the indices in the query.
        If `None`, omit the `ORDER BY` clause.

    limit: Optional[int], default None
        If specified, limit the number of rows retrieved to this value.

    begin_add_minutes: int, default 0
        The number of minutes to add to the `begin` datetime (i.e. `DATEADD`.

    end_add_minutes: int, default 0
        The number of minutes to add to the `end` datetime (i.e. `DATEADD`.

    chunksize: Optional[int], default -1
        The size of dataframe chunks to load into memory.

    debug: bool, default False
        Verbosity toggle.

    Returns
    -------
    A `pd.DataFrame` of the pipe's data.

    """
    import json
    from meerschaum.utils.sql import sql_item_name
    from meerschaum.utils.misc import parse_df_datetimes, to_pandas_dtype
    from meerschaum.utils.packages import import_pandas
    from meerschaum.utils.dtypes import (
        attempt_cast_to_numeric,
        attempt_cast_to_uuid,
        are_dtypes_equal,
    )
    from meerschaum.utils.dtypes.sql import get_pd_type_from_db_type
    pd = import_pandas()
    is_dask = 'dask' in pd.__name__

    cols_types = pipe.get_columns_types(debug=debug)
    dtypes = {
        **{
            p_col: to_pandas_dtype(p_typ)
            for p_col, p_typ in pipe.dtypes.items()
        },
        **{
            col: get_pd_type_from_db_type(typ)
            for col, typ in cols_types.items()
        }
    }
    if dtypes:
        if self.flavor == 'sqlite':
            if not pipe.columns.get('datetime', None):
                _dt = pipe.guess_datetime()
                dt = sql_item_name(_dt, self.flavor, None) if _dt else None
                is_guess = True
            else:
                _dt = pipe.get_columns('datetime')
                dt = sql_item_name(_dt, self.flavor, None)
                is_guess = False

            if _dt:
                dt_type = dtypes.get(_dt, 'object').lower()
                if 'datetime' not in dt_type:
                    if 'int' not in dt_type:
                        dtypes[_dt] = 'datetime64[ns, UTC]'
    existing_cols = pipe.get_columns_types(debug=debug)
    select_columns = (
        [
            col
            for col in existing_cols
            if col not in (omit_columns or [])
        ]
        if not select_columns
        else [
            col
            for col in select_columns
            if col in existing_cols
            and col not in (omit_columns or [])
        ]
    )
    if select_columns:
        dtypes = {col: typ for col, typ in dtypes.items() if col in select_columns}
    dtypes = {
        col: to_pandas_dtype(typ)
        for col, typ in dtypes.items()
        if col in select_columns and col not in (omit_columns or [])
    }
    query = self.get_pipe_data_query(
        pipe,
        select_columns=select_columns,
        omit_columns=omit_columns,
        begin=begin,
        end=end,
        params=params,
        order=order,
        limit=limit,
        begin_add_minutes=begin_add_minutes,
        end_add_minutes=end_add_minutes,
        debug=debug,
        **kw
    )

    if is_dask:
        index_col = pipe.columns.get('datetime', None)
        kw['index_col'] = index_col

    numeric_columns = [
        col
        for col, typ in pipe.dtypes.items()
        if typ == 'numeric' and col in dtypes
    ]
    uuid_columns = [
        col
        for col, typ in pipe.dtypes.items()
        if typ == 'uuid' and col in dtypes
    ]

    kw['coerce_float'] = kw.get('coerce_float', (len(numeric_columns) == 0))

    df = self.read(
        query,
        dtype=dtypes,
        debug=debug,
        **kw
    )
    for col in numeric_columns:
        if col not in df.columns:
            continue
        df[col] = df[col].apply(attempt_cast_to_numeric)

    for col in uuid_columns:
        if col not in df.columns:
            continue
        df[col] = df[col].apply(attempt_cast_to_uuid)

    if self.flavor == 'sqlite':
        ignore_dt_cols = [
            col
            for col, dtype in pipe.dtypes.items()
            if not are_dtypes_equal(str(dtype), 'datetime')
        ]
        ### NOTE: We have to consume the iterator here to ensure that datetimes are parsed correctly
        df = (
            parse_df_datetimes(
                df,
                ignore_cols=ignore_dt_cols,
                chunksize=kw.get('chunksize', None),
                strip_timezone=(pipe.tzinfo is None),
                debug=debug,
            ) if isinstance(df, pd.DataFrame) else (
                [
                    parse_df_datetimes(
                        c,
                        ignore_cols=ignore_dt_cols,
                        chunksize=kw.get('chunksize', None),
                        strip_timezone=(pipe.tzinfo is None),
                        debug=debug,
                    )
                    for c in df
                ]
            )
        )
        for col, typ in dtypes.items():
            if typ != 'json':
                continue
            df[col] = df[col].apply(lambda x: json.loads(x) if x is not None else x)
    return df


def get_pipe_data_query(
    self,
    pipe: mrsm.Pipe,
    select_columns: Optional[List[str]] = None,
    omit_columns: Optional[List[str]] = None,
    begin: Union[datetime, int, str, None] = None,
    end: Union[datetime, int, str, None] = None,
    params: Optional[Dict[str, Any]] = None,
    order: Optional[str] = 'asc',
    sort_datetimes: bool = False,
    limit: Optional[int] = None,
    begin_add_minutes: int = 0,
    end_add_minutes: int = 0,
    replace_nulls: Optional[str] = None,
    skip_existing_cols_check: bool = False,
    debug: bool = False,
    **kw: Any
) -> Union[str, None]:
    """
    Return the `SELECT` query for retrieving a pipe's data from its instance.

    Parameters
    ----------
    pipe: mrsm.Pipe:
        The pipe to get data from.

    select_columns: Optional[List[str]], default None
        If provided, only select these given columns.
        Otherwise select all available columns (i.e. `SELECT *`).

    omit_columns: Optional[List[str]], default None
        If provided, remove these columns from the selection.

    begin: Union[datetime, int, str, None], default None
        If provided, get rows newer than or equal to this value.

    end: Union[datetime, str, None], default None
        If provided, get rows older than or equal to this value.

    params: Optional[Dict[str, Any]], default None
        Additional parameters to filter by.
        See `meerschaum.connectors.sql.build_where`.

    order: Optional[str], default None
        The selection order for all of the indices in the query.
        If `None`, omit the `ORDER BY` clause.

    sort_datetimes: bool, default False
        Alias for `order='desc'`.

    limit: Optional[int], default None
        If specified, limit the number of rows retrieved to this value.

    begin_add_minutes: int, default 0
        The number of minutes to add to the `begin` datetime (i.e. `DATEADD`).

    end_add_minutes: int, default 0
        The number of minutes to add to the `end` datetime (i.e. `DATEADD`).

    chunksize: Optional[int], default -1
        The size of dataframe chunks to load into memory.

    replace_nulls: Optional[str], default None
        If provided, replace null values with this value.

    skip_existing_cols_check: bool, default False
        If `True`, do not verify that querying columns are actually on the table.

    debug: bool, default False
        Verbosity toggle.

    Returns
    -------
    A `SELECT` query to retrieve a pipe's data.
    """
    from meerschaum.utils.misc import items_str
    from meerschaum.utils.sql import sql_item_name, dateadd_str
    from meerschaum.utils.dtypes import coerce_timezone
    from meerschaum.utils.dtypes.sql import get_pd_type_from_db_type

    dt_col = pipe.columns.get('datetime', None)
    existing_cols = pipe.get_columns_types(debug=debug)
    dt_typ = get_pd_type_from_db_type(existing_cols[dt_col]) if dt_col in existing_cols else None
    select_columns = (
        [col for col in existing_cols]
        if not select_columns
        else [col for col in select_columns if col in existing_cols or skip_existing_cols_check]
    )
    if omit_columns:
        select_columns = [col for col in select_columns if col not in omit_columns]

    if order is None and sort_datetimes:
        order = 'desc'

    if begin == '':
        begin = pipe.get_sync_time(debug=debug)
        backtrack_interval = pipe.get_backtrack_interval(debug=debug)
        if begin is not None:
            begin -= backtrack_interval

    begin, end = pipe.parse_date_bounds(begin, end)
    if isinstance(begin, datetime) and dt_typ:
        begin = coerce_timezone(begin, strip_utc=('utc' not in dt_typ.lower()))
    if isinstance(end, datetime) and dt_typ:
        end = coerce_timezone(end, strip_utc=('utc' not in dt_typ.lower()))

    cols_names = [
        sql_item_name(col, self.flavor, None)
        for col in select_columns
    ]
    select_cols_str = (
        'SELECT\n    '
        + ',\n    '.join(
            [
                (
                    col_name
                    if not replace_nulls
                    else f"COALESCE(col_name, '{replace_nulls}') AS {col_name}"
                )
                for col_name in cols_names
            ]
        )
    ) if cols_names else 'SELECT *'
    pipe_table_name = sql_item_name(pipe.target, self.flavor, self.get_pipe_schema(pipe))
    query = f"{select_cols_str}\nFROM {pipe_table_name}"
    where = ""

    if order is not None:
        default_order = 'asc'
        if order not in ('asc', 'desc'):
            warn(f"Ignoring unsupported order '{order}'. Falling back to '{default_order}'.")
            order = default_order
        order = order.upper()

    if not pipe.columns.get('datetime', None):
        _dt = pipe.guess_datetime()
        dt = sql_item_name(_dt, self.flavor, None) if _dt else None
        is_guess = True
    else:
        _dt = pipe.get_columns('datetime')
        dt = sql_item_name(_dt, self.flavor, None)
        is_guess = False

    quoted_indices = {
        key: sql_item_name(val, self.flavor, None)
        for key, val in pipe.columns.items()
        if val in existing_cols or skip_existing_cols_check
    }

    if begin is not None or end is not None:
        if is_guess:
            if _dt is None:
                warn(
                    f"No datetime could be determined for {pipe}."
                    + "\n    Ignoring begin and end...",
                    stack=False,
                )
                begin, end = None, None
            else:
                warn(
                    f"A datetime wasn't specified for {pipe}.\n"
                    + f"    Using column \"{_dt}\" for datetime bounds...",
                    stack=False,
                )

    is_dt_bound = False
    if begin is not None and (_dt in existing_cols or skip_existing_cols_check):
        begin_da = dateadd_str(
            flavor=self.flavor,
            datepart='minute',
            number=begin_add_minutes,
            begin=begin,
        )
        where += f"{dt} >= {begin_da}" + (" AND " if end is not None else "")
        is_dt_bound = True

    if end is not None and (_dt in existing_cols or skip_existing_cols_check):
        if 'int' in str(type(end)).lower() and end == begin:
            end += 1
        end_da = dateadd_str(
            flavor=self.flavor,
            datepart='minute',
            number=end_add_minutes,
            begin=end
        )
        where += f"{dt} < {end_da}"
        is_dt_bound = True

    if params is not None:
        from meerschaum.utils.sql import build_where
        valid_params = {
            k: v
            for k, v in params.items()
            if k in existing_cols or skip_existing_cols_check
        }
        if valid_params:
            where += build_where(valid_params, self).replace(
                'WHERE', ('AND' if is_dt_bound else "")
            )

    if len(where) > 0:
        query += "\nWHERE " + where

    if order is not None:
        ### Sort by indices, starting with datetime.
        order_by = ""
        if quoted_indices:
            order_by += "\nORDER BY "
            if _dt and (_dt in existing_cols or skip_existing_cols_check):
                order_by += dt + ' ' + order + ','
            for key, quoted_col_name in quoted_indices.items():
                if dt == quoted_col_name:
                    continue
                order_by += ' ' + quoted_col_name + ' ' + order + ','
            order_by = order_by[:-1]

        query += order_by

    if isinstance(limit, int):
        if self.flavor == 'mssql':
            query = f'SELECT TOP {limit}\n' + query[len("SELECT "):]
        elif self.flavor == 'oracle':
            query = (
                f"SELECT * FROM (\n  {query}\n)\n"
                + f"WHERE ROWNUM IN ({', '.join([str(i) for i in range(1, limit+1)])})"
            )
        else:
            query += f"\nLIMIT {limit}"

    if debug:
        to_print = (
            []
            + ([f"begin='{begin}'"] if begin else [])
            + ([f"end='{end}'"] if end else [])
            + ([f"params={params}"] if params else [])
        )
        dprint("Getting pipe data with constraints: " + items_str(to_print, quotes=False))

    return query


def get_pipe_id(
    self,
    pipe: mrsm.Pipe,
    debug: bool = False,
) -> Any:
    """
    Get a Pipe's ID from the pipes table.
    """
    if pipe.temporary:
        return None
    from meerschaum.utils.packages import attempt_import
    import json
    sqlalchemy = attempt_import('sqlalchemy')
    from meerschaum.connectors.sql.tables import get_tables
    pipes_tbl = get_tables(mrsm_instance=self, create=(not pipe.temporary), debug=debug)['pipes']

    query = sqlalchemy.select(pipes_tbl.c.pipe_id).where(
        pipes_tbl.c.connector_keys == pipe.connector_keys
    ).where(
        pipes_tbl.c.metric_key == pipe.metric_key
    ).where(
        (pipes_tbl.c.location_key == pipe.location_key) if pipe.location_key is not None
        else pipes_tbl.c.location_key.is_(None)
    )
    _id = self.value(query, debug=debug, silent=pipe.temporary)
    if _id is not None:
        _id = int(_id)
    return _id


def get_pipe_attributes(
    self,
    pipe: mrsm.Pipe,
    debug: bool = False,
) -> Dict[str, Any]:
    """
    Get a Pipe's attributes dictionary.
    """
    from meerschaum.connectors.sql.tables import get_tables
    from meerschaum.utils.packages import attempt_import
    sqlalchemy = attempt_import('sqlalchemy')

    if pipe.get_id(debug=debug) is None:
        return {}

    pipes_tbl = get_tables(mrsm_instance=self, create=(not pipe.temporary), debug=debug)['pipes']

    try:
        q = sqlalchemy.select(pipes_tbl).where(pipes_tbl.c.pipe_id == pipe.id)
        if debug:
            dprint(q)
        attributes = (
            dict(self.exec(q, silent=True, debug=debug).first()._mapping)
            if self.flavor != 'duckdb'
            else self.read(q, debug=debug).to_dict(orient='records')[0]
        )
    except Exception as e:
        import traceback
        traceback.print_exc()
        warn(e)
        print(pipe)
        return {}

    ### handle non-PostgreSQL databases (text vs JSON)
    if not isinstance(attributes.get('parameters', None), dict):
        try:
            import json
            parameters = json.loads(attributes['parameters'])
            if isinstance(parameters, str) and parameters[0] == '{':
                parameters = json.loads(parameters)
            attributes['parameters'] = parameters
        except Exception as e:
            attributes['parameters'] = {}

    return attributes


def create_pipe_table_from_df(
    self,
    pipe: mrsm.Pipe,
    df: 'pd.DataFrame',
    debug: bool = False,
) -> mrsm.SuccessTuple:
    """
    Create a pipe's table from its configured dtypes and an incoming dataframe.
    """
    from meerschaum.utils.dataframe import get_json_cols, get_numeric_cols, get_uuid_cols
    from meerschaum.utils.sql import get_create_table_queries, sql_item_name
    primary_key = pipe.columns.get('primary', None)
    dt_col = pipe.columns.get('datetime', None)
    new_dtypes = {
        **{
            col: str(typ)
            for col, typ in df.dtypes.items()
        },
        **{
            col: str(df.dtypes.get(col, 'int'))
            for col_ix, col in pipe.columns.items()
            if col and col_ix != 'primary'
        },
        **{
            col: 'uuid'
            for col in get_uuid_cols(df)
        },
        **{
            col: 'json'
            for col in get_json_cols(df)
        },
        **{
            col: 'numeric'
            for col in get_numeric_cols(df)
        },
        **pipe.dtypes
    }
    autoincrement = (
        pipe.parameters.get('autoincrement', False)
        or (primary_key and primary_key not in new_dtypes)
    )
    if autoincrement:
        _ = new_dtypes.pop(primary_key, None)

    create_table_queries = get_create_table_queries(
        new_dtypes,
        pipe.target,
        self.flavor,
        schema=self.get_pipe_schema(pipe),
        primary_key=primary_key,
        datetime_column=dt_col,
    )
    success = all(
        self.exec_queries(create_table_queries, break_on_error=True, rollback=True, debug=debug)
    )
    target_name = sql_item_name(pipe.target, schema=self.get_pipe_schema(pipe), flavor=self.flavor)
    msg = (
        "Success"
        if success
        else f"Failed to create {target_name}."
    )
    return success, msg


def sync_pipe(
    self,
    pipe: mrsm.Pipe,
    df: Union[pd.DataFrame, str, Dict[Any, Any], None] = None,
    begin: Optional[datetime] = None,
    end: Optional[datetime] = None,
    chunksize: Optional[int] = -1,
    check_existing: bool = True,
    blocking: bool = True,
    debug: bool = False,
    _check_temporary_tables: bool = True,
    **kw: Any
) -> SuccessTuple:
    """
    Sync a pipe using a database connection.

    Parameters
    ----------
    pipe: mrsm.Pipe
        The Meerschaum Pipe instance into which to sync the data.

    df: Union[pandas.DataFrame, str, Dict[Any, Any], List[Dict[str, Any]]]
        An optional DataFrame or equivalent to sync into the pipe.
        Defaults to `None`.

    begin: Optional[datetime], default None
        Optionally specify the earliest datetime to search for data.
        Defaults to `None`.

    end: Optional[datetime], default None
        Optionally specify the latest datetime to search for data.
        Defaults to `None`.

    chunksize: Optional[int], default -1
        Specify the number of rows to sync per chunk.
        If `-1`, resort to system configuration (default is `900`).
        A `chunksize` of `None` will sync all rows in one transaction.
        Defaults to `-1`.

    check_existing: bool, default True
        If `True`, pull and diff with existing data from the pipe. Defaults to `True`.

    blocking: bool, default True
        If `True`, wait for sync to finish and return its result, otherwise asyncronously sync.
        Defaults to `True`.

    debug: bool, default False
        Verbosity toggle. Defaults to False.

    kw: Any
        Catch-all for keyword arguments.

    Returns
    -------
    A `SuccessTuple` of success (`bool`) and message (`str`).
    """
    from meerschaum.utils.packages import import_pandas
    from meerschaum.utils.sql import (
        get_update_queries,
        sql_item_name,
        update_queries,
        get_create_table_queries,
        get_reset_autoincrement_queries,
    )
    from meerschaum.utils.misc import generate_password
    from meerschaum.utils.dataframe import get_json_cols, get_numeric_cols, get_uuid_cols
    from meerschaum.utils.dtypes import are_dtypes_equal
    from meerschaum.utils.dtypes.sql import get_db_type_from_pd_type
    from meerschaum import Pipe
    import time
    import copy
    pd = import_pandas()
    if df is None:
        msg = f"DataFrame is None. Cannot sync {pipe}."
        warn(msg)
        return False, msg

    start = time.perf_counter()
    pipe_name = sql_item_name(pipe.target, self.flavor, schema=self.get_pipe_schema(pipe))

    if not pipe.temporary and not pipe.get_id(debug=debug):
        register_tuple = pipe.register(debug=debug)
        if not register_tuple[0]:
            return register_tuple

    ### df is the dataframe returned from the remote source
    ### via the connector
    if debug:
        dprint("Fetched data:\n" + str(df))

    if not isinstance(df, pd.DataFrame):
        df = pipe.enforce_dtypes(
            df,
            chunksize=chunksize,
            safe_copy=kw.get('safe_copy', False),
            debug=debug,
        )

    ### if table does not exist, create it with indices
    is_new = False
    if not pipe.exists(debug=debug):
        check_existing = False
        is_new = True
    else:
        ### Check for new columns.
        add_cols_queries = self.get_add_columns_queries(pipe, df, debug=debug)
        if add_cols_queries:
            _ = pipe.__dict__.pop('_columns_indices', None)
            _ = pipe.__dict__.pop('_columns_types', None)
            if not self.exec_queries(add_cols_queries, debug=debug):
                warn(f"Failed to add new columns to {pipe}.")

        alter_cols_queries = self.get_alter_columns_queries(pipe, df, debug=debug)
        if alter_cols_queries:
            _ = pipe.__dict__.pop('_columns_indices', None)
            _ = pipe.__dict__.pop('_columns_types', None)
            if not self.exec_queries(alter_cols_queries, debug=debug):
                warn(f"Failed to alter columns for {pipe}.")
            else:
                _ = pipe.infer_dtypes(persist=True)

    ### NOTE: Oracle SQL < 23c (2023) and SQLite does not support booleans,
    ### so infer bools and persist them to `dtypes`.
    if self.flavor in ('oracle', 'sqlite', 'mysql', 'mariadb'):
        pipe_dtypes = pipe.dtypes
        new_bool_cols = {
            col: 'bool[pyarrow]'
            for col, typ in df.dtypes.items()
            if col not in pipe_dtypes
            and are_dtypes_equal(str(typ), 'bool')
        }
        pipe_dtypes.update(new_bool_cols)
        pipe.dtypes = pipe_dtypes
        if new_bool_cols and not pipe.temporary:
            infer_bool_success, infer_bool_msg = pipe.edit(debug=debug)
            if not infer_bool_success:
                return infer_bool_success, infer_bool_msg

    upsert = pipe.parameters.get('upsert', False) and (self.flavor + '-upsert') in update_queries
    if upsert:
        check_existing = False
    kw['safe_copy'] = kw.get('safe_copy', False)

    unseen_df, update_df, delta_df = (
        pipe.filter_existing(
            df,
            chunksize=chunksize,
            debug=debug,
            **kw
        ) if check_existing else (df, None, df)
    )
    if upsert:
        unseen_df, update_df, delta_df = (df.head(0), df, df)

    if debug:
        dprint("Delta data:\n" + str(delta_df))
        dprint("Unseen data:\n" + str(unseen_df))
        if update_df is not None:
            dprint(("Update" if not upsert else "Upsert") + " data:\n" + str(update_df))

    if_exists = kw.get('if_exists', 'append')
    if 'if_exists' in kw:
        kw.pop('if_exists')
    if 'name' in kw:
        kw.pop('name')

    ### Insert new data into Pipe's table.
    unseen_kw = copy.deepcopy(kw)
    unseen_kw.update({
        'name': pipe.target,
        'if_exists': if_exists,
        'debug': debug,
        'as_dict': True,
        'chunksize': chunksize,
        'dtype': self.get_to_sql_dtype(pipe, unseen_df, update_dtypes=True),
        'schema': self.get_pipe_schema(pipe),
    })

    primary_key = pipe.columns.get('primary', None)
    autoincrement = (
        pipe.parameters.get('autoincrement', False)
        or (
            is_new
            and primary_key
            and primary_key
            not in pipe.dtypes
            and primary_key not in unseen_df.columns
        )
    )
    if autoincrement and autoincrement not in pipe.parameters:
        pipe.parameters['autoincrement'] = autoincrement
        edit_success, edit_msg = pipe.edit(debug=debug)
        if not edit_success:
            return edit_success, edit_msg

    autoincrement_needs_reset = False
    if autoincrement and primary_key:
        if primary_key not in df.columns:
            if unseen_df is not None and primary_key in unseen_df.columns:
                del unseen_df[primary_key]
            if update_df is not None and primary_key in update_df.columns:
                del update_df[primary_key]
            if delta_df is not None and primary_key in delta_df.columns:
                del delta_df[primary_key]
        elif unseen_df[primary_key].notnull().any():
            autoincrement_needs_reset = True

    if is_new:
        create_success, create_msg = self.create_pipe_table_from_df(
            pipe,
            unseen_df,
            debug=debug,
        )
        if not create_success:
            return create_success, create_msg

    do_identity_insert = bool(
        self.flavor in ('mssql',)
        and primary_key in unseen_df.columns
        and autoincrement
    )
    with self.engine.connect() as connection:
        with connection.begin():
            if do_identity_insert:
                identity_on_result = self.exec(
                    f"SET IDENTITY_INSERT {pipe_name} ON",
                    commit=False,
                    _connection=connection,
                    close=False,
                    debug=debug,
                )
                if identity_on_result is None:
                    return False, f"Could not enable identity inserts on {pipe}."

            stats = self.to_sql(
                unseen_df,
                _connection=connection,
                **unseen_kw
            )

            if do_identity_insert:
                identity_off_result = self.exec(
                    f"SET IDENTITY_INSERT {pipe_name} OFF",
                    commit=False,
                    _connection=connection,
                    close=False,
                    debug=debug,
                )
                if identity_off_result is None:
                    return False, f"Could not disable identity inserts on {pipe}."

    if is_new:
        if not self.create_indices(pipe, debug=debug):
            warn(f"Failed to create indices for {pipe}. Continuing...")

    if autoincrement_needs_reset:
        reset_autoincrement_queries = get_reset_autoincrement_queries(
            pipe.target,
            primary_key,
            self,
            schema=self.get_pipe_schema(pipe),
            debug=debug,
        )
        results = self.exec_queries(reset_autoincrement_queries, debug=debug)
        for result in results:
            if result is None:
                warn(f"Could not reset auto-incrementing primary key for {pipe}.", stack=False)

    if update_df is not None and len(update_df) > 0:
        transact_id = generate_password(3)
        temp_prefix = '##' if self.flavor != 'oracle' else '_'
        temp_target = temp_prefix + transact_id + '_' + pipe.target
        self._log_temporary_tables_creation(temp_target, create=(not pipe.temporary), debug=debug)
        temp_pipe = Pipe(
            pipe.connector_keys.replace(':', '_') + '_', pipe.metric_key, pipe.location_key,
            instance=pipe.instance_keys,
            columns={
                (ix_key if ix_key != 'primary' else 'primary_'): ix
                for ix_key, ix in pipe.columns.items()
                if ix and ix in update_df.columns
            },
            dtypes={
                col: typ
                for col, typ in pipe.dtypes.items()
                if col in update_df.columns
            },
            target=temp_target,
            temporary=True,
            parameters={
                'static': True,
                'schema': self.internal_schema,
                'hypertable': False,
                'autoincrement': False,
            },
        )
        temp_pipe.__dict__['_columns_types'] = {
            col: get_db_type_from_pd_type(
                pipe.dtypes.get(col, str(typ)),
                self.flavor,
            )
            for col, typ in update_df.dtypes.items()
        }
        now_ts = time.perf_counter()
        temp_pipe.__dict__['_columns_types_timestamp'] = now_ts
        temp_pipe.__dict__['_skip_check_indices'] = True
        temp_success, temp_msg = temp_pipe.sync(update_df, check_existing=False, debug=debug)
        if not temp_success:
            return temp_success, temp_msg
        existing_cols = pipe.get_columns_types(debug=debug)
        join_cols = [
            col
            for col_key, col in pipe.columns.items()
            if col and col in existing_cols
        ]
        update_queries = get_update_queries(
            pipe.target,
            temp_target,
            self,
            join_cols,
            upsert=upsert,
            schema=self.get_pipe_schema(pipe),
            patch_schema=self.internal_schema,
            datetime_col=pipe.columns.get('datetime', None),
            debug=debug,
        )
        update_success = all(
            self.exec_queries(update_queries, break_on_error=True, rollback=True, debug=debug)
        )
        self._log_temporary_tables_creation(
            temp_target,
            ready_to_drop=True,
            create=(not pipe.temporary),
            debug=debug,
        )
        if not update_success:
            warn(f"Failed to apply update to {pipe}.")

    stop = time.perf_counter()
    success = stats['success']
    if not success:
        return success, stats['msg']

    unseen_count = len(unseen_df.index) if unseen_df is not None else 0
    update_count = len(update_df.index) if update_df is not None else 0
    msg = (
        (
            f"Inserted {unseen_count}, "
            + f"updated {update_count} rows."
        )
        if not upsert
        else (
            f"Upserted {update_count} row"
            + ('s' if update_count != 1 else '')
            + "."
        )
    )
    if debug:
        msg = msg[:-1] + (
            f"\non table {sql_item_name(pipe.target, self.flavor, self.get_pipe_schema(pipe))}\n"
            + f"in {round(stop - start, 2)} seconds."
        )

    if _check_temporary_tables:
        drop_stale_success, drop_stale_msg = self._drop_old_temporary_tables(
            refresh=False, debug=debug
        )
        if not drop_stale_success:
            warn(drop_stale_msg)

    return success, msg


def sync_pipe_inplace(
    self,
    pipe: 'mrsm.Pipe',
    params: Optional[Dict[str, Any]] = None,
    begin: Optional[datetime] = None,
    end: Optional[datetime] = None,
    chunksize: Optional[int] = -1,
    check_existing: bool = True,
    debug: bool = False,
    **kw: Any
) -> SuccessTuple:
    """
    If a pipe's connector is the same as its instance connector,
    it's more efficient to sync the pipe in-place rather than reading data into Pandas.

    Parameters
    ----------
    pipe: mrsm.Pipe
        The pipe whose connector is the same as its instance.

    params: Optional[Dict[str, Any]], default None
        Optional params dictionary to build the `WHERE` clause.
        See `meerschaum.utils.sql.build_where`.

    begin: Optional[datetime], default None
        Optionally specify the earliest datetime to search for data.
        Defaults to `None`.

    end: Optional[datetime], default None
        Optionally specify the latest datetime to search for data.
        Defaults to `None`.

    chunksize: Optional[int], default -1
        Specify the number of rows to sync per chunk.
        If `-1`, resort to system configuration (default is `900`).
        A `chunksize` of `None` will sync all rows in one transaction.
        Defaults to `-1`.

    check_existing: bool, default True
        If `True`, pull and diff with existing data from the pipe.

    debug: bool, default False
        Verbosity toggle.

    Returns
    -------
    A SuccessTuple.
    """
    if self.flavor == 'duckdb':
        return pipe.sync(
            params=params,
            begin=begin,
            end=end,
            chunksize=chunksize,
            check_existing=check_existing,
            debug=debug,
            _inplace=False,
            **kw
        )
    from meerschaum.utils.sql import (
        sql_item_name,
        get_update_queries,
        get_null_replacement,
        get_create_table_queries,
        get_table_cols_types,
        session_execute,
        update_queries,
    )
    from meerschaum.utils.dtypes import coerce_timezone, are_dtypes_equal
    from meerschaum.utils.dtypes.sql import (
        get_pd_type_from_db_type,
    )
    from meerschaum.utils.misc import generate_password

    transact_id = generate_password(3)
    def get_temp_table_name(label: str) -> str:
        temp_prefix = '##' if self.flavor != 'oracle' else ''
        return temp_prefix + transact_id + '_' + label + '_' + pipe.target

    internal_schema = self.internal_schema
    temp_table_roots = ['backtrack', 'new', 'delta', 'joined', 'unseen', 'update']
    temp_tables = {
        table_root: get_temp_table_name(table_root)
        for table_root in temp_table_roots
    }
    temp_table_names = {
        table_root: sql_item_name(
            table_name_raw,
            self.flavor,
            internal_schema,
        )
        for table_root, table_name_raw in temp_tables.items()
    }
    metadef = self.get_pipe_metadef(
        pipe,
        params=params,
        begin=begin,
        end=end,
        check_existing=check_existing,
        debug=debug,
    )
    pipe_name = sql_item_name(pipe.target, self.flavor, self.get_pipe_schema(pipe))
    upsert = pipe.parameters.get('upsert', False) and f'{self.flavor}-upsert' in update_queries
    static = pipe.parameters.get('static', False)
    database = getattr(self, 'database', self.parse_uri(self.URI).get('database', None))
    primary_key = pipe.columns.get('primary', None)
    autoincrement = pipe.parameters.get('autoincrement', False)
    dt_col = pipe.columns.get('datetime', None)
    dt_col_name = sql_item_name(dt_col, self.flavor, None) if dt_col else None
    dt_typ = pipe.dtypes.get(dt_col, 'datetime64[ns, UTC]') if dt_col else None

    def clean_up_temp_tables(ready_to_drop: bool = False):
        log_success, log_msg = self._log_temporary_tables_creation(
            [
                table
                for table in temp_tables.values()
            ] if not upsert else [temp_tables['update']],
            ready_to_drop=ready_to_drop,
            create=(not pipe.temporary),
            debug=debug,
        )
        if not log_success:
            warn(log_msg)
        drop_stale_success, drop_stale_msg = self._drop_old_temporary_tables(
            refresh=False,
            debug=debug,
        )
        if not drop_stale_success:
            warn(drop_stale_msg)
        return drop_stale_success, drop_stale_msg

    sqlalchemy, sqlalchemy_orm = mrsm.attempt_import('sqlalchemy', 'sqlalchemy.orm')
    if not pipe.exists(debug=debug):
        create_pipe_queries = get_create_table_queries(
            metadef,
            pipe.target,
            self.flavor,
            schema=self.get_pipe_schema(pipe),
            primary_key=primary_key,
            autoincrement=autoincrement,
            datetime_column=dt_col,
        )
        result = self.exec_queries(create_pipe_queries, debug=debug)
        if result is None:
            _ = clean_up_temp_tables()
            return False, f"Could not insert new data into {pipe} from its SQL query definition."

        if not self.create_indices(pipe, debug=debug):
            warn(f"Failed to create indices for {pipe}. Continuing...")

        rowcount = pipe.get_rowcount(debug=debug)
        _ = clean_up_temp_tables()
        return True, f"Inserted {rowcount}, updated 0 rows."

    session = sqlalchemy_orm.Session(self.engine)
    connectable = session if self.flavor != 'duckdb' else self

    create_new_query = get_create_table_queries(
        metadef,
        temp_tables[('new') if not upsert else 'update'],
        self.flavor,
        schema=internal_schema,
    )[0]
    (create_new_success, create_new_msg), create_new_results = session_execute(
        session,
        create_new_query,
        with_results=True,
        debug=debug,
    )
    if not create_new_success:
        _ = clean_up_temp_tables()
        return create_new_success, create_new_msg
    new_count = create_new_results[0].rowcount if create_new_results else 0

    new_cols_types = get_table_cols_types(
        temp_tables[('new' if not upsert else 'update')],
        connectable=connectable,
        flavor=self.flavor,
        schema=internal_schema,
        database=database,
        debug=debug,
    ) if not static else pipe.get_columns_types(debug=debug)
    if not new_cols_types:
        return False, f"Failed to get new columns for {pipe}."

    new_cols = {
        str(col_name): get_pd_type_from_db_type(str(col_type))
        for col_name, col_type in new_cols_types.items()
    }
    new_cols_str = ', '.join([
        sql_item_name(col, self.flavor)
        for col in new_cols
    ])
    def get_col_typ(col: str, cols_types: Dict[str, str]) -> str:
        if self.flavor == 'oracle' and new_cols_types.get(col, '').lower() == 'char':
            return new_cols_types[col]
        return cols_types[col]

    add_cols_queries = self.get_add_columns_queries(pipe, new_cols, debug=debug)
    if add_cols_queries:
        _ = pipe.__dict__.pop('_columns_types', None)
        _ = pipe.__dict__.pop('_columns_indices', None)
        self.exec_queries(add_cols_queries, debug=debug)

    alter_cols_queries = self.get_alter_columns_queries(pipe, new_cols, debug=debug)
    if alter_cols_queries:
        _ = pipe.__dict__.pop('_columns_types', None)
        self.exec_queries(alter_cols_queries, debug=debug)

    insert_queries = [
        (
            f"INSERT INTO {pipe_name} ({new_cols_str})\n"
            + f"SELECT {new_cols_str}\nFROM {temp_table_names['new']}"
        )
    ] if not check_existing and not upsert else []

    new_queries = insert_queries
    new_success, new_msg = (
        session_execute(session, new_queries, debug=debug)
        if new_queries
        else (True, "Success")
    )
    if not new_success:
        _ = clean_up_temp_tables()
        return new_success, new_msg

    if not check_existing:
        session.commit()
        _ = clean_up_temp_tables()
        return True, f"Inserted {new_count}, updated 0 rows."

    (new_dt_bounds_success, new_dt_bounds_msg), new_dt_bounds_results = session_execute(
        session,
        [
            "SELECT\n"
            f"    MIN({dt_col_name}) AS {sql_item_name('min_dt', self.flavor)},\n"
            f"    MAX({dt_col_name}) AS {sql_item_name('max_dt', self.flavor)}\n"
            f"FROM {temp_table_names['new' if not upsert else 'update']}\n"
            f"WHERE {dt_col_name} IS NOT NULL"
        ],
        with_results=True,
        debug=debug,
    ) if not upsert else ((True, "Success"), None)
    if not new_dt_bounds_success:
        return (
            new_dt_bounds_success,
            f"Could not determine in-place datetime bounds:\n{new_dt_bounds_msg}"
        )

    if not upsert:
        begin, end = new_dt_bounds_results[0].fetchone()

    backtrack_def = self.get_pipe_data_query(
        pipe,
        begin=begin,
        end=end,
        begin_add_minutes=0,
        end_add_minutes=1,
        params=params,
        debug=debug,
        order=None,
    )
    create_backtrack_query = get_create_table_queries(
        backtrack_def,
        temp_tables['backtrack'],
        self.flavor,
        schema=internal_schema,
    )[0]
    (create_backtrack_success, create_backtrack_msg), create_backtrack_results = session_execute(
        session,
        create_backtrack_query,
        with_results=True,
        debug=debug,
    ) if not upsert else ((True, "Success"), None)

    if not create_backtrack_success:
        _ = clean_up_temp_tables()
        return create_backtrack_success, create_backtrack_msg

    backtrack_cols_types = get_table_cols_types(
        temp_tables['backtrack'],
        connectable=connectable,
        flavor=self.flavor,
        schema=internal_schema,
        database=database,
        debug=debug,
    ) if not (upsert or static) else new_cols_types

    common_cols = [col for col in new_cols if col in backtrack_cols_types]
    on_cols = {
        col: new_cols.get(col)
        for col_key, col in pipe.columns.items()
        if (
            col
            and
            col_key != 'value'
            and col in backtrack_cols_types
            and col in new_cols
        )
    }

    null_replace_new_cols_str = (
        ', '.join([
            f"COALESCE({temp_table_names['new']}.{sql_item_name(col, self.flavor, None)}, "
            + get_null_replacement(get_col_typ(col, new_cols), self.flavor)
            + ") AS "
            + sql_item_name(col, self.flavor, None)
            for col, typ in new_cols.items()
        ])
    )

    select_delta_query = (
        "SELECT\n"
        + null_replace_new_cols_str + "\n"
        + f"\nFROM {temp_table_names['new']}\n"
        + f"LEFT OUTER JOIN {temp_table_names['backtrack']}\nON\n"
        + '\nAND\n'.join([
            (
                f"COALESCE({temp_table_names['new']}."
                + sql_item_name(c, self.flavor, None)
                + ", "
                + get_null_replacement(get_col_typ(c, new_cols), self.flavor)
                + ") "
                + ' = '
                + f"COALESCE({temp_table_names['backtrack']}."
                + sql_item_name(c, self.flavor, None)
                + ", "
                + get_null_replacement(backtrack_cols_types[c], self.flavor)
                + ") "
            ) for c in common_cols
        ])
        + "\nWHERE\n"
        + '\nAND\n'.join([
            (
                f"{temp_table_names['backtrack']}." + sql_item_name(c, self.flavor, None) + ' IS NULL'
            ) for c in common_cols
        ])
    )
    create_delta_query = get_create_table_queries(
        select_delta_query,
        temp_tables['delta'],
        self.flavor,
        schema=internal_schema,
    )[0]
    create_delta_success, create_delta_msg = session_execute(
        session,
        create_delta_query,
        debug=debug,
    ) if not upsert else (True, "Success")
    if not create_delta_success:
        _ = clean_up_temp_tables()
        return create_delta_success, create_delta_msg

    delta_cols_types = get_table_cols_types(
        temp_tables['delta'],
        connectable=connectable,
        flavor=self.flavor,
        schema=internal_schema,
        database=database,
        debug=debug,
    ) if not (upsert or static) else new_cols_types

    ### This is a weird bug on SQLite.
    ### Sometimes the backtrack dtypes are all empty strings.
    if not all(delta_cols_types.values()):
        delta_cols_types = new_cols_types

    delta_cols = {
        col: get_pd_type_from_db_type(typ)
        for col, typ in delta_cols_types.items()
    }
    delta_cols_str = ', '.join([
        sql_item_name(col, self.flavor)
        for col in delta_cols
    ])

    select_joined_query = (
        "SELECT "
        + (', '.join([
            (
                f"{temp_table_names['delta']}." + sql_item_name(c, self.flavor, None)
                + " AS " + sql_item_name(c + '_delta', self.flavor, None)
            ) for c in delta_cols
        ]))
        + ", "
        + (', '.join([
            (
                f"{temp_table_names['backtrack']}." + sql_item_name(c, self.flavor, None)
                + " AS " + sql_item_name(c + '_backtrack', self.flavor, None)
            ) for c in backtrack_cols_types
        ]))
        + f"\nFROM {temp_table_names['delta']}\n"
        + f"LEFT OUTER JOIN {temp_table_names['backtrack']}\nON\n"
        + '\nAND\n'.join([
            (
                f"COALESCE({temp_table_names['delta']}." + sql_item_name(c, self.flavor, None)
                + ", "
                + get_null_replacement(
                    get_col_typ(c, on_cols),
                    self.flavor
                ) + ")"
                + ' = '
                + f"COALESCE({temp_table_names['backtrack']}." + sql_item_name(c, self.flavor, None)
                + ", "
                + get_null_replacement(
                    get_col_typ(c, on_cols),
                    self.flavor
                ) + ")"
            ) for c, typ in on_cols.items()
        ])
    )

    create_joined_query = get_create_table_queries(
        select_joined_query,
        temp_tables['joined'],
        self.flavor,
        schema=internal_schema,
    )[0]
    create_joined_success, create_joined_msg = session_execute(
        session,
        create_joined_query,
        debug=debug,
    ) if on_cols and not upsert else (True, "Success")
    if not create_joined_success:
        _ = clean_up_temp_tables()
        return create_joined_success, create_joined_msg

    select_unseen_query = (
        "SELECT "
        + (', '.join([
            (
                "CASE\n    WHEN " + sql_item_name(c + '_delta', self.flavor, None)
                + " != " + get_null_replacement(get_col_typ(c, delta_cols), self.flavor)
                + " THEN " + sql_item_name(c + '_delta', self.flavor, None)
                + "\n    ELSE NULL\nEND "
                + " AS " + sql_item_name(c, self.flavor, None)
            ) for c, typ in delta_cols.items()
        ]))
        + f"\nFROM {temp_table_names['joined']}\n"
        + "WHERE "
        + '\nAND\n'.join([
            (
                sql_item_name(c + '_backtrack', self.flavor, None) + ' IS NULL'
            ) for c in delta_cols
        ])
    )
    create_unseen_query = get_create_table_queries(
        select_unseen_query,
        temp_tables['unseen'],
        self.flavor,
        internal_schema,
    )[0]
    (create_unseen_success, create_unseen_msg), create_unseen_results = session_execute(
        session,
        create_unseen_query,
        with_results=True,
        debug=debug
    ) if not upsert else ((True, "Success"), None)
    if not create_unseen_success:
        _ = clean_up_temp_tables()
        return create_unseen_success, create_unseen_msg

    select_update_query = (
        "SELECT "
        + (', '.join([
            (
                "CASE\n    WHEN " + sql_item_name(c + '_delta', self.flavor, None)
                + " != " + get_null_replacement(get_col_typ(c, delta_cols), self.flavor)
                + " THEN " + sql_item_name(c + '_delta', self.flavor, None)
                + "\n    ELSE NULL\nEND "
                + " AS " + sql_item_name(c, self.flavor, None)
            ) for c, typ in delta_cols.items()
        ]))
        + f"\nFROM {temp_table_names['joined']}\n"
        + "WHERE "
        + '\nOR\n'.join([
            (
                sql_item_name(c + '_backtrack', self.flavor, None) + ' IS NOT NULL'
            ) for c in delta_cols
        ])
    )

    create_update_query = get_create_table_queries(
        select_update_query,
        temp_tables['update'],
        self.flavor,
        internal_schema,
    )[0]
    (create_update_success, create_update_msg), create_update_results = session_execute(
        session,
        create_update_query,
        with_results=True,
        debug=debug,
    ) if on_cols and not upsert else ((True, "Success"), [])
    apply_update_queries = (
        get_update_queries(
            pipe.target,
            temp_tables['update'],
            session,
            on_cols,
            upsert=upsert,
            schema=self.get_pipe_schema(pipe),
            patch_schema=internal_schema,
            datetime_col=pipe.columns.get('datetime', None),
            flavor=self.flavor,
            debug=debug,
        )
        if on_cols else []
    )

    apply_unseen_queries = [
        (
            f"INSERT INTO {pipe_name} ({delta_cols_str})\n"
            + f"SELECT {delta_cols_str}\nFROM "
            + (
                temp_table_names['unseen']
                if on_cols
                else temp_table_names['delta']
            )
        ),
    ]

    (apply_unseen_success, apply_unseen_msg), apply_unseen_results = session_execute(
        session,
        apply_unseen_queries,
        with_results=True,
        debug=debug,
    ) if not upsert else ((True, "Success"), None)
    if not apply_unseen_success:
        _ = clean_up_temp_tables()
        return apply_unseen_success, apply_unseen_msg
    unseen_count = apply_unseen_results[0].rowcount if apply_unseen_results else 0

    (apply_update_success, apply_update_msg), apply_update_results = session_execute(
        session,
        apply_update_queries,
        with_results=True,
        debug=debug,
    )
    if not apply_update_success:
        _ = clean_up_temp_tables()
        return apply_update_success, apply_update_msg
    update_count = apply_update_results[0].rowcount if apply_update_results else 0

    session.commit()

    msg = (
        f"Inserted {unseen_count}, updated {update_count} rows."
        if not upsert
        else f"Upserted {update_count} row" + ('s' if update_count != 1 else '') + "."
    )
    _ = clean_up_temp_tables(ready_to_drop=True)

    return True, msg


def get_sync_time(
    self,
    pipe: 'mrsm.Pipe',
    params: Optional[Dict[str, Any]] = None,
    newest: bool = True,
    debug: bool = False,
) -> Union[datetime, int, None]:
    """Get a Pipe's most recent datetime value.

    Parameters
    ----------
    pipe: mrsm.Pipe
        The pipe to get the sync time for.

    params: Optional[Dict[str, Any]], default None
        Optional params dictionary to build the `WHERE` clause.
        See `meerschaum.utils.sql.build_where`.

    newest: bool, default True
        If `True`, get the most recent datetime (honoring `params`).
        If `False`, get the oldest datetime (ASC instead of DESC).

    Returns
    -------
    A `datetime` object (or `int` if using an integer axis) if the pipe exists, otherwise `None`.
    """
    from meerschaum.utils.sql import sql_item_name, build_where
    table = sql_item_name(pipe.target, self.flavor, self.get_pipe_schema(pipe))

    dt_col = pipe.columns.get('datetime', None)
    dt_type = pipe.dtypes.get(dt_col, 'datetime64[ns, UTC]')
    if not dt_col:
        _dt = pipe.guess_datetime()
        dt = sql_item_name(_dt, self.flavor, None) if _dt else None
        is_guess = True
    else:
        _dt = dt_col
        dt = sql_item_name(_dt, self.flavor, None)
        is_guess = False

    if _dt is None:
        return None

    ASC_or_DESC = "DESC" if newest else "ASC"
    existing_cols = pipe.get_columns_types(debug=debug)
    valid_params = {}
    if params is not None:
        valid_params = {k: v for k, v in params.items() if k in existing_cols}

    ### If no bounds are provided for the datetime column,
    ### add IS NOT NULL to the WHERE clause.
    if _dt not in valid_params:
        valid_params[_dt] = '_None'
    where = "" if not valid_params else build_where(valid_params, self)
    q = f"SELECT {dt}\nFROM {table}{where}\nORDER BY {dt} {ASC_or_DESC}\nLIMIT 1"
    if self.flavor == 'mssql':
        q = f"SELECT TOP 1 {dt}\nFROM {table}{where}\nORDER BY {dt} {ASC_or_DESC}"
    elif self.flavor == 'oracle':
        q = (
            "SELECT * FROM (\n"
            + f"    SELECT {dt}\nFROM {table}{where}\n    ORDER BY {dt} {ASC_or_DESC}\n"
            + ") WHERE ROWNUM = 1"
        )

    try:
        db_time = self.value(q, silent=True, debug=debug)

        ### No datetime could be found.
        if db_time is None:
            return None
        ### sqlite returns str.
        if isinstance(db_time, str):
            from meerschaum.utils.packages import attempt_import
            dateutil_parser = attempt_import('dateutil.parser')
            st = dateutil_parser.parse(db_time)
        ### Do nothing if a datetime object is returned.
        elif isinstance(db_time, datetime):
            if hasattr(db_time, 'to_pydatetime'):
                st = db_time.to_pydatetime()
            else:
                st = db_time
        ### Sometimes the datetime is actually a date.
        elif isinstance(db_time, date):
            st = datetime.combine(db_time, datetime.min.time())
        ### Adding support for an integer datetime axis.
        elif 'int' in str(type(db_time)).lower():
            st = int(db_time)
        ### Convert pandas timestamp to Python datetime.
        else:
            st = db_time.to_pydatetime()

        sync_time = st

    except Exception as e:
        sync_time = None
        warn(str(e))

    return sync_time


def pipe_exists(
    self,
    pipe: mrsm.Pipe,
    debug: bool = False
) -> bool:
    """
    Check that a Pipe's table exists.

    Parameters
    ----------
    pipe: mrsm.Pipe:
        The pipe to check.

    debug: bool, default False
        Verbosity toggle.

    Returns
    -------
    A `bool` corresponding to whether a pipe's table exists.

    """
    from meerschaum.utils.sql import table_exists
    exists = table_exists(
        pipe.target,
        self,
        schema=self.get_pipe_schema(pipe),
        debug=debug,
    )
    if debug:
        from meerschaum.utils.debug import dprint
        dprint(f"{pipe} " + ('exists.' if exists else 'does not exist.'))
    return exists


def get_pipe_rowcount(
    self,
    pipe: mrsm.Pipe,
    begin: Union[datetime, int, None] = None,
    end: Union[datetime, int, None] = None,
    params: Optional[Dict[str, Any]] = None,
    remote: bool = False,
    debug: bool = False
) -> Union[int, None]:
    """
    Get the rowcount for a pipe in accordance with given parameters.

    Parameters
    ----------
    pipe: mrsm.Pipe
        The pipe to query with.

    begin: Union[datetime, int, None], default None
        The begin datetime value.

    end: Union[datetime, int, None], default None
        The end datetime value.

    params: Optional[Dict[str, Any]], default None
        See `meerschaum.utils.sql.build_where`.

    remote: bool, default False
        If `True`, get the rowcount for the remote table.

    debug: bool, default False
        Verbosity toggle.

    Returns
    -------
    An `int` for the number of rows if the `pipe` exists, otherwise `None`.

    """
    from meerschaum.utils.sql import dateadd_str, sql_item_name, wrap_query_with_cte
    from meerschaum.connectors.sql._fetch import get_pipe_query
    if remote:
        msg = f"'fetch:definition' must be an attribute of {pipe} to get a remote rowcount."
        if 'fetch' not in pipe.parameters:
            error(msg)
            return None
        if 'definition' not in pipe.parameters['fetch']:
            error(msg)
            return None

    _pipe_name = sql_item_name(pipe.target, self.flavor, self.get_pipe_schema(pipe))

    if not pipe.columns.get('datetime', None):
        _dt = pipe.guess_datetime()
        dt = sql_item_name(_dt, self.flavor, None) if _dt else None
        is_guess = True
    else:
        _dt = pipe.get_columns('datetime')
        dt = sql_item_name(_dt, self.flavor, None)
        is_guess = False

    if begin is not None or end is not None:
        if is_guess:
            if _dt is None:
                warn(
                    f"No datetime could be determined for {pipe}."
                    + "\n    Ignoring begin and end...",
                    stack=False,
                )
                begin, end = None, None
            else:
                warn(
                    f"A datetime wasn't specified for {pipe}.\n"
                    + f"    Using column \"{_dt}\" for datetime bounds...",
                    stack=False,
                )


    _datetime_name = sql_item_name(
        _dt,
        (
            pipe.instance_connector.flavor
            if not remote
            else pipe.connector.flavor
        ),
        None,
    )
    _cols_names = [
        sql_item_name(
            col,
            (
                pipe.instance_connector.flavor
                if not remote
                else pipe.connector.flavor
            ),
            None,
        )
        for col in set(
            (
                [_dt]
                if _dt
                else []
            )
            + (
                []
                if params is None
                else list(params.keys())
            )
        )
    ]
    if not _cols_names:
        _cols_names = ['*']

    src = (
        f"SELECT {', '.join(_cols_names)} FROM {_pipe_name}"
        if not remote
        else get_pipe_query(pipe)
    )
    parent_query = f"SELECT COUNT(*)\nFROM {sql_item_name('src', self.flavor)}"
    query = wrap_query_with_cte(src, parent_query, self.flavor)
    if begin is not None or end is not None:
        query += "\nWHERE"
    if begin is not None:
        query += f"""
        {dt} >= {dateadd_str(self.flavor, datepart='minute', number=0, begin=begin)}
        """
    if end is not None and begin is not None:
        query += "AND"
    if end is not None:
        query += f"""
        {dt} < {dateadd_str(self.flavor, datepart='minute', number=0, begin=end)}
        """
    if params is not None:
        from meerschaum.utils.sql import build_where
        existing_cols = pipe.get_columns_types(debug=debug)
        valid_params = {k: v for k, v in params.items() if k in existing_cols}
        if valid_params:
            query += build_where(valid_params, self).replace('WHERE', (
                'AND' if (begin is not None or end is not None)
                    else 'WHERE'
                )
            )

    result = self.value(query, debug=debug, silent=True)
    try:
        return int(result)
    except Exception as e:
        return None


def drop_pipe(
    self,
    pipe: mrsm.Pipe,
    debug: bool = False,
    **kw
) -> SuccessTuple:
    """
    Drop a pipe's tables but maintain its registration.

    Parameters
    ----------
    pipe: mrsm.Pipe
        The pipe to drop.

    Returns
    -------
    A `SuccessTuple` indicated success.
    """
    from meerschaum.utils.sql import table_exists, sql_item_name, DROP_IF_EXISTS_FLAVORS
    success = True
    target = pipe.target
    target_name = (
        sql_item_name(target, self.flavor, self.get_pipe_schema(pipe))
    )
    if table_exists(target, self, debug=debug):
        if_exists_str = "IF EXISTS" if self.flavor in DROP_IF_EXISTS_FLAVORS else ""
        success = self.exec(
            f"DROP TABLE {if_exists_str} {target_name}", silent=True, debug=debug
        ) is not None

    msg = "Success" if success else f"Failed to drop {pipe}."
    return success, msg


def clear_pipe(
    self,
    pipe: mrsm.Pipe,
    begin: Union[datetime, int, None] = None,
    end: Union[datetime, int, None] = None,
    params: Optional[Dict[str, Any]] = None,
    debug: bool = False,
    **kw
) -> SuccessTuple:
    """
    Delete a pipe's data within a bounded or unbounded interval without dropping the table.

    Parameters
    ----------
    pipe: mrsm.Pipe
        The pipe to clear.
        
    begin: Union[datetime, int, None], default None
        Beginning datetime. Inclusive.

    end: Union[datetime, int, None], default None
         Ending datetime. Exclusive.

    params: Optional[Dict[str, Any]], default None
         See `meerschaum.utils.sql.build_where`.

    """
    if not pipe.exists(debug=debug):
        return True, f"{pipe} does not exist, so nothing was cleared."

    from meerschaum.utils.sql import sql_item_name, build_where, dateadd_str
    pipe_name = sql_item_name(pipe.target, self.flavor, self.get_pipe_schema(pipe))

    if not pipe.columns.get('datetime', None):
        _dt = pipe.guess_datetime()
        dt_name = sql_item_name(_dt, self.flavor, None) if _dt else None
        is_guess = True
    else:
        _dt = pipe.get_columns('datetime')
        dt_name = sql_item_name(_dt, self.flavor, None)
        is_guess = False

    if begin is not None or end is not None:
        if is_guess:
            if _dt is None:
                warn(
                    f"No datetime could be determined for {pipe}."
                    + "\n    Ignoring datetime bounds...",
                    stack = False,
                )
                begin, end = None, None
            else:
                warn(
                    f"A datetime wasn't specified for {pipe}.\n"
                    + f"    Using column \"{_dt}\" for datetime bounds...",
                    stack = False,
                )

    valid_params = {}
    if params is not None:
        existing_cols = pipe.get_columns_types(debug=debug)
        valid_params = {k: v for k, v in params.items() if k in existing_cols}
    clear_query = (
        f"DELETE FROM {pipe_name}\nWHERE 1 = 1\n"
        + ('  AND ' + build_where(valid_params, self, with_where=False) if valid_params else '')
        + (
            f'  AND {dt_name} >= ' + dateadd_str(self.flavor, 'day', 0, begin)
            if begin is not None else ''
        ) + (
            f'  AND {dt_name} < ' + dateadd_str(self.flavor, 'day', 0, end)
            if end is not None else ''
        )
    )
    success = self.exec(clear_query, silent=True, debug=debug) is not None
    msg = "Success" if success else f"Failed to clear {pipe}."
    return success, msg


def get_pipe_table(
    self,
    pipe: mrsm.Pipe,
    debug: bool = False,
) -> Union['sqlalchemy.Table', None]:
    """
    Return the `sqlalchemy.Table` object for a `mrsm.Pipe`.

    Parameters
    ----------
    pipe: mrsm.Pipe:
        The pipe in question.

    Returns
    -------
    A `sqlalchemy.Table` object. 

    """
    from meerschaum.utils.sql import get_sqlalchemy_table
    if not pipe.exists(debug=debug):
        return None
    return get_sqlalchemy_table(
        pipe.target,
        connector=self,
        schema=self.get_pipe_schema(pipe),
        debug=debug,
        refresh=True,
    )


def get_pipe_columns_types(
    self,
    pipe: mrsm.Pipe,
    debug: bool = False,
) -> Dict[str, str]:
    """
    Get the pipe's columns and types.

    Parameters
    ----------
    pipe: mrsm.Pipe:
        The pipe to get the columns for.

    Returns
    -------
    A dictionary of columns names (`str`) and types (`str`).

    Examples
    --------
    >>> conn.get_pipe_columns_types(pipe)
    {
      'dt': 'TIMESTAMP WITHOUT TIMEZONE',
      'id': 'BIGINT',
      'val': 'DOUBLE PRECISION',
    }
    >>> 
    """
    from meerschaum.utils.sql import get_table_cols_types
    if not pipe.exists(debug=debug):
        return {}

    if self.flavor not in ('oracle', 'mysql', 'mariadb', 'sqlite'):
        return get_table_cols_types(
            pipe.target,
            self,
            flavor=self.flavor,
            schema=self.get_pipe_schema(pipe),
            debug=debug,
        )

    table_columns = {}
    try:
        pipe_table = self.get_pipe_table(pipe, debug=debug)
        if pipe_table is None:
            return {}
        for col in pipe_table.columns:
            table_columns[str(col.name)] = str(col.type)
    except Exception as e:
        import traceback
        traceback.print_exc()
        warn(e)
        table_columns = {}

    return table_columns


def get_pipe_columns_indices(
    self,
    pipe: mrsm.Pipe,
    debug: bool = False,
) -> Dict[str, List[Dict[str, str]]]:
    """
    Return a dictionary mapping columns to the indices created on those columns.

    Parameters
    ----------
    pipe: mrsm.Pipe
        The pipe to be queried against.

    Returns
    -------
    A dictionary mapping columns names to lists of dictionaries.
    The dictionaries in the lists contain the name and type of the indices.
    """
    if pipe.__dict__.get('_skip_check_indices', False):
        return {}
    from meerschaum.utils.sql import get_table_cols_indices
    return get_table_cols_indices(
        pipe.target,
        self,
        flavor=self.flavor,
        schema=self.get_pipe_schema(pipe),
        debug=debug,
    )


def get_add_columns_queries(
    self,
    pipe: mrsm.Pipe,
    df: Union[pd.DataFrame, Dict[str, str]],
    _is_db_types: bool = False,
    debug: bool = False,
) -> List[str]:
    """
    Add new null columns of the correct type to a table from a dataframe.

    Parameters
    ----------
    pipe: mrsm.Pipe
        The pipe to be altered.

    df: Union[pd.DataFrame, Dict[str, str]]
        The pandas DataFrame which contains new columns.
        If a dictionary is provided, assume it maps columns to Pandas data types.

    _is_db_types: bool, default False
        If `True`, assume `df` is a dictionary mapping columns to SQL native dtypes.

    Returns
    -------
    A list of the `ALTER TABLE` SQL query or queries to be executed on the provided connector.
    """
    if not pipe.exists(debug=debug):
        return []

    if pipe.parameters.get('static', False):
        return []

    from decimal import Decimal
    import copy
    from meerschaum.utils.sql import (
        sql_item_name,
        SINGLE_ALTER_TABLE_FLAVORS,
        get_table_cols_types,
    )
    from meerschaum.utils.dtypes.sql import (
        get_pd_type_from_db_type,
        get_db_type_from_pd_type,
    )
    from meerschaum.utils.misc import flatten_list
    table_obj = self.get_pipe_table(pipe, debug=debug)
    is_dask = 'dask' in df.__module__ if not isinstance(df, dict) else False
    if is_dask:
        df = df.partitions[0].compute()
    df_cols_types = (
        {
            col: str(typ)
            for col, typ in df.dtypes.items()
        }
        if not isinstance(df, dict)
        else copy.deepcopy(df)
    )
    if not isinstance(df, dict) and len(df.index) > 0:
        for col, typ in list(df_cols_types.items()):
            if typ != 'object':
                continue
            val = df.iloc[0][col]
            if isinstance(val, (dict, list)):
                df_cols_types[col] = 'json'
            elif isinstance(val, Decimal):
                df_cols_types[col] = 'numeric'
            elif isinstance(val, str):
                df_cols_types[col] = 'str'
    db_cols_types = {
        col: get_pd_type_from_db_type(str(typ.type))
        for col, typ in table_obj.columns.items()
    } if table_obj is not None else {
        col: get_pd_type_from_db_type(typ)
        for col, typ in get_table_cols_types(
            pipe.target,
            self,
            schema=self.get_pipe_schema(pipe),
            debug=debug,
        ).items()
    }
    new_cols = set(df_cols_types) - set(db_cols_types)
    if not new_cols:
        return []

    new_cols_types = {
        col: get_db_type_from_pd_type(
            df_cols_types[col],
            self.flavor
        ) for col in new_cols
    }

    alter_table_query = "ALTER TABLE " + sql_item_name(
        pipe.target, self.flavor, self.get_pipe_schema(pipe)
    )
    queries = []
    for col, typ in new_cols_types.items():
        add_col_query = (
            "\nADD "
            + sql_item_name(col, self.flavor, None)
            + " " + typ + ","
        )

        if self.flavor in SINGLE_ALTER_TABLE_FLAVORS:
            queries.append(alter_table_query + add_col_query[:-1])
        else:
            alter_table_query += add_col_query

    ### For most flavors, only one query is required.
    ### This covers SQLite which requires one query per column.
    if not queries:
        queries.append(alter_table_query[:-1])

    if self.flavor != 'duckdb':
        return queries

    ### NOTE: For DuckDB, we must drop and rebuild the indices.
    drop_index_queries = list(flatten_list(
        [q for ix, q in self.get_drop_index_queries(pipe, debug=debug).items()]
    ))
    create_index_queries = list(flatten_list(
        [q for ix, q in self.get_create_index_queries(pipe, debug=debug).items()]
    ))

    return drop_index_queries + queries + create_index_queries


def get_alter_columns_queries(
    self,
    pipe: mrsm.Pipe,
    df: Union[pd.DataFrame, Dict[str, str]],
    debug: bool = False,
) -> List[str]:
    """
    If we encounter a column of a different type, set the entire column to text.
    If the altered columns are numeric, alter to numeric instead.

    Parameters
    ----------
    pipe: mrsm.Pipe
        The pipe to be altered.

    df: Union[pd.DataFrame, Dict[str, str]]
        The pandas DataFrame which may contain altered columns.
        If a dict is provided, assume it maps columns to Pandas data types.

    Returns
    -------
    A list of the `ALTER TABLE` SQL query or queries to be executed on the provided connector.
    """
    if not pipe.exists(debug=debug):
        return []
    if pipe.static:
        return
    from meerschaum.utils.sql import sql_item_name, DROP_IF_EXISTS_FLAVORS, get_table_cols_types
    from meerschaum.utils.dataframe import get_numeric_cols
    from meerschaum.utils.dtypes import are_dtypes_equal
    from meerschaum.utils.dtypes.sql import (
        get_pd_type_from_db_type,
        get_db_type_from_pd_type,
    )
    from meerschaum.utils.misc import flatten_list, generate_password, items_str
    table_obj = self.get_pipe_table(pipe, debug=debug)
    target = pipe.target
    session_id = generate_password(3)
    numeric_cols = (
        get_numeric_cols(df)
        if not isinstance(df, dict)
        else [
            col
            for col, typ in df.items()
            if typ == 'numeric'
        ]
    )
    df_cols_types = (
        {
            col: str(typ)
            for col, typ in df.dtypes.items()
        }
        if not isinstance(df, dict)
        else df
    )
    db_cols_types = {
        col: get_pd_type_from_db_type(str(typ.type))
        for col, typ in table_obj.columns.items()
    } if table_obj is not None else {
        col: get_pd_type_from_db_type(typ)
        for col, typ in get_table_cols_types(
            pipe.target,
            self,
            schema=self.get_pipe_schema(pipe),
            debug=debug,
        ).items()
    }
    pipe_bool_cols = [col for col, typ in pipe.dtypes.items() if are_dtypes_equal(str(typ), 'bool')]
    pd_db_df_aliases = {
        'int': 'bool',
        'float': 'bool',
        'numeric': 'bool',
        'guid': 'object',
    }
    if self.flavor == 'oracle':
        pd_db_df_aliases['int'] = 'numeric'

    altered_cols = {
        col: (db_cols_types.get(col, 'object'), typ)
        for col, typ in df_cols_types.items()
        if not are_dtypes_equal(typ, db_cols_types.get(col, 'object').lower())
        and not are_dtypes_equal(db_cols_types.get(col, 'object'), 'string')
    }

    ### NOTE: Sometimes bools are coerced into ints or floats.
    altered_cols_to_ignore = set()
    for col, (db_typ, df_typ) in altered_cols.items():
        for db_alias, df_alias in pd_db_df_aliases.items():
            if db_alias in db_typ.lower() and df_alias in df_typ.lower():
                altered_cols_to_ignore.add(col)

    ### Oracle's bool handling sometimes mixes NUMBER and INT.
    for bool_col in pipe_bool_cols:
        if bool_col not in altered_cols:
            continue
        db_is_bool_compatible = (
            are_dtypes_equal('int', altered_cols[bool_col][0])
            or are_dtypes_equal('float', altered_cols[bool_col][0])
            or are_dtypes_equal('numeric', altered_cols[bool_col][0])
            or are_dtypes_equal('bool', altered_cols[bool_col][0])
        )
        df_is_bool_compatible = (
            are_dtypes_equal('int', altered_cols[bool_col][1])
            or are_dtypes_equal('float', altered_cols[bool_col][1])
            or are_dtypes_equal('numeric', altered_cols[bool_col][1])
            or are_dtypes_equal('bool', altered_cols[bool_col][1])
        )
        if db_is_bool_compatible and df_is_bool_compatible:
            altered_cols_to_ignore.add(bool_col)

    for col in altered_cols_to_ignore:
        _ = altered_cols.pop(col, None)
    if not altered_cols:
        return []

    if numeric_cols:
        pipe.dtypes.update({col: 'numeric' for col in numeric_cols})
        edit_success, edit_msg = pipe.edit(debug=debug)
        if not edit_success:
            warn(
                f"Failed to update dtypes for numeric columns {items_str(numeric_cols)}:\n"
                + f"{edit_msg}"
            )
    else:
        numeric_cols.extend([col for col, typ in pipe.dtypes.items() if typ == 'numeric'])

    numeric_type = get_db_type_from_pd_type('numeric', self.flavor, as_sqlalchemy=False)
    text_type = get_db_type_from_pd_type('str', self.flavor, as_sqlalchemy=False)
    altered_cols_types = {
        col: (
            numeric_type
            if col in numeric_cols
            else text_type
        )
        for col, (db_typ, typ) in altered_cols.items()
    }

    if self.flavor == 'sqlite':
        temp_table_name = '-' + session_id + '_' + target
        rename_query = (
            "ALTER TABLE "
            + sql_item_name(target, self.flavor, self.get_pipe_schema(pipe))
            + " RENAME TO "
            + sql_item_name(temp_table_name, self.flavor, None)
        )
        create_query = (
            "CREATE TABLE "
            + sql_item_name(target, self.flavor, self.get_pipe_schema(pipe))
            + " (\n"
        )
        for col_name, col_obj in table_obj.columns.items():
            create_query += (
                sql_item_name(col_name, self.flavor, None)
                + " "
                + (
                    str(col_obj.type)
                    if col_name not in altered_cols
                    else altered_cols_types[col_name]
                )
                + ",\n"
            )
        create_query = create_query[:-2] + "\n)"

        insert_query = (
            "INSERT INTO "
            + sql_item_name(target, self.flavor, self.get_pipe_schema(pipe))
            + ' ('
            + ', '.join([
                sql_item_name(col_name, self.flavor, None)
                for col_name, _ in table_obj.columns.items()
            ])
            + ')'
            + "\nSELECT\n"
        )
        for col_name, col_obj in table_obj.columns.items():
            new_col_str = (
                sql_item_name(col_name, self.flavor, None)
                if col_name not in altered_cols
                else (
                    "CAST("
                    + sql_item_name(col_name, self.flavor, None)
                    + " AS "
                    + altered_cols_types[col_name]
                    + ")"
                )
            )
            insert_query += new_col_str + ",\n"
        insert_query = insert_query[:-2] + (
            f"\nFROM {sql_item_name(temp_table_name, self.flavor, self.get_pipe_schema(pipe))}"
        )

        if_exists_str = "IF EXISTS" if self.flavor in DROP_IF_EXISTS_FLAVORS else ""

        drop_query = f"DROP TABLE {if_exists_str}" + sql_item_name(
            temp_table_name, self.flavor, self.get_pipe_schema(pipe)
        )
        return [
            rename_query,
            create_query,
            insert_query,
            drop_query,
        ]

    queries = []
    if self.flavor == 'oracle':
        for col, typ in altered_cols_types.items():
            add_query = (
                "ALTER TABLE "
                + sql_item_name(target, self.flavor, self.get_pipe_schema(pipe))
                + "\nADD " + sql_item_name(col + '_temp', self.flavor, None)
                + " " + typ
            )
            queries.append(add_query)

        for col, typ in altered_cols_types.items():
            populate_temp_query = (
                "UPDATE "
                + sql_item_name(target, self.flavor, self.get_pipe_schema(pipe))
                + "\nSET " + sql_item_name(col + '_temp', self.flavor, None)
                + ' = ' + sql_item_name(col, self.flavor, None)
            )
            queries.append(populate_temp_query)

        for col, typ in altered_cols_types.items():
            set_old_cols_to_null_query = (
                "UPDATE "
                + sql_item_name(target, self.flavor, self.get_pipe_schema(pipe))
                + "\nSET " + sql_item_name(col, self.flavor, None)
                + ' = NULL'
            )
            queries.append(set_old_cols_to_null_query)

        for col, typ in altered_cols_types.items():
            alter_type_query = (
                "ALTER TABLE "
                + sql_item_name(target, self.flavor, self.get_pipe_schema(pipe))
                + "\nMODIFY " + sql_item_name(col, self.flavor, None) + ' '
                + typ
            )
            queries.append(alter_type_query)

        for col, typ in altered_cols_types.items():
            set_old_to_temp_query = (
                "UPDATE "
                + sql_item_name(target, self.flavor, self.get_pipe_schema(pipe))
                + "\nSET " + sql_item_name(col, self.flavor, None)
                + ' = ' + sql_item_name(col + '_temp', self.flavor, None)
            )
            queries.append(set_old_to_temp_query)

        for col, typ in altered_cols_types.items():
            drop_temp_query = (
                "ALTER TABLE "
                + sql_item_name(target, self.flavor, self.get_pipe_schema(pipe))
                + "\nDROP COLUMN " + sql_item_name(col + '_temp', self.flavor, None)
            )
            queries.append(drop_temp_query)

        return queries

    query = "ALTER TABLE " + sql_item_name(target, self.flavor, self.get_pipe_schema(pipe))
    for col, typ in altered_cols_types.items():
        alter_col_prefix = (
            'ALTER' if self.flavor not in ('mysql', 'mariadb', 'oracle')
            else 'MODIFY'
        )
        type_prefix = (
            '' if self.flavor in ('mssql', 'mariadb', 'mysql')
            else 'TYPE '
        )
        column_str = 'COLUMN' if self.flavor != 'oracle' else ''
        query += (
            f"\n{alter_col_prefix} {column_str} "
            + sql_item_name(col, self.flavor, None)
            + " " + type_prefix + typ + ","
        )

    query = query[:-1]
    queries.append(query)
    if self.flavor != 'duckdb':
        return queries

    drop_index_queries = list(flatten_list(
        [q for ix, q in self.get_drop_index_queries(pipe, debug=debug).items()]
    ))
    create_index_queries = list(flatten_list(
        [q for ix, q in self.get_create_index_queries(pipe, debug=debug).items()]
    ))

    return drop_index_queries + queries + create_index_queries


def get_to_sql_dtype(
    self,
    pipe: 'mrsm.Pipe',
    df: 'pd.DataFrame',
    update_dtypes: bool = True,
) -> Dict[str, 'sqlalchemy.sql.visitors.TraversibleType']:
    """
    Given a pipe and DataFrame, return the `dtype` dictionary for `to_sql()`.

    Parameters
    ----------
    pipe: mrsm.Pipe
        The pipe which may contain a `dtypes` parameter.

    df: pd.DataFrame
        The DataFrame to be pushed via `to_sql()`.

    update_dtypes: bool, default True
        If `True`, patch the pipe's dtypes onto the DataFrame's dtypes.

    Returns
    -------
    A dictionary with `sqlalchemy` datatypes.

    Examples
    --------
    >>> import pandas as pd
    >>> import meerschaum as mrsm
    >>> 
    >>> conn = mrsm.get_connector('sql:memory')
    >>> df = pd.DataFrame([{'a': {'b': 1}}])
    >>> pipe = mrsm.Pipe('a', 'b', dtypes={'a': 'json'})
    >>> get_to_sql_dtype(pipe, df)
    {'a': <class 'sqlalchemy.sql.sqltypes.JSON'>}
    """
    from meerschaum.utils.dataframe import get_json_cols, get_numeric_cols, get_uuid_cols
    from meerschaum.utils.dtypes.sql import get_db_type_from_pd_type
    df_dtypes = {
        col: str(typ)
        for col, typ in df.dtypes.items()
    }
    json_cols = get_json_cols(df)
    numeric_cols = get_numeric_cols(df)
    uuid_cols = get_uuid_cols(df)
    df_dtypes.update({col: 'json' for col in json_cols})
    df_dtypes.update({col: 'numeric' for col in numeric_cols})
    df_dtypes.update({col: 'uuid' for col in uuid_cols})
    if update_dtypes:
        df_dtypes.update(pipe.dtypes)
    return {
        col: get_db_type_from_pd_type(typ, self.flavor, as_sqlalchemy=True)
        for col, typ in df_dtypes.items()
    }


def deduplicate_pipe(
    self,
    pipe: mrsm.Pipe,
    begin: Union[datetime, int, None] = None,
    end: Union[datetime, int, None] = None,
    params: Optional[Dict[str, Any]] = None,
    debug: bool = False,
    **kwargs: Any
) -> SuccessTuple:
    """
    Delete duplicate values within a pipe's table.

    Parameters
    ----------
    pipe: mrsm.Pipe
        The pipe whose table to deduplicate.

    begin: Union[datetime, int, None], default None
        If provided, only deduplicate values greater than or equal to this value.

    end: Union[datetime, int, None], default None
        If provided, only deduplicate values less than this value.

    params: Optional[Dict[str, Any]], default None
        If provided, further limit deduplication to values which match this query dictionary.

    debug: bool, default False
        Verbosity toggle.

    Returns
    -------
    A `SuccessTuple` indicating success.
    """
    from meerschaum.utils.sql import (
        sql_item_name,
        NO_CTE_FLAVORS,
        get_rename_table_queries,
        NO_SELECT_INTO_FLAVORS,
        DROP_IF_EXISTS_FLAVORS,
        get_create_table_query,
        format_cte_subquery,
        get_null_replacement,
    )
    from meerschaum.utils.misc import generate_password, flatten_list

    pipe_table_name = sql_item_name(pipe.target, self.flavor, self.get_pipe_schema(pipe))

    if not pipe.exists(debug=debug):
        return False, f"Table {pipe_table_name} does not exist."

    ### TODO: Handle deleting duplicates without a datetime axis.
    dt_col = pipe.columns.get('datetime', None)
    dt_col_name = sql_item_name(dt_col, self.flavor, None)
    cols_types = pipe.get_columns_types(debug=debug)
    existing_cols = pipe.get_columns_types(debug=debug)

    get_rowcount_query = f"SELECT COUNT(*) FROM {pipe_table_name}"
    old_rowcount = self.value(get_rowcount_query, debug=debug)
    if old_rowcount is None:
        return False, f"Failed to get rowcount for table {pipe_table_name}."

    ### Non-datetime indices that in fact exist.
    indices = [
        col
        for key, col in pipe.columns.items()
        if col and col != dt_col and col in cols_types
    ]
    indices_names = [sql_item_name(index_col, self.flavor, None) for index_col in indices]
    existing_cols_names = [sql_item_name(col, self.flavor, None) for col in existing_cols]
    duplicates_cte_name = sql_item_name('dups', self.flavor, None)
    duplicate_row_number_name = sql_item_name('dup_row_num', self.flavor, None)
    previous_row_number_name = sql_item_name('prev_row_num', self.flavor, None)

    index_list_str = (
        sql_item_name(dt_col, self.flavor, None)
        if dt_col
        else ''
    )
    index_list_str_ordered = (
        (
            sql_item_name(dt_col, self.flavor, None) + " DESC"
        )
        if dt_col
        else ''
    )
    if indices:
        index_list_str += ', ' + ', '.join(indices_names)
        index_list_str_ordered += ', ' + ', '.join(indices_names)
    if index_list_str.startswith(','):
        index_list_str = index_list_str.lstrip(',').lstrip()
    if index_list_str_ordered.startswith(','):
        index_list_str_ordered = index_list_str_ordered.lstrip(',').lstrip()

    cols_list_str = ', '.join(existing_cols_names)

    try:
        ### NOTE: MySQL 5 and below does not support window functions (ROW_NUMBER()).
        is_old_mysql = (
            self.flavor in ('mysql', 'mariadb')
            and
            int(self.db_version.split('.')[0]) < 8
        )
    except Exception as e:
        is_old_mysql = False

    src_query = f"""
        SELECT
            {cols_list_str},
            ROW_NUMBER() OVER (
                PARTITION BY
                {index_list_str}
                ORDER BY {index_list_str_ordered}
            ) AS {duplicate_row_number_name}
        FROM {pipe_table_name}
    """
    duplicates_cte_subquery = format_cte_subquery(
        src_query,
        self.flavor,
        sub_name = 'src',
        cols_to_select = cols_list_str,
    ) + f"""
        WHERE {duplicate_row_number_name} = 1
        """
    old_mysql_query = (
        f"""
        SELECT
            {index_list_str}
        FROM (
          SELECT
            {index_list_str},
            IF(
                @{previous_row_number_name} <> {index_list_str.replace(', ', ' + ')},
                @{duplicate_row_number_name} := 0,
                @{duplicate_row_number_name}
            ),
            @{previous_row_number_name} := {index_list_str.replace(', ', ' + ')},
            @{duplicate_row_number_name} := @{duplicate_row_number_name} + 1 AS """
        + f"""{duplicate_row_number_name}
          FROM
            {pipe_table_name},
            (
                SELECT @{duplicate_row_number_name} := 0
            ) AS {duplicate_row_number_name},
            (
                SELECT @{previous_row_number_name} := '{get_null_replacement('str', 'mysql')}'
            ) AS {previous_row_number_name}
          ORDER BY {index_list_str_ordered}
        ) AS t
        WHERE {duplicate_row_number_name} = 1
        """
    )
    if is_old_mysql:
        duplicates_cte_subquery = old_mysql_query

    session_id = generate_password(3)

    dedup_table = '-' + session_id + f'_dedup_{pipe.target}'
    temp_old_table = '-' + session_id + f"_old_{pipe.target}"

    dedup_table_name = sql_item_name(dedup_table, self.flavor, self.get_pipe_schema(pipe))
    temp_old_table_name = sql_item_name(temp_old_table, self.flavor, self.get_pipe_schema(pipe))

    create_temporary_table_query = get_create_table_query(
        duplicates_cte_subquery,
        dedup_table,
        self.flavor,
    ) + f"""
    ORDER BY {index_list_str_ordered}
    """
    if_exists_str = "IF EXISTS" if self.flavor in DROP_IF_EXISTS_FLAVORS else ""
    alter_queries = flatten_list([
        get_rename_table_queries(
            pipe.target, temp_old_table, self.flavor, schema=self.get_pipe_schema(pipe)
        ),
        get_rename_table_queries(
            dedup_table, pipe.target, self.flavor, schema=self.get_pipe_schema(pipe)
        ),
        f"""
        DROP TABLE {if_exists_str} {temp_old_table_name}
        """,
    ])

    create_temporary_result = self.execute(create_temporary_table_query, debug=debug)
    if create_temporary_result is None:
        return False, f"Failed to deduplicate table {pipe_table_name}."

    results = self.exec_queries(
        alter_queries,
        break_on_error=True,
        rollback=True,
        debug=debug,
    )

    fail_query = None
    for result, query in zip(results, alter_queries):
        if result is None:
            fail_query = query
            break
    success = fail_query is None

    new_rowcount = (
        self.value(get_rowcount_query, debug=debug)
        if success
        else None
    )

    msg = (
        (
            f"Successfully deduplicated table {pipe_table_name}"
            + (
                f"\nfrom {old_rowcount} to {new_rowcount} rows"
                if old_rowcount != new_rowcount
                else ''
            )
            + '.'
        )
        if success
        else f"Failed to execute query:\n{fail_query}"
    )
    return success, msg


def get_pipe_schema(self, pipe: mrsm.Pipe) -> Union[str, None]:
    """
    Return the schema to use for this pipe.
    First check `pipe.parameters['schema']`, then check `self.schema`.

    Parameters
    ----------
    pipe: mrsm.Pipe
        The pipe which may contain a configured schema.

    Returns
    -------
    A schema string or `None` if nothing is configured.
    """
    return pipe.parameters.get('schema', self.schema)
