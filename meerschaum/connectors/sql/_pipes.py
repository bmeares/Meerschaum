#! /usr/bin/env python
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

"""
Interact with Pipes metadata via SQLConnector.
"""
from __future__ import annotations
from meerschaum.utils.typing import (
    Union, Any, SuccessTuple, Tuple, Dict, Optional, List
)

def register_pipe(
        self,
        pipe: meerschaum.Pipe,
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
    pipes = get_tables(mrsm_instance=self, create=(not pipe.temporary), debug=debug)['pipes']

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
    query = sqlalchemy.insert(pipes).values(**values)
    result = self.exec(query, debug=debug)
    if result is None:
        return False, f"Failed to register {pipe}."
    return True, f"Successfully registered {pipe}."


def edit_pipe(
        self,
        pipe : meerschaum.Pipe = None,
        patch: bool = False,
        debug: bool = False,
        **kw : Any
    ) -> SuccessTuple:
    """
    Persist a Pipe's parameters to its database.

    Parameters
    ----------
    pipe: meerschaum.Pipe, default None
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
    pipes = get_tables(mrsm_instance=self, create=(not pipe.temporary), debug=debug)['pipes']

    import json
    sqlalchemy = attempt_import('sqlalchemy')

    values = {
        'parameters': (
            json.dumps(parameters)
            if self.flavor not in json_flavors
            else parameters
        ),
    }
    q = sqlalchemy.update(pipes).values(**values).where(
        pipes.c.pipe_id == pipe.id
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
    from meerschaum.utils.warnings import error
    from meerschaum.utils.debug import dprint
    from meerschaum.utils.packages import attempt_import
    from meerschaum.utils.misc import separate_negation_values
    from meerschaum.utils.sql import OMIT_NULLSFIRST_FLAVORS, table_exists
    from meerschaum.config.static import STATIC_CONFIG
    sqlalchemy = attempt_import('sqlalchemy')
    import json
    from copy import deepcopy

    if connector_keys is None:
        connector_keys = []
    if metric_keys is None:
        metric_keys = []
    if location_keys is None:
        location_keys = []
    else:
        location_keys = [
            (lk if lk not in ('[None]', 'None', 'null') else None)
                for lk in location_keys
        ]
    if tags is None:
        tags = []

    if params is None:
        params = {}

    ### Add three primary keys to params dictionary
    ###   (separated for convenience of arguments).
    cols = {
        'connector_keys': connector_keys,
        'metric_key': metric_keys,
        'location_key': location_keys,
    }

    ### Make deep copy so we don't mutate this somewhere else.
    parameters = deepcopy(params)
    for col, vals in cols.items():
        ### Allow for IS NULL to be declared as a single-item list ([None]).
        if vals == [None]:
            vals = None
        if vals not in [[], ['*']]:
            parameters[col] = vals
    cols = {k: v for k, v in cols.items() if v != [None]}

    if not table_exists('pipes', self, debug=debug):
        return []

    from meerschaum.connectors.sql.tables import get_tables
    pipes = get_tables(mrsm_instance=self, create=False, debug=debug)['pipes']

    _params = {}
    for k, v in parameters.items():
        _v = json.dumps(v) if isinstance(v, dict) else v
        _params[k] = _v

    negation_prefix = STATIC_CONFIG['system']['fetch_pipes_keys']['negation_prefix']
    ### Parse regular params.
    ### If a param begins with '_', negate it instead.
    _where = [
        (
            (pipes.c[key] == val) if not str(val).startswith(negation_prefix)
            else (pipes.c[key] != key)
        ) for key, val in _params.items()
            if not isinstance(val, (list, tuple)) and key in pipes.c
    ]
    q = sqlalchemy.select(
        [pipes.c.connector_keys, pipes.c.metric_key, pipes.c.location_key]
        + ([pipes.c.parameters] if tags else [])
    ).where(sqlalchemy.and_(True, *_where))

    ### Parse IN params and add OR IS NULL if None in list.
    for c, vals in cols.items():
        if not isinstance(vals, (list, tuple)) or not vals or not c in pipes.c:
            continue
        _in_vals, _ex_vals = separate_negation_values(vals)
        ### Include params (positive)
        q = (
            q.where(pipes.c[c].in_(_in_vals)) if None not in _in_vals
            else q.where(sqlalchemy.or_(pipes.c[c].in_(_in_vals), pipes.c[c].is_(None)))
        ) if _in_vals else q
        ### Exclude params (negative)
        q = q.where(pipes.c[c].not_in(_ex_vals)) if _ex_vals else q

    ### Finally, parse tags.
    _in_tags, _ex_tags = separate_negation_values(tags)
    ors = []
    for nt in _in_tags:
        ors.append(
            sqlalchemy.cast(
                pipes.c['parameters'],
                sqlalchemy.String,
            ).like(f'%"tags":%"{nt}"%')
        )
    q = q.where(sqlalchemy.and_(sqlalchemy.or_(*ors).self_group())) if ors else q
    ors = []
    for xt in _ex_tags:
        ors.append(
            sqlalchemy.cast(
                pipes.c['parameters'],
                sqlalchemy.String,
            ).not_like(f'%"tags":%"{xt}"%')
        )
    q = q.where(sqlalchemy.and_(sqlalchemy.or_(*ors).self_group())) if ors else q
    loc_asc = sqlalchemy.asc(pipes.c['location_key'])
    if self.flavor not in OMIT_NULLSFIRST_FLAVORS:
        loc_asc = sqlalchemy.nullsfirst(loc_asc)
    q = q.order_by(
        sqlalchemy.asc(pipes.c['connector_keys']),
        sqlalchemy.asc(pipes.c['metric_key']),
        loc_asc,
    )

    ### execute the query and return a list of tuples
    if debug:
        dprint(q.compile(compile_kwargs={'literal_binds': True}))
    try:
        rows = self.engine.execute(q).fetchall()
    except Exception as e:
        error(str(e))

    _keys = [(row['connector_keys'], row['metric_key'], row['location_key']) for row in rows]
    if not tags:
        return _keys
    ### Make 100% sure that the tags are correct.
    keys = []
    for row in rows:
        ktup = (row['connector_keys'], row['metric_key'], row['location_key'])
        _actual_tags = (
            json.loads(row['parameters']) if isinstance(row['parameters'], str)
            else row['parameters']
        ).get('tags', [])
        for nt in _in_tags:
            if nt in _actual_tags:
                keys.append(ktup)
        for xt in _ex_tags:
            if xt in _actual_tags:
                keys.remove(ktup)
            else:
                keys.append(ktup)
    return keys


def create_indices(
        self,
        pipe: meerschaum.Pipe,
        indices: Optional[List[str]] = None,
        debug: bool = False
    ) -> bool:
    """
    Create a pipe's indices.
    """
    from meerschaum.utils.warnings import warn
    from meerschaum.utils.debug import dprint
    if debug:
        dprint(f"Creating indices for {pipe}...")
    if not pipe.columns:
        warn(f"Unable to create indices for {pipe} without columns.", stack=False)
        return False
    ix_queries = {
        ix: queries
        for ix, queries in self.get_create_index_queries(pipe, debug=debug).items()
        if indices is None or ix in indices
    }
    success = True
    for ix, queries in ix_queries.items():
        ix_success = all(self.exec_queries(queries, debug=debug, silent=True))
        if not ix_success:
            success = False
            if debug:
                dprint(f"Failed to create index on column: {ix}")
    return success


def drop_indices(
        self,
        pipe: meerschaum.Pipe,
        indices: Optional[List[str]] = None,
        debug: bool = False
    ) -> bool:
    """
    Drop a pipe's indices.
    """
    from meerschaum.utils.warnings import warn
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
        pipe: meerschaum.Pipe,
        debug: bool = False,
    ) -> Dict[str, List[str]]:
    """
    Return a dictionary mapping columns to a `CREATE INDEX` or equivalent query.

    Parameters
    ----------
    pipe: meerschaum.Pipe
        The pipe to which the queries will correspond.

    Returns
    -------
    A dictionary of column names mapping to lists of queries.
    """
    from meerschaum.utils.debug import dprint
    from meerschaum.utils.sql import sql_item_name, get_distinct_col_count
    from meerschaum.utils.warnings import warn
    from meerschaum.config import get_config
    index_queries = {}

    indices = pipe.get_indices()

    _datetime = pipe.get_columns('datetime', error=False)
    _datetime_type = pipe.dtypes.get(_datetime, 'datetime64[ns]')
    _datetime_name = sql_item_name(_datetime, self.flavor) if _datetime is not None else None
    _datetime_index_name = (
        sql_item_name(indices['datetime'], self.flavor) if indices.get('datetime', None)
        else None
    )
    _id = pipe.get_columns('id', error=False)
    _id_name = sql_item_name(_id, self.flavor) if _id is not None else None

    _id_index_name = sql_item_name(indices['id'], self.flavor) if indices.get('id') else None
    _pipe_name = sql_item_name(pipe.target, self.flavor)
    _create_space_partition = get_config('system', 'experimental', 'space')

    ### create datetime index
    if _datetime is not None:
        if self.flavor == 'timescaledb':
            _id_count = (
                get_distinct_col_count(_id, f"SELECT {_id_name} FROM {_pipe_name}", self)
                if (_id is not None and _create_space_partition) else None
            )
            chunk_time_interval = (
                pipe.parameters.get('chunk_time_interval', None)
                or
                ("INTERVAL '1 DAY'" if not 'int' in _datetime_type.lower() else '100000')
            )

            dt_query = (
                f"SELECT create_hypertable('{_pipe_name}', " +
                f"'{_datetime}', "
                + (
                    f"'{_id}', {_id_count}, " if (_id is not None and _create_space_partition)
                    else ''
                )
                + f'chunk_time_interval => {chunk_time_interval}, '
                + "migrate_data => true);"
            )
        else: ### mssql, sqlite, etc.
            dt_query = (
                f"CREATE INDEX {_datetime_index_name} "
                + f"ON {_pipe_name} ({_datetime_name})"
            )

        index_queries[_datetime] = [dt_query]

    ### create id index
    if _id_name is not None:
        if self.flavor == 'timescaledb':
            ### Already created indices via create_hypertable.
            id_query = (
                None if (_id is not None and _create_space_partition)
                else (
                    f"CREATE INDEX {_id_index_name} ON {_pipe_name} ({_id_name})"
                    if _id is not None
                    else None
                )
            )
            pass
        elif self.flavor == 'citus':
            id_query = [(
                f"CREATE INDEX {_id_index_name} "
                + f"ON {_pipe_name} ({_id_name});"
            ), (
                f"SELECT create_distributed_table('{_pipe_name}', '{_id}');"
            )]
        else: ### mssql, sqlite, etc.
            id_query = f"CREATE INDEX {_id_index_name} ON {_pipe_name} ({_id_name})"

        if id_query is not None:
            index_queries[_id] = [id_query]


    ### Create indices for other labels in `pipe.columns`.
    other_indices = {
        ix_key: ix_unquoted
        for ix_key, ix_unquoted in pipe.get_indices().items()
        if ix_key not in ('datetime', 'id')
    }
    for ix_key, ix_unquoted in other_indices.items():
        ix_name = sql_item_name(ix_unquoted, self.flavor)
        col = pipe.columns[ix_key]
        col_name = sql_item_name(col, self.flavor)
        index_queries[col] = [f"CREATE INDEX {ix_name} ON {_pipe_name} ({col_name})"]

    return index_queries


def get_drop_index_queries(
        self,
        pipe: meerschaum.Pipe,
        debug: bool = False,
    ) -> Dict[str, List[str]]:
    """
    Return a dictionary mapping columns to a `DROP INDEX` or equivalent query.

    Parameters
    ----------
    pipe: meerschaum.Pipe
        The pipe to which the queries will correspond.

    Returns
    -------
    A dictionary of column names mapping to lists of queries.
    """
    if not pipe.exists(debug=debug):
        return {}
    from meerschaum.utils.sql import sql_item_name, table_exists, hypertable_queries
    drop_queries = {}
    indices = pipe.get_indices()
    pipe_name = sql_item_name(pipe.target, self.flavor)

    if self.flavor not in hypertable_queries:
        is_hypertable = False
    else:
        is_hypertable_query = hypertable_queries[self.flavor].format(table_name=pipe_name)
        is_hypertable = self.value(is_hypertable_query, silent=True, debug=debug) is not None

    if is_hypertable:
        nuke_queries = []
        temp_table = '_' + pipe.target + '_temp_migration'
        temp_table_name = sql_item_name(temp_table, self.flavor)

        if table_exists(temp_table, self, debug=debug):
            nuke_queries.append(f"DROP TABLE {temp_table_name}")
        nuke_queries += [
            f"SELECT * INTO {temp_table_name} FROM {pipe_name}",
            f"DROP TABLE {pipe_name}",
            f"ALTER TABLE {temp_table_name} RENAME TO {pipe_name}",
        ]
        nuke_ix_keys = ('datetime', 'id')
        nuked = False
        for ix_key in nuke_ix_keys:
            if ix_key in indices and not nuked:
                drop_queries[ix_key] = nuke_queries
                nuked = True

    drop_queries.update({
        ix_key: ["DROP INDEX " + sql_item_name(ix_unquoted, self.flavor)]
        for ix_key, ix_unquoted in indices.items()
        if ix_key not in drop_queries
    })
    return drop_queries


def delete_pipe(
        self,
        pipe: meerschaum.Pipe,
        debug: bool = False,
    ) -> SuccessTuple:
    """
    Delete a Pipe's registration and drop its table.
    """
    from meerschaum.utils.warnings import warn
    from meerschaum.utils.sql import sql_item_name
    from meerschaum.utils.debug import dprint
    from meerschaum.utils.packages import attempt_import
    sqlalchemy = attempt_import('sqlalchemy')

    ### try dropping first
    drop_tuple = pipe.drop(debug=debug)
    if not drop_tuple[0]:
        return drop_tuple

    if not pipe.id:
        return False, f"{pipe} is not registered."

    ### ensure pipes table exists
    from meerschaum.connectors.sql.tables import get_tables
    pipes = get_tables(mrsm_instance=self, create=(not pipe.temporary), debug=debug)['pipes']

    q = sqlalchemy.delete(pipes).where(pipes.c.pipe_id == pipe.id)
    if not self.exec(q, debug=debug):
        return False, f"Failed to delete registration for {pipe}."

    return True, "Success"


def get_backtrack_data(
        self,
        pipe: Optional[meerschaum.Pipe] = None,
        backtrack_minutes: int = 0,
        begin: Optional[datetime.datetime] = None,
        params: Optional[Dict[str, Any]] = None,
        limit: Optional[int] = None,
        chunksize: Optional[int] = -1,
        debug: bool = False
    ) -> Union[pandas.DataFrame, None]:
    """
    Get the most recent backtrack_minutes' worth of data from a Pipe.

    Parameters
    ----------
    pipe: meerschaum.Pipe:
        The pipe to get data from.

    backtrack_minutes: int, default 0
        How far into the past to look for data.

    begin: Optional[datetime.datetime], default None
        Where to start traversing from. Defaults to `None`, which uses the
        `meerschaum.Pipe.get_sync_time` value.

    params: Optional[Dict[str, Any]], default None
        Additional parameters to filter by.
        See `meerschaum.connectors.sql.build_where`.

    limit: Optional[int], default None
        If specified, limit the number of rows retrieved to this value.

    chunksize: Optional[int], default -1
        The size of dataframe chunks to load into memory.

    debug: bool, default False
        Verbosity toggle.

    Returns
    -------
    A `pd.DataFrame` of backtracked data.

    """
    import datetime
    from meerschaum.utils.warnings import error
    if pipe is None:
        error("Pipe must be provided.")
    if begin is None:
        begin = pipe.get_sync_time(debug=debug)

    return pipe.get_data(
        begin = begin,
        begin_add_minutes = (-1 * backtrack_minutes),
        order = 'desc',
        params = params,
        limit = limit,
        chunksize = chunksize,
        debug = debug,
    )


def get_pipe_data(
        self,
        pipe: Optional[meerschaum.Pipe] = None,
        begin: Union[datetime.datetime, str, None] = None,
        end: Union[datetime.datetime, str, None] = None,
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
    pipe: meerschaum.Pipe:
        The pipe to get data from.

    begin: Optional[datetime.datetime], default None
        If provided, get rows newer than or equal to this value.

    end: Optional[datetime.datetime], default None
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
    dtypes = pipe.dtypes
    if dtypes:
        if self.flavor == 'sqlite':
            if not pipe.columns.get('datetime', None):
                _dt = pipe.guess_datetime()
                dt = sql_item_name(_dt, self.flavor) if _dt else None
                is_guess = True
            else:
                _dt = pipe.get_columns('datetime')
                dt = sql_item_name(_dt, self.flavor)
                is_guess = False

            if _dt:
                dt_type = dtypes.get(_dt, 'object').lower()
                if 'datetime' not in dt_type:
                    if 'int' not in dt_type:
                        dtypes[_dt] = 'datetime64[ns]'
    existing_cols = pipe.get_columns_types(debug=debug)
    if existing_cols:
        dtypes = {col: typ for col, typ in dtypes.items() if col in existing_cols}
    query = self.get_pipe_data_query(
        pipe,
        begin = begin,
        end = end,
        params = params,
        order = order,
        limit = limit,
        begin_add_minutes = begin_add_minutes,
        end_add_minutes = end_add_minutes,
        debug = debug,
        **kw
    )
    df = self.read(
        query,
        debug = debug,
        **kw
    )
    if self.flavor == 'sqlite':
        from meerschaum.utils.misc import parse_df_datetimes
        from meerschaum.utils.packages import import_pandas
        pd = import_pandas()
        ### NOTE: We have to consume the iterator here to ensure that datatimes are parsed correctly
        df = parse_df_datetimes(df, debug=debug) if isinstance(df, pd.DataFrame) else (
            [parse_df_datetimes(c, debug=debug) for c in df]
        )
        for col, typ in dtypes.items():
            if typ != 'json':
                continue
            df[col] = df[col].apply(lambda x: json.loads(x) if x is not None else x)
    return df


def get_pipe_data_query(
        self,
        pipe: Optional[meerschaum.Pipe] = None,
        begin: Union[datetime.datetime, str, None] = None,
        end: Union[datetime.datetime, str, None] = None,
        params: Optional[Dict[str, Any]] = None,
        order: str = 'asc',
        limit: Optional[int] = None,
        begin_add_minutes: int = 0,
        end_add_minutes: int = 0,
        replace_nulls: Optional[str] = None,
        debug: bool = False,
        **kw: Any
    ) -> Union[str, None]:
    """
    Return the `SELECT` query for retrieving a pipe's data from its instance.

    Parameters
    ----------
    pipe: meerschaum.Pipe:
        The pipe to get data from.

    begin: Optional[datetime.datetime], default None
        If provided, get rows newer than or equal to this value.

    end: Optional[datetime.datetime], default None
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

    replace_nulls: Optional[str], default None
        If provided, replace null values with this value.

    debug: bool, default False
        Verbosity toggle.

    Returns
    -------
    A `SELECT` query to retrieve a pipe's data.
    """
    import json
    from meerschaum.utils.debug import dprint
    from meerschaum.utils.misc import items_str
    from meerschaum.utils.sql import sql_item_name, dateadd_str
    from meerschaum.utils.packages import import_pandas
    from meerschaum.utils.warnings import warn
    pd = import_pandas()

    select_cols = "SELECT *"
    if replace_nulls:
        existing_cols = pipe.get_columns_types(debug=debug)
        if existing_cols:
            select_cols = "SELECT "
            for col in existing_cols:
                select_cols += (
                    f"\n    COALESCE({sql_item_name(col, self.flavor)}, "
                    + f"'{replace_nulls}') AS {sql_item_name(col, self.flavor)},"
                )
            select_cols = select_cols[:-1]
    query = f"{select_cols}\nFROM {sql_item_name(pipe.target, self.flavor)}"
    where = ""

    if order is not None:
        default_order = 'asc'
        if order not in ('asc', 'desc'):
            warn(f"Ignoring unsupported order '{order}'. Falling back to '{default_order}'.")
            order = default_order
        order = order.upper()

    existing_cols = pipe.get_columns_types(debug=debug)

    if not pipe.columns.get('datetime', None):
        _dt = pipe.guess_datetime()
        dt = sql_item_name(_dt, self.flavor) if _dt else None
        is_guess = True
    else:
        _dt = pipe.get_columns('datetime')
        dt = sql_item_name(_dt, self.flavor)
        is_guess = False

    quoted_indices = {
        key: sql_item_name(val, self.flavor)
        for key, val in pipe.columns.items()
        if val in existing_cols
    }

    if begin is not None or end is not None:
        if is_guess:
            if _dt is None:
                warn(
                    f"No datetime could be determined for {pipe}."
                    + "\n    Ignoring begin and end...",
                    stack = False,
                )
                begin, end = None, None
            else:
                warn(
                    f"A datetime wasn't specified for {pipe}.\n"
                    + f"    Using column \"{_dt}\" for datetime bounds...",
                    stack = False,
                )

    is_dt_bound = False
    if begin is not None and _dt in existing_cols:
        begin_da = dateadd_str(
            flavor = self.flavor,
            datepart = 'minute',
            number = begin_add_minutes,
            begin = begin
        )
        where += f"{dt} >= {begin_da}" + (" AND " if end is not None else "")
        is_dt_bound = True

    if end is not None and _dt in existing_cols:
        if 'int' in str(type(end)).lower() and end == begin:
            end += 1
        end_da = dateadd_str(
            flavor = self.flavor,
            datepart = 'minute',
            number = end_add_minutes,
            begin = end
        )
        where += f"{dt} < {end_da}"
        is_dt_bound = True

    if params is not None:
        from meerschaum.utils.sql import build_where
        valid_params = {k: v for k, v in params.items() if k in existing_cols}
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
            if _dt and _dt in existing_cols:
                order_by += dt + ' ' + order + ','
            for key, quoted_col_name in quoted_indices.items():
                if key == 'datetime':
                    continue
                order_by += ' ' + quoted_col_name + ' ' + order + ','
            order_by = order_by[:-1]

        query += order_by

    if isinstance(limit, int):
        if self.flavor == 'mssql':
            query = f'SELECT TOP {limit} ' + query[len("SELECT *"):]
        elif self.flavor == 'oracle':
            query = f"SELECT * FROM (\n  {query}\n)\nWHERE ROWNUM = 1"
        else:
            query += f"\nLIMIT {limit}"
    
    if debug:
        to_print = (
            []
            + ([f"begin='{begin}'"] if begin else [])
            + ([f"end='{end}'"] if end else [])
            + ([f"params='{json.dumps(params)}'"] if params else [])
        )
        dprint("Getting pipe data with constraints: " + items_str(to_print, quotes=False))

    return query


def get_pipe_id(
        self,
        pipe: meerschaum.Pipe,
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
    pipes = get_tables(mrsm_instance=self, create=(not pipe.temporary), debug=debug)['pipes']

    query = sqlalchemy.select([pipes.c.pipe_id]).where(
        pipes.c.connector_keys == pipe.connector_keys
    ).where(
        pipes.c.metric_key == pipe.metric_key
    ).where(
        (pipes.c.location_key == pipe.location_key) if pipe.location_key is not None
        else pipes.c.location_key.is_(None)
    )
    _id = self.value(query, debug=debug, silent=pipe.temporary)
    if _id is not None:
        _id = int(_id)
    return _id


def get_pipe_attributes(
        self,
        pipe: meerschaum.Pipe,
        debug: bool = False,
    ) -> Dict[str, Any]:
    """
    Get a Pipe's attributes dictionary.
    """
    from meerschaum.utils.warnings import warn
    from meerschaum.utils.debug import dprint
    from meerschaum.connectors.sql.tables import get_tables
    from meerschaum.utils.packages import attempt_import
    sqlalchemy = attempt_import('sqlalchemy')

    if pipe.get_id(debug=debug) is None:
        return {}

    pipes = get_tables(mrsm_instance=self, create=(not pipe.temporary), debug=debug)['pipes']

    try:
        q = sqlalchemy.select([pipes]).where(pipes.c.pipe_id == pipe.id)
        if debug:
            dprint(q)
        attributes = (
            dict(self.exec(q, silent=True, debug=debug).first())
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


def sync_pipe(
        self,
        pipe: meerschaum.Pipe,
        df: Union[pandas.DataFrame, str, Dict[Any, Any], None] = None,
        begin: Optional[datetime.datetime] = None,
        end: Optional[datetime.datetime] = None,
        chunksize: Optional[int] = -1,
        check_existing: bool = True,
        blocking: bool = True,
        debug: bool = False,
        **kw: Any
    ) -> SuccessTuple:
    """
    Sync a pipe using a database connection.

    Parameters
    ----------
    pipe: meerschaum.Pipe
        The Meerschaum Pipe instance into which to sync the data.

    df: Union[pandas.DataFrame, str, Dict[Any, Any], List[Dict[str, Any]]]
        An optional DataFrame or equivalent to sync into the pipe.
        Defaults to `None`.

    begin: Optional[datetime.datetime], default None
        Optionally specify the earliest datetime to search for data.
        Defaults to `None`.

    end: Optional[datetime.datetime], default None
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
    from meerschaum.utils.warnings import warn
    from meerschaum.utils.debug import dprint
    from meerschaum.utils.packages import import_pandas
    from meerschaum.utils.sql import get_update_queries, sql_item_name, json_flavors
    from meerschaum.utils.misc import generate_password, get_json_cols
    from meerschaum import Pipe
    import time
    import copy
    pd = import_pandas()
    if df is None:
        msg = f"DataFrame is None. Cannot sync {pipe}."
        warn(msg)
        return False, msg

    start = time.perf_counter()

    if not pipe.temporary and not pipe.get_id(debug=debug):
        register_tuple = pipe.register(debug=debug)
        if not register_tuple[0]:
            return register_tuple

    ### quit here if implicitly syncing MQTT pipes.
    ### (pipe.sync() is called in the callback of the MQTTConnector.fetch() method)
    if df is None and pipe.connector.type == 'mqtt':
        return True, "Success"

    ### df is the dataframe returned from the remote source
    ### via the connector
    if debug:
        dprint("Fetched data:\n" + str(df))

    if not isinstance(df, pd.DataFrame):
        df = pipe.enforce_dtypes(df, debug=debug)

    ### if table does not exist, create it with indices
    is_new = False
    add_cols_query = None
    if not pipe.exists(debug=debug):
        check_existing = False
        is_new = True
    else:
        ### Check for new columns.
        add_cols_queries = self.get_add_columns_queries(pipe, df, debug=debug)
        if add_cols_queries:
            if not self.exec_queries(add_cols_queries, debug=debug):
                warn(f"Failed to add new columns to {pipe}.")

        alter_cols_queries = self.get_alter_columns_queries(pipe, df, debug=debug)
        if alter_cols_queries:
            if not self.exec_queries(alter_cols_queries, debug=debug):
                warn(f"Failed to alter columns for {pipe}.")


    unseen_df, update_df, delta_df = (
        pipe.filter_existing(
            df,
            chunksize = chunksize,
            begin = begin,
            end = end,
            debug = debug,
            **kw
        ) if check_existing else (df, None, df)
    )
    if debug:
        dprint("Delta data:\n" + str(delta_df))
        dprint("Unseen data:\n" + str(unseen_df))
        if update_df is not None:
            dprint("Update data:\n" + str(update_df))

    if update_df is not None and not update_df.empty:
        transact_id = generate_password(6)
        temp_target = '_' + transact_id + '_' + pipe.target
        update_kw = copy.deepcopy(kw)
        update_kw.update({
            'name': temp_target,
            'if_exists': 'append',
            'chunksize': chunksize,
            'dtype': self.get_to_sql_dtype(pipe, update_df, update_dtypes=False),
            'debug': debug,
        })
        self.to_sql(update_df, **update_kw)
        temp_pipe = Pipe(
            pipe.connector_keys + '_', pipe.metric_key, pipe.location_key,
            instance = pipe.instance_keys,
            columns = pipe.columns,
            target = temp_target,
            temporary = True,
        )

        existing_cols = pipe.get_columns_types(debug=debug)
        join_cols = [
            col for col_key, col in pipe.columns.items()
            if col and col_key != 'value' and col in existing_cols
        ]

        queries = get_update_queries(
            pipe.target,
            temp_target,
            self,
            join_cols,
            debug = debug
        )
        success = all(self.exec_queries(queries, break_on_error=True, debug=debug))
        drop_success, drop_msg = temp_pipe.drop(debug=debug)
        if not drop_success:
            warn(drop_msg)
        if not success:
            return False, f"Failed to apply update to {pipe}."

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
    })

    ### Account for first-time syncs of JSON columns.
    json_cols = get_json_cols(unseen_df)
    if json_cols:
        if not pipe.exists(debug=debug):
            pipe.dtypes.update({col: 'json' for col in json_cols})
            edit_success, edit_msg = pipe.edit(interactive=False, debug=debug)
            if not edit_success:
                warn(f"Unable to update JSON dtypes for {pipe}:\n{e}")

    stats = self.to_sql(unseen_df, **unseen_kw)
    if is_new:
        if not self.create_indices(pipe, debug=debug):
            if debug:
                dprint(f"Failed to create indices for {pipe}. Continuing...")

    end = time.perf_counter()
    success = stats['success']
    if not success:
        return success, stats['msg']
    msg = (
        f"Inserted {len(unseen_df)}, "
        + f"updated {len(update_df) if update_df is not None else 0} rows."
    )
    if debug:
        msg = msg[:-1] + (
            f"\non table {sql_item_name(pipe.target, self.flavor)}\n"
            + f"in {round(end-start, 2)} seconds."
        )
    return success, msg


def sync_pipe_inplace(
        self,
        pipe: 'meerschaum.Pipe',
        params: Optional[Dict[str, Any]] = None,
        begin: Optional[datetime.datetime] = None,
        end: Optional[datetime.datetime] = None,
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
    pipe: meerschaum.Pipe
        The pipe whose connector is the same as its instance.

    params: Optional[Dict[str, Any]], default None
        Optional params dictionary to build the `WHERE` clause.
        See `meerschaum.utils.sql.build_where`.

    begin: Optional[datetime.datetime], default None
        Optionally specify the earliest datetime to search for data.
        Defaults to `None`.

    end: Optional[datetime.datetime], default None
        Optionally specify the latest datetime to search for data.
        Defaults to `None`.

    chunksize: Optional[int], default -1
        Specify the number of rows to sync per chunk.
        If `-1`, resort to system configuration (default is `900`).
        A `chunksize` of `None` will sync all rows in one transaction.
        Defaults to `-1`.

    check_existing: bool, default True
        If `True`, pull and diff with existing data from the pipe. Defaults to `True`.

    debug: bool, default False
        Verbosity toggle.

    Returns
    -------
    A SuccessTuple.
    """
    from meerschaum.utils.sql import (
        sql_item_name, table_exists, get_sqlalchemy_table, get_pd_type,
        get_update_queries, get_null_replacement,
    )
    from meerschaum.utils.misc import generate_password
    from meerschaum.utils.debug import dprint
    metadef = self.get_pipe_metadef(
        pipe,
        params = params,
        begin = begin,
        end = end,
        debug = debug,
    )
    if self.flavor in ('mssql',):
        final_select_ix = metadef.lower().rfind('select')
        def_name = metadef[len('WITH '):].split(' ', maxsplit=1)[0]
        metadef = (
            metadef[:final_select_ix].rstrip() + ',\n'
            + "metadef AS (\n"
            + metadef[final_select_ix:]
            + "\n)\n"
        )

    pipe_name = sql_item_name(pipe.target, self.flavor)
    if not pipe.exists(debug=debug):
        if self.flavor in ('mssql',):
            create_pipe_query = metadef + f"SELECT *\nINTO {pipe_name}\nFROM metadef"
        elif self.flavor in ('sqlite', 'oracle', 'mysql', 'mariadb', 'duckdb'):
            create_pipe_query = (
                f"CREATE TABLE {pipe_name} AS\n"
                + f"SELECT *\nFROM ({metadef})"
                + (" AS metadef" if self.flavor in ('mysql', 'mariadb') else '')
            )
        else:
            create_pipe_query = f"SELECT *\nINTO {pipe_name}\nFROM ({metadef}) AS metadef"
        result = self.exec(create_pipe_query, debug=debug)
        if result is None:
            return False, f"Could not insert new data into {pipe} from its SQL query definition."
        if not self.create_indices(pipe, debug=debug):
            if debug:
                dprint(f"Failed to create indices for {pipe}. Continuing...")

        rowcount = pipe.get_rowcount(debug=debug)
        return True, f"Inserted {rowcount}, updated 0 rows."

    ### Generate names for the tables.
    transact_id = generate_password(6)
    def get_temp_table_name(label: str) -> str:
        return '_' + transact_id + '_' + label + '_' + pipe.target

    backtrack_table_raw = get_temp_table_name('backtrack')
    backtrack_table_name = sql_item_name(backtrack_table_raw, self.flavor)
    new_table_raw = get_temp_table_name('new')
    new_table_name = sql_item_name(new_table_raw, self.flavor)
    delta_table_raw = get_temp_table_name('delta')
    delta_table_name = sql_item_name(delta_table_raw, self.flavor)
    joined_table_raw = get_temp_table_name('joined')
    joined_table_name = sql_item_name(joined_table_raw, self.flavor)
    unseen_table_raw = get_temp_table_name('unseen')
    unseen_table_name = sql_item_name(unseen_table_raw, self.flavor)
    update_table_raw = get_temp_table_name('update')
    update_table_name = sql_item_name(update_table_raw, self.flavor)

    new_queries = []
    drop_new_query = f"DROP TABLE {new_table_name}"
    if table_exists(new_table_raw, self, debug=debug):
        new_queries.append(drop_new_query)

    if self.flavor in ('mssql',):
        create_new_query = metadef + f"SELECT *\nINTO {new_table_name}\nFROM metadef"
    elif self.flavor in ('sqlite', 'oracle', 'mysql', 'mariadb', 'duckdb'):
        create_new_query = (
            f"CREATE TABLE {new_table_name} AS\n"
            + f"SELECT *\nFROM ({metadef})"
            + (" AS metadef" if self.flavor in ('mysql', 'mariadb') else '')
        )
    else:
        create_new_query = f"SELECT *\nINTO {new_table_name}\nFROM ({metadef}) AS metadef"

    new_queries.append(create_new_query)

    new_success = all(self.exec_queries(new_queries, break_on_error=True, debug=debug))
    if not new_success:
        self.exec_queries([drop_new_query], break_on_error=False, debug=debug)
        return False, f"Could not fetch new data for {pipe}."

    new_table_obj = get_sqlalchemy_table(
        new_table_raw,
        connector = self,
        refresh = True,
        debug = debug,
    )
    new_cols = {str(col.name): get_pd_type(str(col.type)) for col in new_table_obj.columns}

    add_cols_queries = self.get_add_columns_queries(pipe, new_cols, debug=debug)
    if add_cols_queries:
        if not self.exec_queries(add_cols_queries, debug=debug):
            warn(f"Failed to add new columns to {pipe}.")

    alter_cols_queries = self.get_alter_columns_queries(pipe, new_cols, debug=debug)
    if alter_cols_queries:
        if not self.exec_queries(alter_cols_queries, debug=debug):
            warn(f"Failed to alter columns for {pipe}.")

    if not check_existing:
        new_count = self.value(f"SELECT COUNT(*) FROM {new_table_name}", debug=debug)
        insert_queries = [
            (
                f"INSERT INTO {pipe_name}\n"
                + f"SELECT *\nFROM {new_table_name}"
            ),
            f"DROP TABLE {new_table_name}"
        ]
        if not self.exec_queries(insert_queries, debug=debug, break_on_error=False):
            return False, f"Failed to insert into rows into {pipe}."
        return True, f"Inserted {new_count}, updated 0 rows."


    backtrack_queries = []
    drop_backtrack_query = f"DROP TABLE {backtrack_table_name}"
    if table_exists(backtrack_table_raw, self, debug=debug):
        backtrack_queries.append(drop_backtrack_query)
    backtrack_def = self.get_pipe_data_query(
        pipe,
        begin = begin,
        end = end,
        params = params,
        debug = debug,
        order = None,
    )

    create_backtrack_query = (
        (
            f"WITH backtrack_def AS ({backtrack_def})\n"
            + f"SELECT *\nINTO {backtrack_table_name}\nFROM backtrack_def"
        ) if self.flavor not in ('sqlite', 'oracle', 'mysql', 'mariadb', 'duckdb')
        else (
            f"CREATE TABLE {backtrack_table_name} AS\n"
            + f"SELECT *\nFROM ({backtrack_def})"
            + (" AS backtrack" if self.flavor in ('mysql', 'mariadb') else '')
        )
    )
    backtrack_queries.append(create_backtrack_query)
    backtrack_success = all(self.exec_queries(backtrack_queries, break_on_error=True, debug=debug))
    if not backtrack_success:
        self.exec_queries([drop_new_query, drop_backtrack_query], break_on_error=False, debug=debug)
        return False, f"Could not fetch backtrack data from {pipe}."


    ### Determine which index columns are present in both tables.
    backtrack_table_obj = get_sqlalchemy_table(
        backtrack_table_raw,
        connector = self,
        refresh = True,
        debug = debug,
    )
    backtrack_cols = {str(col.name): str(col.type) for col in backtrack_table_obj.columns}
    common_cols = [col for col in new_cols if col in backtrack_cols]
    on_cols = {
        col: new_cols.get(col, 'object')
        for col_key, col in pipe.columns.items()
        if (
            col
            and
            col_key != 'value'
            and col in backtrack_cols
            and col in new_cols
        )
    }

    delta_queries = []
    drop_delta_query = f"DROP TABLE {delta_table_name}"
    if table_exists(delta_table_raw, self, debug=debug):
        delta_queries.append(drop_delta_query)

    create_delta_query = (
        (
            f"SELECT\n"
            + (
                ', '.join([
                    f"COALESCE(new.{sql_item_name(col, self.flavor)}, "
                    + f"{get_null_replacement(typ, self.flavor)}) AS "
                    + sql_item_name(col, self.flavor)
                    for col, typ in new_cols.items()
                ])
            )
            + "\n"
            + f"INTO {delta_table_name}\n"
            + f"FROM {new_table_name} AS new\n"
            + f"LEFT OUTER JOIN {backtrack_table_name} AS old\nON\n"
            + '\nAND\n'.join([
                (
                    'COALESCE(new.' + sql_item_name(c, self.flavor) + ", "
                    + get_null_replacement(new_cols[c], self.flavor) + ") "
                    + ' = '
                    + 'COALESCE(old.' + sql_item_name(c, self.flavor) + ", "
                    + get_null_replacement(backtrack_cols[c], self.flavor) + ") "
                ) for c in common_cols
            ])
            + "\nWHERE\n"
            + '\nAND\n'.join([
                (
                    'old.' + sql_item_name(c, self.flavor) + ' IS NULL'
                ) for c in common_cols
            ])
            #  + "\nAND\n"
            #  + '\nAND\n'.join([
                #  (
                    #  'new.' + sql_item_name(c, self.flavor) + ' IS NOT NULL'
                #  ) for c in new_cols
            #  ])
        ) if self.flavor not in ('sqlite', 'oracle', 'mysql', 'mariadb', 'duckdb')
        else (
            f"CREATE TABLE {delta_table_name} AS\n"
            + f"SELECT\n"
            + (
                ', '.join([
                    f"COALESCE(new.{sql_item_name(col, self.flavor)}, "
                    + f"{get_null_replacement(typ, self.flavor)}) AS "
                    + sql_item_name(col, self.flavor)
                    for col, typ in new_cols.items()
                ])
            )
            + "\n"
            + f"FROM {new_table_name} new\n"
            + f"LEFT OUTER JOIN {backtrack_table_name} old\nON\n"
            + '\nAND\n'.join([
                (
                    'COALESCE(new.' + sql_item_name(c, self.flavor) + ", "
                    + get_null_replacement(new_cols[c], self.flavor) + ") "
                    + ' = '
                    + 'COALESCE(old.' + sql_item_name(c, self.flavor) + ", "
                    + get_null_replacement(backtrack_cols[c], self.flavor) + ") "
                ) for c in common_cols
            ])
            + "\nWHERE\n"
            + '\nAND\n'.join([
                (
                    'old.' + sql_item_name(c, self.flavor) + ' IS NULL'
                ) for c in common_cols
            ])
            #  + "\nAND\n"
            #  + '\nAND\n'.join([
                #  (
                    #  'new.' + sql_item_name(c, self.flavor) + ' IS NOT NULL'
                #  ) for c in new_cols
            #  ])
        )
    )

    delta_queries.append(create_delta_query)

    delta_success = all(self.exec_queries(delta_queries, break_on_error=True, debug=debug))
    if not delta_success:
        self.exec_queries(
            [
                drop_new_query,
                drop_backtrack_query,
                drop_delta_query,
            ],
            break_on_error = False,
            debug = debug,
        )
        return False, f"Could not filter data for {pipe}."

    delta_table_obj = get_sqlalchemy_table(
        delta_table_raw,
        connector = self,
        refresh = True,
        debug = debug,
    )
    delta_cols = {str(col.name): get_pd_type(str(col.type)) for col in delta_table_obj.columns}

    joined_queries = []
    drop_joined_query = f"DROP TABLE {joined_table_name}"
    if on_cols and table_exists(joined_table_raw, self, debug=debug):
        joined_queries.append(drop_joined_query)

    create_joined_query = (
        (
            "SELECT "
            + (', '.join([
                (
                    'delta.' + sql_item_name(c, self.flavor)
                    + " AS " + sql_item_name(c + '_delta', self.flavor)
                ) for c in delta_cols
            ]))
            + ", "
            + (', '.join([
                (
                    'bt.' + sql_item_name(c, self.flavor)
                    + " AS " + sql_item_name(c + '_backtrack', self.flavor)
                ) for c in backtrack_cols
            ]))
            + f"\nINTO {joined_table_name}\n"
            + f"FROM {delta_table_name} AS delta\n"
            + f"LEFT OUTER JOIN {backtrack_table_name} AS bt\nON\n"
            + '\nAND\n'.join([
                (
                    'COALESCE(delta.' + sql_item_name(c, self.flavor)
                    + ", " + get_null_replacement(typ, self.flavor) + ")"
                    + ' = '
                    + 'COALESCE(bt.' + sql_item_name(c, self.flavor)
                    + ", " + get_null_replacement(typ, self.flavor) + ")"
                ) for c, typ in on_cols.items()
            ])
        ) if self.flavor not in ('sqlite', 'oracle', 'mysql', 'mariadb', 'duckdb')
        else (
            f"CREATE TABLE {joined_table_name} AS\n"
            + "SELECT "
            + (', '.join([
                (
                    'delta.' + sql_item_name(c, self.flavor)
                    + " AS " + sql_item_name(c + '_delta', self.flavor)
                ) for c in delta_cols
            ]))
            + ", "
            + (', '.join([
                (
                    'bt.' + sql_item_name(c, self.flavor)
                    + " AS " + sql_item_name(c + '_backtrack', self.flavor)
                ) for c in backtrack_cols
            ]))
            + f"\nFROM {delta_table_name} delta\n"
            + f"LEFT OUTER JOIN {backtrack_table_name} bt\nON\n"
            + '\nAND\n'.join([
                (
                    'COALESCE(delta.' + sql_item_name(c, self.flavor)
                    + ", " + get_null_replacement(typ, self.flavor) + ")"
                    + ' = '
                    + 'COALESCE(bt.' + sql_item_name(c, self.flavor)
                    + ", " + get_null_replacement(typ, self.flavor) + ")"
                ) for c, typ in on_cols.items()
            ])
        )
    )

    joined_queries.append(create_joined_query)

    joined_success = (
        all(self.exec_queries(joined_queries, break_on_error=True, debug=debug))
        if on_cols else True
    )
    if not joined_success:
        self.exec_queries(
            [
                drop_new_query,
                drop_backtrack_query,
                drop_delta_query,
                drop_joined_query,
            ],
            break_on_error = False,
            debug = debug,
        )
        return False, f"Could not separate new and updated data for {pipe}."

    unseen_queries = []
    drop_unseen_query = f"DROP TABLE {unseen_table_name}"
    if on_cols and table_exists(unseen_table_raw, self, debug=debug):
        unseen_queries.append(drop_unseen_query)

    create_unseen_query = (
        (
            "SELECT "
            + (', '.join([
                (
                    "CASE\n    WHEN " + sql_item_name(c + '_delta', self.flavor)
                    + " != " + get_null_replacement(typ, self.flavor) 
                    + " THEN " + sql_item_name(c + '_delta', self.flavor)
                    + "\n    ELSE NULL\nEND "
                    + " AS " + sql_item_name(c, self.flavor)
                ) for c, typ in delta_cols.items()
            ]))
            + f"\nINTO {unseen_table_name}\n"
            + f"\nFROM {joined_table_name} AS joined\n"
            + f"WHERE "
            + '\nAND\n'.join([
                (
                    sql_item_name(c + '_backtrack', self.flavor) + ' IS NULL'
                ) for c in delta_cols
            ])
            #  + "\nAND\n"
            #  + '\nAND\n'.join([
                #  (
                    #  sql_item_name(c + '_delta', self.flavor) + ' IS NOT NULL'
                #  ) for c in delta_cols
            #  ])
        ) if self.flavor not in ('sqlite', 'oracle', 'mysql', 'mariadb', 'duckdb') else (
            f"CREATE TABLE {unseen_table_name} AS\n"
            + "SELECT "
            + (', '.join([
                (
                    "CASE\n    WHEN " + sql_item_name(c + '_delta', self.flavor)
                    + " != "
                    + get_null_replacement(typ, self.flavor)
                    + " THEN " + sql_item_name(c + '_delta', self.flavor)
                    + "\n    ELSE NULL\nEND "
                    + " AS " + sql_item_name(c, self.flavor)
                ) for c, typ in delta_cols.items()
            ]))
            + f"\nFROM {joined_table_name} joined\n"
            + f"WHERE "
            + '\nAND\n'.join([
                (
                    sql_item_name(c + '_backtrack', self.flavor) + ' IS NULL'
                ) for c in delta_cols
            ])
            #  + "\nAND\n"
            #  + '\nAND\n'.join([
                #  (
                    #  sql_item_name(c + '_delta', self.flavor) + ' IS NOT NULL'
                #  ) for c in delta_cols
            #  ])
        )
    )

    unseen_queries.append(create_unseen_query)

    unseen_success = (
        all(self.exec_queries(unseen_queries, break_on_error=True, debug=debug))
        if on_cols else True
    )
    if not unseen_success:
        self.exec_queries(
            [
                drop_new_query,
                drop_backtrack_query,
                drop_delta_query,
                drop_joined_query,
                drop_unseen_query,
            ],
            break_on_error = False,
            debug = debug,
        )
        return False, f"Could not determine new data for {pipe}."
    unseen_count = self.value(
        (
            "SELECT COUNT(*) FROM "
            + (unseen_table_name if on_cols else delta_table_name)
        ), debug = debug,
    )

    update_queries = []
    drop_update_query = f"DROP TABLE {update_table_name}"
    if on_cols and table_exists(update_table_raw, self, debug=debug):
        update_queries.append(drop_unseen_query)

    create_update_query = (
        (
            "SELECT "
            + (', '.join([
                (
                    "CASE\n    WHEN " + sql_item_name(c + '_delta', self.flavor)
                    + " != " + get_null_replacement(typ, self.flavor)
                    + " THEN " + sql_item_name(c + '_delta', self.flavor)
                    + "\n    ELSE NULL\nEND "
                    + " AS " + sql_item_name(c, self.flavor)
                ) for c, typ in delta_cols.items()
            ]))
            + f"\nINTO {update_table_name}"
            + f"\nFROM {joined_table_name} AS joined\n"
            + f"WHERE "
            + '\nOR\n'.join([
                (
                    sql_item_name(c + '_backtrack', self.flavor) + ' IS NOT NULL'
                ) for c in delta_cols
            ])
        ) if self.flavor not in ('sqlite', 'oracle', 'mysql', 'mariadb', 'duckdb') else (
            f"CREATE TABLE {update_table_name} AS\n"
            + "SELECT "
            + (', '.join([
                (
                    "CASE\n    WHEN " + sql_item_name(c + '_delta', self.flavor)
                    + " != "
                    + get_null_replacement(typ, self.flavor)
                    + " THEN " + sql_item_name(c + '_delta', self.flavor)
                    + "\n    ELSE NULL\nEND "
                    + " AS " + sql_item_name(c, self.flavor)
                ) for c, typ in delta_cols.items()
            ]))
            + f"\nFROM {joined_table_name} joined\n"
            + f"WHERE "
            + '\nOR\n'.join([
                (
                    sql_item_name(c + '_backtrack', self.flavor) + ' IS NOT NULL'
                ) for c in delta_cols
            ])
        )
    )

    update_queries.append(create_update_query)

    update_success = (
        all(self.exec_queries(update_queries, break_on_error=True, debug=debug))
        if on_cols else True
    )
    if not update_success:
        self.exec_queries(
            [
                drop_new_query,
                drop_backtrack_query,
                drop_delta_query,
                drop_joined_query,
                drop_unseen_query,
                drop_update_query,
            ],
            break_on_error = False,
            debug = debug,
        )
        return False, "Could not determine updated data for {pipe}."
    update_count = (
        self.value(f"SELECT COUNT(*) FROM {update_table_name}", debug=debug)
        if on_cols else 0
    )

    apply_update_queries = (
        get_update_queries(
            pipe.target,
            update_table_raw,
            self,
            on_cols,
            debug = debug
        )
        if on_cols else []
    )

    apply_unseen_queries = [
        (
            f"INSERT INTO {pipe_name}\n"
            + f"SELECT *\nFROM " + (unseen_table_name if on_cols else delta_table_name)
        ),
    ]

    apply_queries = (
        (apply_unseen_queries if unseen_count > 0 else [])
        + (apply_update_queries if update_count > 0 else [])
        + [
            drop_new_query,
            drop_backtrack_query,
            drop_delta_query,
        ] + (
            [
                drop_joined_query,
                drop_unseen_query,
                drop_update_query,
            ] if on_cols else []
        )
    )
    success = all(self.exec_queries(apply_queries, break_on_error=False, debug=debug))
    msg = (
        f"Was not able to apply changes to {pipe}."
        if not success else f"Inserted {unseen_count}, updated {update_count} rows."
    )
    return success, msg


def get_sync_time(
        self,
        pipe: 'meerschaum.Pipe',
        params: Optional[Dict[str, Any]] = None,
        newest: bool = True,
        round_down: bool = True,
        debug: bool = False,
    ) -> 'datetime.datetime':
    """Get a Pipe's most recent datetime.

    Parameters
    ----------
    pipe: meerschaum.Pipe
        The pipe to get the sync time for.

    params: Optional[Dict[str, Any]], default None
        Optional params dictionary to build the `WHERE` clause.
        See `meerschaum.utils.sql.build_where`.

    newest: bool, default True
        If `True`, get the most recent datetime (honoring `params`).
        If `False`, get the oldest datetime (ASC instead of DESC).

    round_down: bool, default True
        If `True`, round the resulting datetime value down to the nearest minute.
        Defaults to `True`.

    Returns
    -------
    A `datetime.datetime` object if the pipe exists, otherwise `None`.
    """
    from meerschaum.utils.sql import sql_item_name, build_where
    from meerschaum.utils.warnings import warn
    import datetime
    table = sql_item_name(pipe.target, self.flavor)

    dt_col = pipe.columns.get('datetime', None)
    dt_type = pipe.dtypes.get(dt_col, 'datetime64[ns]')
    if not dt_col:
        _dt = pipe.guess_datetime()
        dt = sql_item_name(_dt, self.flavor) if _dt else None
        is_guess = True
    else:
        _dt = dt_col
        dt = sql_item_name(_dt, self.flavor)
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
        from meerschaum.utils.misc import round_time
        import datetime
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
        elif isinstance(db_time, datetime.datetime):
            if hasattr(db_time, 'to_pydatetime'):
                st = db_time.to_pydatetime()
            else:
                st = db_time
        ### Sometimes the datetime is actually a date.
        elif isinstance(db_time, datetime.date):
            st = datetime.datetime.combine(db_time, datetime.datetime.min.time())
        ### Adding support for an integer datetime axis.
        elif 'int' in str(type(db_time)).lower():
            st = db_time
        ### Convert pandas timestamp to Python datetime.
        else:
            st = db_time.to_pydatetime()

        ### round down to smooth timestamp
        sync_time = (
            round_time(st, date_delta=datetime.timedelta(minutes=1), to='down')
            if round_down else st
        ) if not isinstance(st, int) else st

    except Exception as e:
        sync_time = None
        warn(str(e))

    return sync_time

def pipe_exists(
        self,
        pipe : meerschaum.Pipe,
        debug : bool = False
    ) -> bool:
    """
    Check that a Pipe's table exists.

    Parameters
    ----------
    pipe: meerschaum.Pipe:
        The pipe to check.
        
    debug: bool:, default False
        Verbosity toggle.

    Returns
    -------
    A `bool` corresponding to whether a pipe's table exists.

    """
    from meerschaum.utils.sql import table_exists
    exists = table_exists(pipe.target, self, debug=debug)
    if debug:
        from meerschaum.utils.debug import dprint
        dprint(f"{pipe} " + ('exists.' if exists else 'does not exist.'))
    return exists


def get_pipe_rowcount(
        self,
        pipe: meerschaum.Pipe,
        begin: Optional[datetime.datetime] = None,
        end: Optional[datetime.datetime] = None,
        remote: bool = False,
        params: Optional[Dict[str, Any]] = None,
        debug: bool = False
    ) -> int:
    """
    Get the rowcount for a pipe in accordance with given parameters.

    Parameters
    ----------
    pipe: meerschaum.Pipe
        The pipe to query with.
        
    begin: Optional[datetime.datetime], default None
        The beginning datetime value.

    end: Optional[datetime.datetime], default None
        The beginning datetime value.

    remote: bool, default False
        If `True`, get the rowcount for the remote table.

    params: Optional[Dict[str, Any]], default None
        See `meerschaum.utils.sql.build_where`.

    debug: bool, default False
        Verbosity toggle.

    Returns
    -------
    An `int` for the number of rows if the `pipe` exists, otherwise `None`.

    """
    from meerschaum.utils.sql import dateadd_str, sql_item_name
    from meerschaum.utils.warnings import error, warn
    from meerschaum.connectors.sql._fetch import get_pipe_query
    if remote:
        msg = f"'fetch:definition' must be an attribute of {pipe} to get a remote rowcount."
        if 'fetch' not in pipe.parameters:
            error(msg)
            return None
        if 'definition' not in pipe.parameters['fetch']:
            error(msg)
            return None

    _pipe_name = sql_item_name(pipe.target, self.flavor)

    if not pipe.columns.get('datetime', None):
        _dt = pipe.guess_datetime()
        dt = sql_item_name(_dt, self.flavor) if _dt else None
        is_guess = True
    else:
        _dt = pipe.get_columns('datetime')
        dt = sql_item_name(_dt, self.flavor)
        is_guess = False

    if begin is not None or end is not None:
        if is_guess:
            if _dt is None:
                warn(
                    f"No datetime could be determined for {pipe}."
                    + "\n    Ignoring begin and end...",
                    stack = False,
                )
                begin, end = None, None
            else:
                warn(
                    f"A datetime wasn't specified for {pipe}.\n"
                    + f"    Using column \"{_dt}\" for datetime bounds...",
                    stack = False,
                )


    _datetime_name = sql_item_name(
        _dt,
        pipe.instance_connector.flavor if not remote else pipe.connector.flavor
    )
    _cols_names = [
        sql_item_name(col, pipe.instance_connector.flavor if not remote else pipe.connector.flavor)
        for col in set(
            ([_dt] if _dt else [])
            + ([] if params is None else list(params.keys()))
        )
    ]
    if not _cols_names:
        _cols_names = ['*']

    src = (
        f"SELECT {', '.join(_cols_names)} FROM {_pipe_name}"
        if not remote else get_pipe_query(pipe)
    )
    query = (
        f"""
        WITH src AS ({src})
        SELECT COUNT(*)
        FROM src
        """
    ) if self.flavor not in ('mysql', 'mariadb') else (
        f"""
        SELECT COUNT(*)
        FROM ({src}) AS src
        """
    )
    if begin is not None or end is not None:
        query += "WHERE"
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
        pipe: meerschaum.Pipe,
        debug: bool = False,
        **kw
    ) -> SuccessTuple:
    """
    Drop a pipe's tables but maintain its registration.

    Parameters
    ----------
    pipe: meerschaum.Pipe
        The pipe to drop.
        
    """
    from meerschaum.utils.sql import table_exists, sql_item_name
    success = True
    target, temp_target = pipe.target, '_' + pipe.target
    target_name, temp_name = (
        sql_item_name(target, self.flavor),
        sql_item_name(temp_target, self.flavor),
    )
    if table_exists(target, self, debug=debug):
        success = self.exec(f"DROP TABLE {target_name}", silent=True, debug=debug) is not None
    if table_exists(temp_target, self, debug=debug):
        success = (
            success
            and self.exec(f"DROP TABLE {temp_name}", silent=True, debug=debug) is not None
        )

    msg = "Success" if success else f"Failed to drop {pipe}."
    return success, msg


def clear_pipe(
        self,
        pipe: meerschaum.Pipe,
        begin: Optional[datetime.datetime] = None,
        end: Optional[datetime.datetime] = None,
        params: Optional[Dict[str, Any]] = None,
        debug: bool = False,
        **kw
    ) -> SuccessTuple:
    """
    Delete a pipe's data within a bounded or unbounded interval without dropping the table.

    Parameters
    ----------
    pipe: meerschaum.Pipe
        The pipe to clear.
        
    begin: Optional[datetime.datetime], default None
        Beginning datetime. Inclusive.

    end: Optional[datetime.datetime], default None
         Ending datetime. Exclusive.

    params: Optional[Dict[str, Any]], default None
         See `meerschaum.utils.sql.build_where`.

    """
    if not pipe.exists(debug=debug):
        return True, f"{pipe} does not exist, so nothing was cleared."

    from meerschaum.utils.sql import sql_item_name, build_where, dateadd_str
    from meerschaum.utils.warnings import warn
    pipe_name = sql_item_name(pipe.target, self.flavor)

    if not pipe.columns.get('datetime', None):
        _dt = pipe.guess_datetime()
        dt_name = sql_item_name(_dt, self.flavor) if _dt else None
        is_guess = True
    else:
        _dt = pipe.get_columns('datetime')
        dt_name = sql_item_name(_dt, self.flavor)
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
        pipe: meerschaum.Pipe,
        debug: bool = False,
    ) -> sqlalchemy.Table:
    """
    Return the `sqlalchemy.Table` object for a `meerschaum.Pipe`.

    Parameters
    ----------
    pipe: meerschaum.Pipe:
        The pipe in question.
        

    Returns
    -------
    A `sqlalchemy.Table` object. 

    """
    from meerschaum.utils.sql import get_sqlalchemy_table
    if not pipe.exists(debug=debug):
        return None
    return get_sqlalchemy_table(pipe.target, connector=self, debug=debug, refresh=True)


def get_pipe_columns_types(
        self,
        pipe: meerschaum.Pipe,
        debug: bool = False,
    ) -> Optional[Dict[str, str]]:
    """
    Get the pipe's columns and types.

    Parameters
    ----------
    pipe: meerschaum.Pipe:
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
    if not pipe.exists(debug=debug):
        return {}
    table_columns = {}
    try:
        pipe_table = self.get_pipe_table(pipe, debug=debug)
        for col in pipe_table.columns:
            table_columns[str(col.name)] = str(col.type)
    except Exception as e:
        import traceback
        traceback.print_exc()
        from meerschaum.utils.warnings import warn
        warn(e)
        table_columns = None

    return table_columns


def get_add_columns_queries(
        self,
        pipe: mrsm.Pipe,
        df: Union[pd.DataFrame, Dict[str, str]],
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

    Returns
    -------
    A list of the `ALTER TABLE` SQL query or queries to be executed on the provided connector.
    """
    if not pipe.exists(debug=debug):
        return []
    import copy
    from meerschaum.utils.sql import get_pd_type, get_db_type, sql_item_name
    from meerschaum.utils.misc import flatten_list
    table_obj = self.get_pipe_table(pipe, debug=debug)
    df_cols_types = (
        {
            col: str(typ)
            for col, typ in df.dtypes.items()
        }
        if not isinstance(df, dict)
        else copy.deepcopy(df)
    )
    if len(df) > 0 and not isinstance(df, dict):
        for col, typ in list(df_cols_types.items()):
            if typ != 'object':
                continue
            val = df.iloc[0][col]
            if isinstance(val, (dict, list)):
                df_cols_types[col] = 'json'
            elif isinstance(val, str):
                df_cols_types[col] = 'str'
    db_cols_types = {col: get_pd_type(str(typ.type)) for col, typ in table_obj.columns.items()}
    new_cols = set(df_cols_types) - set(db_cols_types)
    if not new_cols:
        return []

    new_cols_types = {
        col: get_db_type(
            df_cols_types[col],
            self.flavor
        ) for col in new_cols
    }

    query = "ALTER TABLE " + sql_item_name(pipe.target, self.flavor)
    for col, typ in new_cols_types.items():
        query += "\nADD " + sql_item_name(col, self.flavor) + " " + typ + ","
    query = query[:-1]
    if self.flavor != 'duckdb':
        return [query]

    drop_index_queries = list(flatten_list(
        [q for ix, q in self.get_drop_index_queries(pipe, debug=debug).items()]
    ))
    create_index_queries = list(flatten_list(
        [q for ix, q in self.get_create_index_queries(pipe, debug=debug).items()]
    ))

    return drop_index_queries + [query] + create_index_queries


def get_alter_columns_queries(
        self,
        pipe: mrsm.Pipe,
        df: Union[pd.DataFrame, Dict[str, str]],
        debug: bool = False,
    ) -> List[str]:
    """
    If we encounter a column of a different type, set the entire column to text.

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
    if self.flavor == 'sqlite':
        return []
    if not pipe.exists(debug=debug):
        return []
    from meerschaum.utils.sql import get_pd_type, get_db_type, sql_item_name
    from meerschaum.utils.misc import flatten_list
    table_obj = self.get_pipe_table(pipe, debug=debug)
    df_cols_types = (
        {col: str(typ) for col, typ in df.dtypes.items()}
        if not isinstance(df, dict) else df
    )
    db_cols_types = {col: get_pd_type(str(typ.type)) for col, typ in table_obj.columns.items()}
    altered_cols = [
        col for col, typ in df_cols_types.items()
        if typ.lower() != db_cols_types.get(col, 'object').lower()
        and db_cols_types.get(col, 'object') != 'object'
    ]
    if not altered_cols:
        return []

    text_type = get_db_type('str', self.flavor)
    altered_cols_types = {
        col: text_type
        for col in altered_cols
    }

    queries = []
    if self.flavor == 'oracle':
        add_query = "ALTER TABLE " + sql_item_name(pipe.target, self.flavor)
        for col, typ in altered_cols_types.items():
            add_query += "\nADD " + sql_item_name(col + '_temp', self.flavor) + " " + typ + ","
        add_query = add_query[:-1]
        queries.append(add_query)

        populate_temp_query = "UPDATE " + sql_item_name(pipe.target, self.flavor)
        for col, typ in altered_cols_types.items():
            populate_temp_query += (
                "\nSET " + sql_item_name(col + '_temp', self.flavor)
                + ' = ' + sql_item_name(col, self.flavor) + ','
            )
        populate_temp_query = populate_temp_query[:-1]
        queries.append(populate_temp_query)

        set_old_cols_to_null_query = "UPDATE " + sql_item_name(pipe.target, self.flavor)
        for col, typ in altered_cols_types.items():
            set_old_cols_to_null_query += (
                "\nSET " + sql_item_name(col, self.flavor)
                + ' = NULL,'
            )
        set_old_cols_to_null_query = set_old_cols_to_null_query[:-1]
        queries.append(set_old_cols_to_null_query)

        alter_type_query = "ALTER TABLE " + sql_item_name(pipe.target, self.flavor)
        for col, typ in altered_cols_types.items():
            alter_type_query += (
                "\nMODIFY " + sql_item_name(col, self.flavor) + ' '
                + typ + ','
            )
        alter_type_query = alter_type_query[:-1]
        queries.append(alter_type_query)

        set_old_to_temp_query = "UPDATE " + sql_item_name(pipe.target, self.flavor)
        for col, typ in altered_cols_types.items():
            set_old_to_temp_query += (
                "\nSET " + sql_item_name(col, self.flavor)
                + ' = ' + sql_item_name(col + '_temp', self.flavor) + ','
            )
        set_old_to_temp_query = set_old_to_temp_query[:-1]
        queries.append(set_old_to_temp_query)

        drop_temp_query = "ALTER TABLE " + sql_item_name(pipe.target, self.flavor)
        for col, typ in altered_cols_types.items():
            drop_temp_query += (
                "\nDROP COLUMN " + sql_item_name(col + '_temp', self.flavor) + ','
            )
        drop_temp_query = drop_temp_query[:-1]
        queries.append(drop_temp_query)

        return queries


    query = "ALTER TABLE " + sql_item_name(pipe.target, self.flavor)
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
            + sql_item_name(col, self.flavor)
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
        pipe: 'meerschaum.Pipe',
        df: 'pd.DataFrame',
        update_dtypes: bool = True,
    ) -> Dict[str, 'sqlalchemy.sql.visitors.TraversibleType']:
    """
    Given a pipe and DataFrame, return the `dtype` dictionary for `to_sql()`.

    Parameters
    ----------
    pipe: meerschaum.Pipe
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
    from meerschaum.utils.sql import get_db_type
    df_dtypes = {col: str(typ) for col, typ in df.dtypes.items()}
    if update_dtypes:
        df_dtypes.update(pipe.dtypes)
    return {
        col: get_db_type(typ, self.flavor, as_sqlalchemy=True)
        for col, typ in df_dtypes.items()
    }
