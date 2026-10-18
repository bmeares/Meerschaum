"""Create or drop the disposable server-side objects of a flavor, through v4's own connector.

usage: python v4_admin.py create|drop <flavor>      (run with the v4 interpreter)

  postgis / timescaledb / postgresql   schema `trn` (dropped with CASCADE); engine rows that v5 wrote about
                                       these pipes into `_mrsm_internal` (tables namespaced `_trn`) are dropped
  mariadb / mysql / mssql              database `trndb`
  oracle                               common user `C##TRN` (dropped with CASCADE)
"""
import sys
import pathlib

HERE = pathlib.Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))
import meerschaum as mrsm  # noqa: E402
from flavors import admin_attrs, flavor_attrs, PRIVATE_DATABASE, PRIVATE_USER, SCHEMA_FLAVORS  # noqa: E402

action, flavor = sys.argv[1], sys.argv[2]
conn = mrsm.get_connector('sql', 'trn_admin', **admin_attrs(flavor))


def run(q):
    r = conn.exec(q)
    print('[admin]', q[:100], '->', 'ok' if r is not None and r is not False else r)


if flavor in SCHEMA_FLAVORS:
    schema = flavor_attrs(flavor, '')['schema']
    if action == 'create':
        run(f'CREATE SCHEMA IF NOT EXISTS {schema}')
    else:
        run(f'DROP SCHEMA IF EXISTS {schema} CASCADE')
        # v5 keeps the engine state of schema `trn` in `_mrsm_internal`, in tables namespaced by the schema
        names = conn.read(
            "SELECT table_name FROM information_schema.tables WHERE table_schema = '_mrsm_internal' "
            "AND table_name ~ '^mrsm_engine_[a-z]+_trn(_p_[0-9]+)?$'"
        )
        for t in (names['table_name'].tolist() if names is not None else []):
            run(f'DROP TABLE IF EXISTS _mrsm_internal."{t}" CASCADE')
elif flavor in PRIVATE_USER:
    if action == 'create':
        run("CREATE USER C##TRN IDENTIFIED BY trnpass1234 QUOTA UNLIMITED ON USERS")
        run("GRANT CONNECT, RESOURCE, CREATE VIEW, CREATE PROCEDURE TO C##TRN")
    else:
        run("DROP USER C##TRN CASCADE")
elif flavor in PRIVATE_DATABASE:
    if flavor == 'mssql':
        if action == 'create':
            run("IF DB_ID('trndb') IS NULL CREATE DATABASE trndb")
            inst = mrsm.get_connector('sql', 'trn_admin_db', **flavor_attrs('mssql', ''))
            r = inst.exec("IF NOT EXISTS (SELECT * FROM sys.schemas WHERE name = 'trn') EXEC('CREATE SCHEMA trn')")
            print('[admin] CREATE SCHEMA trn ->', 'ok' if r is not None and r is not False else r)
        else:
            run("IF DB_ID('trndb') IS NOT NULL BEGIN ALTER DATABASE trndb SET SINGLE_USER WITH ROLLBACK IMMEDIATE; DROP DATABASE trndb END")
    else:
        run('CREATE DATABASE IF NOT EXISTS trndb' if action == 'create' else 'DROP DATABASE IF EXISTS trndb')
