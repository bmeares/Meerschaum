#! /usr/bin/env python
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

"""
Launch into a CLI environment to interact with the SQL Connector
"""

from __future__ import annotations
from meerschaum.utils.typing import SuccessTuple

flavor_clis = {
    'postgresql'  : 'pgcli',
    'timescaledb' : 'pgcli',
    'cockroachdb' : 'pgcli',
    'citus'       : 'pgcli',
    'mysql'       : 'mycli',
    'mariadb'     : 'mycli',
    'percona'     : 'mycli',
    'sqlite'      : 'litecli',
    'mssql'       : 'mssqlcli',
    'duckdb'      : 'gadwall',
}
cli_deps = {
    'pgcli': ['pgspecial', 'pendulum'],
    'mycli': ['cryptography'],
}

def cli(
        self,
        debug : bool = False
    ) -> SuccessTuple:
    """Launch an interactive CLI for the SQLConnector's flavor."""
    from meerschaum.utils.packages import venv_exec, attempt_import
    from meerschaum.utils.debug import dprint
    from meerschaum.utils.warnings import error
    import sys, subprocess

    if self.flavor not in flavor_clis:
        return False, f"No CLI available for flavor '{self.flavor}'."

    if self.flavor == 'duckdb':
        gadwall = attempt_import('gadwall', debug = debug, lazy=False)
        gadwall_shell = gadwall.Gadwall(self.database)
        try:
            gadwall_shell.cmdloop()
        except KeyboardInterrupt:
            pass
        return True, "Success"

    cli_name = flavor_clis[self.flavor]

    ### Install the CLI package and any dependencies.
    cli, cli_main = attempt_import(cli_name, (cli_name + '.main'), lazy=False, debug=debug)
    if cli_name in cli_deps:
        for dep in cli_deps[cli_name]:
            locals()[dep] = attempt_import(dep, lazy=False, warn=False, debug=debug)

    ### NOTE: The `DATABASE_URL` property must be initialized first in case the database is not
    ### yet defined (e.g. 'sql:local').
    cli_arg_str = self.DATABASE_URL
    if self.flavor in ('sqlite', 'duckdb'):
        cli_arg_str = str(self.database)

    ### Define the script to execute to launch the CLI.
    ### The `mssqlcli` script is manually written to avoid telemetry
    ### and because `main.cli()` is not defined.
    launch_cli = f"cli_main.cli(['{cli_arg_str}'])"
    if self.flavor == 'mssql':
        launch_cli = (
            "mssqlclioptionsparser, mssql_cli = attempt_import("
            + "'mssqlcli.mssqlclioptionsparser', 'mssqlcli.mssql_cli', lazy=False)\n"
            + "ms_parser = mssqlclioptionsparser.create_parser()\n"
            + f"ms_options = ms_parser.parse_args(['--server', 'tcp:{self.host},{self.port}', "
            + f"'--database', '{self.database}', "
            + f"'--username', '{self.username}', '--password', '{self.password}'])\n"
            + "ms_object = mssql_cli.MssqlCli(ms_options)\n"
            + "try:\n"
            + "    ms_object.connect_to_database()\n"
            + "    ms_object.run()\n"
            + "finally:\n"
            + "    ms_object.shutdown()"
        )
    elif self.flavor == 'duckdb':
        launch_cli = ()

    try:
        if debug:
            dprint(f'Launching CLI:\n{launch_cli}')
        exec(launch_cli)
        success, msg = True, 'Success'
    except Exception as e:
        success, msg = False, str(e)

    return success, msg
