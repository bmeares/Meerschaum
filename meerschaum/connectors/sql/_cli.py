#! /usr/bin/env python
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

"""
Launch into a CLI environment to interact with the SQL Connector
"""

from __future__ import annotations
import os
import json
import copy
### NOTE: This import adds `Iterable` to collections, which is needed by some CLIs.
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
        debug: bool = False,
    ) -> SuccessTuple:
    """
    Launch a subprocess for an interactive CLI.
    """
    from meerschaum.utils.venv import venv_exec
    env = copy.deepcopy(dict(os.environ))
    env[f'MRSM_SQL_{self.label.upper()}'] = json.dumps(self.meta)
    cli_code = (
        "import sys\n"
        "import meerschaum as mrsm\n"
        f"conn = mrsm.get_connector('sql:{self.label}')\n"
        "success, msg = conn._cli_exit()\n"
        "mrsm.pprint((success, msg))\n"
        "if not success:\n"
        "    raise Exception(msg)"
    )
    try:
        _ = venv_exec(cli_code, venv=None, debug=debug, capture_output=False)
    except Exception as e:
        return False, f"[{self}] Failed to start CLI:\n{e}"
    return True, "Success"


def _cli_exit(
        self,
        debug: bool = False
    ) -> SuccessTuple:
    """Launch an interactive CLI for the SQLConnector's flavor."""
    from meerschaum.utils.packages import venv_exec, attempt_import
    from meerschaum.utils.debug import dprint
    from meerschaum.utils.warnings import error
    import sys, subprocess, os

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
    elif self.flavor == 'mssql':
        if 'DOTNET_SYSTEM_GLOBALIZATION_INVARIANT' not in os.environ:
            os.environ['DOTNET_SYSTEM_GLOBALIZATION_INVARIANT'] = '1'

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
    if cli_arg_str.startswith('postgresql+psycopg://'):
        cli_arg_str = cli_arg_str.replace('postgresql+psycopg://', 'postgresql://')

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

    try:
        if debug:
            dprint(f'Launching CLI:\n{launch_cli}')
        exec(launch_cli)
        success, msg = True, 'Success'
    except Exception as e:
        success, msg = False, str(e)

    return success, msg
