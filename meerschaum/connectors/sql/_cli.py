#! /usr/bin/env python
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

"""
Launch into a CLI environment to interact with the SQL Connector
"""

flavor_clis = {
    'postgresql'  : 'pgcli',
    'timescaledb' : 'pgcli',
    'cockroachdb' : 'pgcli',
    'mysql'       : 'mycli',
    'mariadb'     : 'mycli',
    'percona'     : 'mycli',
    'sqlite'      : 'litecli',
    'mssql'       : 'mssqlcli', ### NOTE: Not added as a dependency due to dependency problems
}

def cli(
        self,
        debug : bool = False
    ):
    """
    Launch an interactive CLI for the SQLConnector's flavor
    """
    from meerschaum.utils.packages import venv_exec, attempt_import
    from meerschaum.utils.debug import dprint
    from meerschaum.utils.warnings import error
    import sys, subprocess

    if self.flavor not in flavor_clis:
        return False, f"No CLI available for flavor '{self.flavor}'."

    ### get CLI module to launch
    cli_name = flavor_clis[self.flavor]
    if debug:
        dprint(f"Opening CLI '{cli_name}' for {self} (flavor '{self.flavor}')...")

    ### attempt an import to raise warnings if not installed
    cli = attempt_import(cli_name, lazy=False, debug=debug)

    ### open sqlalchemy engine URI or just database if sqlite
    cli_arg_str = self.DATABASE_URL
    if self.flavor == 'sqlite':
        cli_arg_str = str(self.database)

    ### run the module in a subprocess because it calls sys.exit(), and __main__ does not
    ### work for these CLIs (something to do with Click?)
    launch_cli = f"from {cli_name} import main; main.cli(['{cli_arg_str}'])"

    success = venv_exec(launch_cli, debug=debug)
    return success, f"CLI exited with response: {success}"
