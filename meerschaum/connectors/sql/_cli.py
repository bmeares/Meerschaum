#! /usr/bin/env python
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

"""
Launch into a CLI environment to interact with the SQL Connector
"""

flavor_clis = {
    'postgres'    : 'pgcli',
    'timescaledb' : 'pgcli',
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
    from meerschaum.utils.misc import attempt_import, run_python_package
    from meerschaum.utils.debug import dprint
    from meerschaum.utils.warnings import error
    import sys, subprocess

    if self.flavor not in flavor_clis:
        error(f"No CLI available for flavor '{self.flavor}'.")

    ### get CLI module to launch
    cli_name = flavor_clis[self.flavor]
    if debug: dprint(f"Opening CLI '{cli_name}' for {self} (flavor '{self.flavor}')...")

    ### attempt an import to raise warnings if not installed
    cli = attempt_import(cli_name)

    ### open sqlalchemy engine URI or just database if sqlite
    cli_arg_str = self.DATABASE_URL
    if self.flavor == 'sqlite': cli_arg_str = self.database + ".sqlite"

    ### run the module in a subprocess because it calls sys.exit(), and __main__ does not
    ### work for these CLIs (something to do with Click?)
    launch_cli = f"from {cli_name} import main; main.cli(['{cli_arg_str}'])"

    if debug: dprint(launch_cli)
    return_code = subprocess.call([sys.executable, '-c', launch_cli]) == 0

    return return_code, f"CLI exited with response: {return_code}"
