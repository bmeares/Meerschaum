#! /usr/bin/env python3
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

"""
Helpers the web console uses in place of the v4 internals that v5 deleted
(`api.routes._jobs._get_job`, `connectors.sql._fetch.get_pipe_query`, `api.routes._login.login`,
`_internal.shell.Shell.get_shell_intro`, ...). Each is a few lines over the v5 façade.
"""

from __future__ import annotations

import textwrap

import meerschaum as mrsm
from meerschaum.utils.typing import List, Optional, Union


def get_job(name: str, sysargs: Union[str, List[str], None] = None):
    """
    Return the job `name`: the `systemd` job if there is one, else the local job.
    """
    from meerschaum.jobs import Job
    systemd_job = Job(name, sysargs, executor_keys='systemd')
    if systemd_job.exists():
        return systemd_job
    return Job(name, sysargs, executor_keys='local')


def get_pipe_query(
    pipe: mrsm.Pipe,
    apply_symlinks: bool = True,
    warn: bool = True,
) -> Union[str, None]:
    """
    Return a pipe's SQL definition: the first of `fetch:definition`, `definition`, `query`
    and `sql` (v4's `get_pipe_query()`).
    """
    from meerschaum.utils.warnings import warn as _warn
    from meerschaum.utils.pipes import replace_pipes_syntax

    parameters = pipe.get_parameters(apply_symlinks=apply_symlinks)

    if (parameters.get('fetch', None) or {}).get('definition', None):
        definition = parameters['fetch']['definition']
    elif parameters.get('definition', None):
        definition = parameters['definition']
    elif parameters.get('query', None):
        definition = parameters['query']
    elif parameters.get('sql', None):
        definition = parameters['sql']
    else:
        if warn:
            _warn(
                f"Could not determine a SQL definition for {pipe}.\n"
                + "    Set the key `query` in `pipe.parameters` to a valid SQL query."
            )
        return None

    if apply_symlinks:
        definition = replace_pipes_syntax(definition, _pipe=pipe)

    return textwrap.dedent(definition.lstrip().rstrip())


def set_pipe_query(pipe: mrsm.Pipe, query: str, save=None) -> mrsm.SuccessTuple:
    """
    Set a pipe's SQL definition under the first of `fetch:definition`, `definition`, `query`
    and `sql` that the pipe already uses. `save` (given the patch) writes it instead of
    `pipe.update_parameters` (the console's checked write: `pipe_writes.update_parameters`).
    """
    parameters = pipe.get_parameters()
    ### v4 wrote `fetch: {defintion: ...}` here (a typo that lost the edit): the key is `definition`.
    if 'definition' in (parameters.get('fetch', None) or {}):
        patch_dict = {'fetch': {'definition': query}}
    elif 'definition' in parameters:
        patch_dict = {'definition': query}
    elif 'query' in parameters:
        patch_dict = {'query': query}
    else:
        patch_dict = {'sql': query}

    if save is not None:
        return save(patch_dict)
    return pipe.update_parameters(patch_dict)


def get_shell_intro(with_color: bool = False) -> str:
    """
    Return the shell's introduction banner (the console shows it until a terminal opens).
    """
    from meerschaum.utils.formatting import CHARSET
    from meerschaum import __version__ as version
    intro = mrsm.get_config('shell', CHARSET, 'intro') or 'Meerschaum'
    lines = str(intro).split('\n')
    width = max((len(line) for line in lines), default=0)
    return str(intro) + '\n' + (' ' * max(width - len('v' + version), 0)) + f'v{version}'


def authenticate_user(username: Optional[str], password: Optional[str]) -> bool:
    """
    Return whether `password` is `username`'s password on the API instance (v4's `/login` check).
    """
    from meerschaum.api import get_api_connector, no_auth, debug
    from meerschaum.core import User
    from meerschaum.core.User import verify_password

    if not username or not password:
        return False
    instance = get_api_connector()
    user = User(str(username), str(password), instance=instance)
    if no_auth:
        return True
    try:
        password_hash = instance.get_user_password_hash(user, debug=debug)
    except Exception:
        return False
    if not password_hash:
        return False
    return bool(verify_password(str(password), password_hash))
