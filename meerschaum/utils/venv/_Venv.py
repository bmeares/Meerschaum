#! /usr/bin/env python3
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

"""
Define a context manager for virtual environments.
"""

from __future__ import annotations

import copy
import pathlib
from meerschaum.utils.typing import Union
from meerschaum.utils.threading import RLock


class Venv:
    """
    Manage a virtual enviroment's activation status.

    Examples
    --------
    >>> from meerschaum.plugins import Plugin
    >>> with Venv('mrsm') as venv:
    ...     import pandas
    >>> with Venv(Plugin('noaa')) as venv:
    ...     import requests
    >>> venv = Venv('mrsm')
    >>> venv.activate()
    True
    >>> venv.deactivate()
    True
    >>> 
    """

    def __init__(
            self,
            venv: Union[str, 'meerschaum.plugins.Plugin', None] = 'mrsm',
            debug: bool = False,
        ) -> None:
        from meerschaum.utils.venv import activate_venv, deactivate_venv, active_venvs
        ### For some weird threading issue,
        ### we can't use `isinstance` here.
        if 'meerschaum.plugins._Plugin' in str(type(venv)):
            self._venv = venv.name
            self._activate = venv.activate_venv
            self._deactivate = venv.deactivate_venv
            self._kwargs = {}
        else:
            self._venv = venv
            self._activate = activate_venv
            self._deactivate = deactivate_venv
            self._kwargs = {'venv': venv}
        self._debug = debug
        ### In case someone calls `deactivate()` before `activate()`.
        self._kwargs['previously_active_venvs'] = copy.deepcopy(active_venvs)


    def activate(self, debug: bool = False) -> bool:
        """
        Activate this virtual environment.
        If a `meerschaum.plugins.Plugin` was provided, its dependent virtual environments
        will also be activated.
        """
        from meerschaum.utils.venv import active_venvs
        self._kwargs['previously_active_venvs'] = copy.deepcopy(active_venvs)
        return self._activate(debug=(debug or self._debug), **self._kwargs)


    def deactivate(self, debug: bool = False) -> bool:
        """
        Deactivate this virtual environment.
        If a `meerschaum.plugins.Plugin` was provided, its dependent virtual environments
        will also be deactivated.
        """
        return self._deactivate(debug=(debug or self._debug), **self._kwargs)


    @property
    def target_path(self) -> pathlib.Path:
        """
        Return the target site-packages path for this virtual environment.
        A `meerschaum.utils.venv.Venv` may have one virtual environment per minor Python version
        (e.g. Python 3.10 and Python 3.7).
        """
        from meerschaum.utils.venv import venv_target_path
        return venv_target_path(venv=self._venv, allow_nonexistent=True, debug=self._debug)


    @property
    def root_path(self) -> pathlib.Path:
        """
        Return the top-level path for this virtual environment.
        """
        from meerschaum.config._paths import VIRTENV_RESOURCES_PATH
        return VIRTENV_RESOURCES_PATH / self._venv


    def __enter__(self) -> None:
        self.activate(debug=self._debug)


    def __exit__(self, exc_type, exc_value, exc_traceback) -> None:
        self.deactivate(debug=self._debug)


    def __str__(self) -> str:
        quote = "'" if self._venv is not None else ""
        return "Venv(" + quote + str(self._venv) + quote + ")"


    def __repr__(self) -> str:
        return self.__str__()
