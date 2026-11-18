"""Logical connectors whose data operations use the current launch's main API grant."""

from __future__ import annotations

import copy
import os

from meerschaum.connectors.api._APIConnector import APIConnector


class APIExecutionConnector(APIConnector):
    """Keep saved connector keys while using an API transport without database credentials."""

    def __init__(self, keys: str, attributes: dict):
        from meerschaum import _core

        # The native factory strictly validates the entire section, including its logical keys.
        client = _core.ApiConnector.from_execution_alias(keys, attributes)
        typ, label = keys.split(':', 1)
        descriptor = copy.deepcopy(attributes['mrsm_api_alias'])
        self.__dict__.update({
            'type': typ,
            'label': label,
            'mrsm_api_alias': descriptor,
            'url': descriptor['callback_url'].rstrip('/'),
            'protocol': descriptor['callback_url'].split(':', 1)[0],
            '_attributes': copy.deepcopy(attributes),
            '_explicit_attributes': copy.deepcopy(attributes),
            '_instance_keys': keys,
            '_sys_config': {},
            '_rust_client_obj': client,
            '_rust_client_pid': os.getpid(),
            '_rust_alias': copy.deepcopy(descriptor),
        })

    def _rust_client(self):
        from meerschaum import _core

        descriptor = self.__dict__['mrsm_api_alias']
        if (
            self.__dict__.get('_rust_client_obj') is None
            or self.__dict__.get('_rust_client_pid') != os.getpid()
            or self.__dict__.get('_rust_alias') != descriptor
        ):
            self.__dict__['_rust_client_obj'] = _core.ApiConnector.from_execution_alias(
                str(self), {'mrsm_api_alias': descriptor},
            )
            self.__dict__['_rust_client_pid'] = os.getpid()
            self.__dict__['_rust_alias'] = copy.deepcopy(descriptor)
        return self.__dict__['_rust_client_obj']

    @property
    def meta(self):
        return {
            'type': self.type,
            'label': self.label,
            'mrsm_api_alias': copy.deepcopy(self.__dict__['mrsm_api_alias']),
        }

    @property
    def engine(self):
        raise RuntimeError(
            'This connector uses the API. Use Pipe data operations instead of a SQL engine.'
        )

    def fetch(self, pipe, begin='', end=None, params=None, **kw):
        if self.type == 'api':
            return super().fetch(pipe, begin=begin, end=end, params=params, **kw)
        if self.type != 'sql':
            raise NotImplementedError('This logical source has no native SQL broker.')
        return self._rust_client().fetch_registered_source(
            pipe, begin=begin, end=end, params=params, **kw,
        )
