#! /usr/bin/env python
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

"""
Generic Connector class
Defines basic data that Connectors should contain
"""

from meerschaum.utils.debug import dprint

class Connector:
    def __init__(
            self,
            type : str = None,
            label : str = "main",
            pandas : str = None,
            **kw
        ):
        """
        type : str
            The type of the connection. Used as a key in config.yaml to get attributes.
            Valid types: ['sql', 'api' TODO, 'mqtt' TODO, 'metasys' TODO?, 'sas' TODO?]

        label : str
            The label for the connection. Used as a key within config.yaml

        pandas : str
            Custom pandas implementation name. Default is in system_config in meerschaum.config.
            May change to modin.pandas soon.

        If config.yaml is set for the given type and label, the hierarchy looks like so:
        meerschaum:
            connections:
                {type}:
                    {label}:
                        ### attributes go here

        Read config.yaml for attributes partitioned by connection type and connection label.
        Example: type="sql", label="main"
        """
        if label == 'default':
            raise Exception("Label cannot be 'default'. Did you mean 'main'?")
        self.type, self.label = type, label

        from meerschaum.config import get_config
        conn_configs = get_config('meerschaum', 'connectors')
        connector_config = get_config('system', 'connectors')

        ### inherit attributes from 'default' if exists
        inherit_from = 'default'
        if self.type in conn_configs and inherit_from in conn_configs[self.type]:
            self.__dict__.update(conn_configs[self.type][inherit_from])

        ### load user config into self.__dict__
        if self.type in conn_configs and self.label in conn_configs[self.type]:
            self.__dict__.update(conn_configs[self.type][self.label])

        ### load system config into self.system_config
        ### (deep copy so future Connectors don't inherit changes)
        if self.type in connector_config:
            from copy import deepcopy
            self.sys_config = deepcopy(connector_config[self.type])

        ### add additional arguments or override configuration
        self.__dict__.update(kw)

        ### handle custom pandas implementation (e.g. modin)
        pandas = pandas if pandas is not None else connector_config['all']['pandas']
        self._pandas_name = pandas

    @property
    def pd(self):
        if '_pd' not in self.__dict__:
            from meerschaum.utils.misc import attempt_import
            self._pd = attempt_import(self._pandas_name)
        return self._pd

    def verify_attributes(
            self,
            required_attributes : set = {
                'label'
            },
            debug : bool = False
        ):
        """
        Ensure that the required attributes have been met.
        
        required_attributes : set
            Attributes to be verified.

        The Connector base class checks the minimum requirements.
        Child classes may enforce additional requirements.
        """
        from meerschaum.utils.warnings import error, warn
        if debug:
            dprint(f'required attributes: {required_attributes}')
            dprint(f'attributes: {self.__dict__}')
        missing_attributes = set()
        for a in required_attributes:
            if a not in self.__dict__:
                missing_attributes.add(a)
        if len(missing_attributes) > 0:
            error(
                f"Please provide connection configuration for connector: '{self.type}:{self.label}' "
                f"in the configuration file (open with `mrsm edit config`) or as arguments for the Connector.\n\n"
                f"Missing attributes: {missing_attributes}"
            )

    def fetch(
            self,
            instructions : dict
        ) -> 'pd.DataFrame':
        """
        Abstract method for all Connectors.
        In addition to specialized functionality, all Connectors must be able to fetch data
        based on criteria provided in the instructions dictionary.

        Returns pandas (or pandas derivative) DataFrame
        """
        raise NotImplementedError("fetch() must be implemented in children classes")

    def __str__(self):
        return f"{self.type}:{self.label}"
        #  return f'Meerschaum {self.type.upper()} Connector: {self.label}'

    def __repr__(self):
        return str(self)
