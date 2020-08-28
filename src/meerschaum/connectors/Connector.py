#! /usr/bin/env python
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

"""
Generic Connector class
Defines basic data that Connectors should contain
"""

from meerschaum.config import config as cf
conn_configs = cf['meerschaum']['connections']

class Connector:
    def __init__(self, conn_type : str = None, conn_label : str = "main", **kw):
        """
        conn_type : str
            The type of the connection. Used as a key in config.yaml to get attributes.
            Valid types: ['sql', 'api' TODO, 'mqtt' TODO, 'metasys' TODO?, 'sas' TODO?]

        conn_label : str
            The label for the connection. Used as a key within config.yaml

        If config.yaml is set for the given type and label, the hierarchy looks like so:
        meerschaum:
            connections:
                {conn_type}:
                    {conn_label}:
                        ### attributes go here

        Read config.yaml for attributes partitioned by connection type and connection label.
        Example: type="sql", label="main"
        """
        self.type, self.label = conn_type, conn_label

        self.__dict__.update(kw)
        if self.type in conn_configs and self.label in conn_configs[self.type]:
            self.__dict__.update(conn_configs[self.type][self.label])

    def verify_attributes(
            self,
            required_attributes : set = {
                'label'
            },
            debug=False
        ):
        """
        Ensure that the required attributes have been met.
        
        required_attributes : set
            Attributes to be verified.

        The Connector base class checks the minimum requirements.
        Child classes may enforce additional requirements.
        """
        if debug:
            print(f'required attributes: {required_attributes}')
            print(f'attributes: {self.__dict__}')
        missing_attributes = set()
        for a in required_attributes:
            if a not in self.__dict__:
                missing_attributes.add(a)
        if len(missing_attributes) > 0:
            raise Exception(
                f"Please provide connection configuration for type: {self.type}, label: {self.label} "
                f"in config.yaml or as arguments for the Connector.\n"
                f"Missing attributes: {missing_attributes}"
            )

    def __str__(self):
        return f'Meerschaum {self.type.upper()} Connector: {self.label}'

    def __repr__(self):
        return str(self)
