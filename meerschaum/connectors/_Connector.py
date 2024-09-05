#! /usr/bin/env python
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

"""
Define the parent `Connector` class.
"""

from __future__ import annotations
import abc
import copy
from meerschaum.utils.typing import Iterable, Optional, Any, Union, List, Dict

class InvalidAttributesError(Exception):
    """
    Raised when the incorrect attributes are set in the Connector.
    """

class Connector(metaclass=abc.ABCMeta):
    """
    The base connector class to hold connection attributes.
    """
    def __init__(
        self,
        type: Optional[str] = None,
        label: Optional[str] = None,
        **kw: Any
    ):
        """
        Set the given keyword arguments as attributes.

        Parameters
        ----------
        type: str
            The `type` of the connector (e.g. `sql`, `api`, `plugin`).

        label: str
            The `label` for the connector.


        Examples
        --------
        Run `mrsm edit config` and to edit connectors in the YAML file:

        ```yaml
        meerschaum:
            connections:
                {type}:
                    {label}:
                        ### attributes go here
        ```

        """
        self._original_dict = copy.deepcopy(self.__dict__)
        self._set_attributes(type=type, label=label, **kw)

        ### NOTE: Override `REQUIRED_ATTRIBUTES` if `uri` is set.
        self.verify_attributes(
            ['uri']
            if 'uri' in self.__dict__
            else getattr(self, 'REQUIRED_ATTRIBUTES', None)
        )

    def _reset_attributes(self):
        self.__dict__ = self._original_dict

    def _set_attributes(
        self,
        *args,
        inherit_default: bool = True,
        **kw: Any
    ):
        from meerschaum.config.static import STATIC_CONFIG
        from meerschaum.utils.warnings import error

        self._attributes = {}

        default_label = STATIC_CONFIG['connectors']['default_label']

        ### NOTE: Support the legacy method of explicitly passing the type.
        label = kw.get('label', None)
        if label is None:
            if len(args) == 2:
                label = args[1]
            elif len(args) == 0:
                label = None
            else:
                label = args[0]

        if label == 'default':
            error(
                f"Label cannot be 'default'. Did you mean '{default_label}'?",
                InvalidAttributesError,
            )
        self.__dict__['label'] = label

        from meerschaum.config import get_config
        conn_configs = copy.deepcopy(get_config('meerschaum', 'connectors'))
        connector_config = copy.deepcopy(get_config('system', 'connectors'))

        ### inherit attributes from 'default' if exists
        if inherit_default:
            inherit_from = 'default'
            if self.type in conn_configs and inherit_from in conn_configs[self.type]:
                _inherit_dict = copy.deepcopy(conn_configs[self.type][inherit_from])
                self._attributes.update(_inherit_dict)

        ### load user config into self._attributes
        if self.type in conn_configs and self.label in conn_configs[self.type]:
            self._attributes.update(conn_configs[self.type][self.label] or {})

        ### load system config into self._sys_config
        ### (deep copy so future Connectors don't inherit changes)
        if self.type in connector_config:
            self._sys_config = copy.deepcopy(connector_config[self.type])

        ### add additional arguments or override configuration
        self._attributes.update(kw)

        ### finally, update __dict__ with _attributes.
        self.__dict__.update(self._attributes)

    def verify_attributes(
        self,
        required_attributes: Optional[List[str]] = None,
        debug: bool = False,
    ) -> None:
        """
        Ensure that the required attributes have been met.
        
        The Connector base class checks the minimum requirements.
        Child classes may enforce additional requirements.

        Parameters
        ----------
        required_attributes: Optional[List[str]], default None
            Attributes to be verified. If `None`, default to `['label']`.

        debug: bool, default False
            Verbosity toggle.

        Returns
        -------
        Don't return anything.

        Raises
        ------
        An error if any of the required attributes are missing.
        """
        from meerschaum.utils.warnings import error, warn
        from meerschaum.utils.debug import dprint
        from meerschaum.utils.misc import items_str
        if required_attributes is None:
            required_attributes = ['label']

        missing_attributes = set()
        for a in required_attributes:
            if a not in self.__dict__:
                missing_attributes.add(a)
        if len(missing_attributes) > 0:
            error(
                (
                    f"Missing {items_str(list(missing_attributes))} "
                    + f"for connector '{self.type}:{self.label}'."
                ),
                InvalidAttributesError,
                silent=True,
                stack=False
            )


    def __str__(self):
        """
        When cast to a string, return type:label.
        """
        return f"{self.type}:{self.label}"

    def __repr__(self):
        """
        Represent the connector as type:label.
        """
        return str(self)

    @property
    def meta(self) -> Dict[str, Any]:
        """
        Return the keys needed to reconstruct this Connector.
        """
        _meta = {
            key: value
            for key, value in self.__dict__.items()
            if not str(key).startswith('_')
        }
        _meta.update({
            'type': self.type,
            'label': self.label,
        })
        return _meta


    @property
    def type(self) -> str:
        """
        Return the type for this connector.
        """
        _type = self.__dict__.get('type', None)
        if _type is None:
            import re
            is_executor = self.__class__.__name__.lower().endswith('executor')
            suffix_regex = (
                r'connector$'
                if not is_executor
                else r'executor$'
            )
            _type = re.sub(suffix_regex, '', self.__class__.__name__.lower())
            self.__dict__['type'] = _type
        return _type


    @property
    def label(self) -> str:
        """
        Return the label for this connector.
        """
        _label = self.__dict__.get('label', None)
        if _label is None:
            from meerschaum.config.static import STATIC_CONFIG
            _label = STATIC_CONFIG['connectors']['default_label']
            self.__dict__['label'] = _label
        return _label

