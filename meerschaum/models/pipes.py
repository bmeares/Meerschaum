#! /usr/bin/env python
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

"""
Pydantic model for a pipe's keys.
"""

from __future__ import annotations

from typing import Optional, List, Tuple, Dict, Any

import meerschaum as mrsm

pydantic = mrsm.attempt_import('pydantic', lazy=False)
from pydantic import (
    BaseModel,
    RootModel,
    field_validator,
    ValidationInfo,
    ConfigDict,
)


class ConnectorKeysModel(RootModel[str]):
    """
    Validate the connector keys:
        - May not begin with an underscore (_).
        - Must be at least one character long.
        - May contain one or zero colons (:).
    """
    model_config = ConfigDict(
        frozen=True,
        json_schema_extra={
            'example': 'sql:main',
        },
    )
    @field_validator('root')
    @classmethod
    def validate_connector_keys(cls, v: str, info: ValidationInfo) -> str:
        """Validate the connector keys."""
        if not v:
            raise ValueError("Connector keys must be at least one character long.")
        if v.startswith('_'):
            raise ValueError("Connector keys may not begin with an underscore.")
        if v.count(':') > 1:
            raise ValueError("Connector keys may contain at most one colon.")
        return v


class MetricKeyModel(RootModel[str]):
    """
    Validate the metric key:

        - May not begin with an underscore (_).
        - Must be at least one character long.
    """
    model_config = ConfigDict(
        frozen=True,
        json_schema_extra={
            'example': 'weather',
        },
    )
    @field_validator('root')
    @classmethod
    def validate_metric_key(cls, v: str, info: ValidationInfo) -> str:
        """Validate the metric key."""
        if not v:
            raise ValueError("Metric key must be at least one character long.")
        if v.startswith('_'):
            raise ValueError("Metric key may not begin with an underscore.")
        return v


class LocationKeyModel(RootModel[Optional[str]]):
    """
    Validate the location key:

        - May not begin with an underscore (_).
        - May be null (`None`).
        - If not null, must be at least one character long.
    """
    model_config = ConfigDict(
        frozen=True,
        json_schema_extra={
            'example': 'us.co.denver',
        },
    )
    @field_validator('root')
    @classmethod
    def validate_location_key(cls, v: Optional[str], info: ValidationInfo) -> Optional[str]:
        """Validate the location key."""
        if v is None:
            return v
        if not v:
            raise ValueError("Location key must be at least one character long if not null.")
        if v.startswith('_'):
            raise ValueError("Location key may not begin with an underscore.")
        return v


class InstanceKeysModel(RootModel[str]):
    """
    Validate the instance keys. The instance keys are connector keys which must have one colon
    (e.g. `'sql:main'`).
    """
    model_config = ConfigDict(
        frozen=True,
        json_schema_extra={
            'example': 'sql:main',
        },
    )
    @field_validator('root')
    @classmethod
    def validate_instance_keys(cls, v: str, info: ValidationInfo) -> str:
        """Validate the instance keys."""
        if not v:
            raise ValueError("Instance keys must be at least one character long.")
        if v.startswith('_'):
            raise ValueError("Instance keys may not begin with an underscore.")
        if v.count(':') != 1:
            raise ValueError("Instance keys must contain exactly one colon.")
        return v


class PipeModel(BaseModel):
    """
    Define the four components to uniquely identify a pipe.
    """
    connector_keys: ConnectorKeysModel
    metric_key: MetricKeyModel
    location_key: Optional[LocationKeyModel] = None
    instance_keys: Optional[InstanceKeysModel] = None
    model_config = ConfigDict(
        json_schema_extra={
            'example': {
                'connector_keys': 'sql:main',
                'metric_key': 'weather',
                'location_key': 'atl',
                'instance_keys': 'sql:main',
            },
        },
    )


class PipeWithParametersModel(PipeModel):
    """
    A Pipe's model including its parameters dictionary.
    """
    parameters: Dict[str, Any]
    model_config = ConfigDict(
        json_schema_extra={
            'example': {
                'connector_keys': 'sql:main',
                'metric_key': 'weather',
                'location_key': 'atl',
                'instance_keys': 'sql:main',
                'parameters': {
                    'columns': {
                        'datetime': 'dt',
                        'id': 'id',
                    },
                },
            },
        },
    )

class PipesWithParametersDictModel(
    RootModel[
        Dict[
            str,
            Dict[
                str,
                Dict[
                    str,
                    PipeWithParametersModel
                ]
            ]
        ]
    ]
):
    """
    A dictionary of pipes' parameters, nested by connector, metric, and location keys.
    """
    @field_validator('root')
    @classmethod
    def validate_keys(cls, v: dict, info: ValidationInfo) -> dict:
        """
        Validate the dictionary keys.
        """
        for c_key, metrics in v.items():
            ConnectorKeysModel(root=c_key)
            for m_key, locations in metrics.items():
                MetricKeyModel(root=m_key)
                for l_key in locations:
                    LocationKeyModel(root=l_key if l_key not in ('None', 'null', '[None]') else None)
        return v

    model_config = ConfigDict(
        json_schema_extra={
            'example': {
                "plugin:noaa": {
                    "weather": {
                        "None": {
                            "pipe_id": 1,
                            "connector_keys": "plugin:noaa",
                            "metric_key": "weather",
                            "location_key": "clemson",
                            "parameters": {
                                "columns": {
                                    "datetime": "timestamp",
                                    "id": "station"
                                },
                                "noaa": {
                                    "stations": {
                                        "KCEU": {
                                            "name": "Clemson, Clemson-Oconee County Airport",
                                            "geometry": {
                                            "type": "Point",
                                                "coordinates": [
                                                    -82.88139,
                                                    34.67222
                                                ]
                                            }
                                        }
                                    }
                                },
                                "verify": {
                                    "chunk_minutes": 20160
                                },
                                "target": "plugin_noaa_weather",
                                "tags": [],
                                "dtypes": {
                                    "timestamp": "datetime",
                                    "geometry": "json",
                                    "cloudLayers": "json",
                                    "presentWeather": "json"
                                },
                                "static": False,
                                "enforce": True
                            }
                        }
                    }
                }
            },
        },
    )
