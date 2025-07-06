#! /usr/bin/env python
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

"""
Pydantic model for a pipe's keys.
"""

from __future__ import annotations

from typing import Optional, List, Tuple

import meerschaum as mrsm

from meerschaum.models.pipes import (
    PipeModel as BasePipeModel,
    ConnectorKeysModel,
    MetricKeyModel,
    LocationKeyModel,
)

pydantic = mrsm.attempt_import('pydantic', lazy=False)
from pydantic import (
    BaseModel,
    RootModel,
    field_validator,
    ValidationInfo,
    ConfigDict,
)


class PipeModel(BasePipeModel):
    """
    A `Pipe`'s model to be used in API responses.
    """
    parameters: Optional[dict] = None
    model_config = ConfigDict(
        json_schema_extra={
            'example': {
                'connector_keys': 'sql:main',
                'metric_key': 'weather',
                'location_key': 'us.co.denver',
                'instance_keys': 'sql:main',
                'parameters': {
                    'columns': {
                        'datetime': 'dt',
                        'id': 'id',
                        'value': 'val',
                    },
                },
            },
        },
    )


class FetchPipesKeysResponseModel(
    RootModel[List[Tuple[ConnectorKeysModel, MetricKeyModel, LocationKeyModel]]]
):
    """
    A list of tuples containing connector, metric, and location keys.
    """
    model_config = ConfigDict(
        json_schema_extra={
            'example': [
                ['sql:main', 'weather', 'us.co.denver'],
                ['plugin:noaa', 'weather', 'us.co.boulder'],
            ],
        },
    )