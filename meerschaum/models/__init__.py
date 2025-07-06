#! /usr/bin/env python3
# vim:fenc=utf-8

"""
Define fundamental Pydantic models (to be built upon for API response models).
"""

import meerschaum as mrsm

annotated_types = mrsm.attempt_import('annotated_types', lazy=False)
pydantic = mrsm.attempt_import('pydantic', lazy=False)

from meerschaum.models.pipes import (
    PipeModel,
    PipeWithParametersModel,
    PipesWithParametersDictModel,
    ConnectorKeysModel,
    MetricKeyModel,
    LocationKeyModel,
    InstanceKeysModel,
)
from meerschaum.models.users import UserModel
from meerschaum.models.tokens import TokenModel

__all__ = (
    'PipeModel',
    'PipeWithParametersModel',
    'PipesWithParametersDictModel',
    'ConnectorKeysModel',
    'MetricKeyModel',
    'LocationKeyModel',
    'InstanceKeysModel',
    'UserModel',
    'TokenModel',
)
