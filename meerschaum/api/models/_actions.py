#! /usr/bin/env python3
# vim:fenc=utf-8

"""
Define actions response models.
"""

from typing import Tuple
from pydantic import RootModel, ConfigDict


class SuccessTupleResponseModel(RootModel[Tuple[bool, str]]):
    """
    A response model for a tuple of a boolean and a string.
    E.g. `[true, "Success"]`
    """
    model_config = ConfigDict(
        json_schema_extra={
            'example': [True, "Success"],
        },
    )
