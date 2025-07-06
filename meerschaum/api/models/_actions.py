#! /usr/bin/env python3
# vim:fenc=utf-8

"""
Define actions response models.
"""

from typing import Tuple
from pydantic import RootModel, ConfigDict
from meerschaum.utils.typing import SuccessTuple


class SuccessTupleResponseModel(RootModel[SuccessTuple]):
    """
    A response model for a tuple of a boolean and a string.
    E.g. `[true, "Success"]`
    """
    model_config = ConfigDict(
        json_schema_extra={
            'example': [True, "Success"],
        },
    )
