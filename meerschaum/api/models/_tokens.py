#! /usr/bin/env python3
# vim:fenc=utf-8

"""
Response models for tokens.
"""

from datetime import datetime
from typing import Optional, List

import meerschaum as mrsm
from meerschaum._internal.static import STATIC_CONFIG

from pydantic import BaseModel, Field, ConfigDict


class RegisterTokenRequestModel(BaseModel):
    label: Optional[str] = None
    expiration: Optional[datetime] = None
    scopes: List[str] = Field(default_factory=lambda: list(STATIC_CONFIG['tokens']['scopes']))
    model_config = ConfigDict(
        json_schema_extra = {
            'examples': [
                {
                    'label': 'my-iot-device',
                    'expiration': '2026-01-01T00:00:00Z',
                    'scopes': list(STATIC_CONFIG['tokens']['scopes']),
                }
            ]
        }
    )


class RegisterTokenResponseModel(BaseModel):
    label: str
    secret: str
    expiration: Optional[datetime]
    model_config = ConfigDict(
        json_schema_extra = {
            'examples': [
                {
                    'label': 'my-iot-device',
                    'secret': 'a_very_long_secret_string_that_is_only_shown_once',
                    'expiration': '2026-01-01T00:00:00Z',
                }
            ]
        }
    )
