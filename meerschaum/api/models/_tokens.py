#! /usr/bin/env python3
# vim:fenc=utf-8

"""
Response models for tokens.
"""

import uuid
from datetime import datetime
from typing import Optional, List, Union

import meerschaum as mrsm
from meerschaum._internal.static import STATIC_CONFIG

from pydantic import BaseModel, RootModel, Field, ConfigDict


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
    id: uuid.UUID
    api_key: str
    expiration: Optional[datetime]
    model_config = ConfigDict(
        json_schema_extra = {
            'examples': [
                {
                    'label': 'my-iot-device',
                    'secret': 'a_very_long_secret_string_that_is_only_shown_once',
                    'id': '1540c2f6-a99d-463c-bfab-47d361200123',
                    'expiration': '2026-01-01T00:00:00Z',
                    'api_key': 'mrsm-key:MTU0MGMyZjYtYTk5ZC00NjNjLWJmYWItNDdkMzYxMjAwMTIzOmFfdmVyeV9sb25nX3NlY3JldF9zdHJpbmdfdGhhdF9pc19vbmx5X3Nob3duX29uY2U=',
                }
            ]
        }
    )


class GetTokenResponseModel(BaseModel):
    id: Optional[uuid.UUID] = Field(default=None)
    creation: datetime = Field()
    expiration: Optional[datetime] = Field()
    label: str = Field()
    user_id: Optional[Union[int, str, uuid.UUID]] = Field(default=None)
    scopes: List[str] = Field(default=list(STATIC_CONFIG['tokens']['scopes']))
    is_valid: bool = Field(default=True)


class GetTokensResponseModel(RootModel[List[GetTokenResponseModel]]):
    model_config = ConfigDict(
        json_schema_extra={
            'example': [
                {
                    'label': 'my-iot-device',
                    'id': '1540c2f6-a99d-463c-bfab-47d361200123',
                    'user_id': 1,
                    'scopes': ['pipes:write'],
                    'creation': '2025-07-01T00:00:00Z',
                    'expiration': '2026-01-01T00:00:00Z',
                    'is_valid': True,
                },
            ],
        }
    )
