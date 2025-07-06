#! /usr/bin/env python3
# vim:fenc=utf-8

"""
Define Pydantic models for tokens.
"""

import uuid
from typing import Optional, List, Union
from datetime import datetime

import meerschaum as mrsm

pydantic = mrsm.attempt_import('pydantic', lazy=False)
from pydantic import (
    BaseModel,
    RootModel,
    field_validator,
    ValidationInfo,
    ConfigDict,
    Field,
)
from meerschaum.models.users import UserModel

class TokenModel(BaseModel):
    """Pydantic model for a Meerschaum token."""
    model_config = ConfigDict(from_attributes=True)

    id: Optional[uuid.UUID] = Field(default=None)
    creation: Optional[datetime] = Field(default=None)
    expiration: Optional[datetime] = Field(default=None)
    label: Optional[str] = Field(default=None)
    user_id: Optional[Union[int, uuid.UUID]] = Field(default=None)
    secret_hash: Optional[str] = Field(default=None)
    scopes: Optional[List[str]] = Field(default=None)
    is_valid: bool = Field(default=True)
