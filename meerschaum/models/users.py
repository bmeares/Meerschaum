#! /usr/bin/env python3
# vim:fenc=utf-8

"""
Define Pydantic models for users.
"""

from __future__ import annotations
from typing import Optional, Dict, Any, Union
import uuid

import meerschaum as mrsm

pydantic = mrsm.attempt_import('pydantic', lazy=False)
from pydantic import BaseModel, Field, ConfigDict


class UserModel(BaseModel):
    """Pydantic model for a Meerschaum User."""
    model_config = ConfigDict(from_attributes=True)

    user_id: Optional[Union[int, uuid.UUID]] = Field(default=None)
    username: str
    email: Optional[str] = Field(default=None)
    type: Optional[str] = Field(default=None)
    attributes: Optional[Dict[str, Any]] = Field(default=None)
