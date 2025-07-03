#! /usr/bin/env python3
# vim:fenc=utf-8

"""
Response models for tokens.
"""

from datetime import datetime
from typing import Optional

import meerschaum as mrsm

from pydantic import BaseModel


class RegisterTokenResponseModel(BaseModel):
    label: str
    secret: str
    expires_at: Optional[datetime]
