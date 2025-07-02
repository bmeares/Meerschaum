#! /usr/bin/env python3
# vim:fenc=utf-8

"""
Define the properties of a long-lived access token.
"""

import uuid
from typing import Optional, Union
from datetime import datetime, timedelta, timezone


class Token:
    """
    Tokens (long lived access tokens) may be registered and revoked to provide easier authentication (e.g. IoT devices).
    Tokens must be tied to a Meerschaum user account.
    """

    def __init__(
        self,
        user_id: Union[str, int, uuid.UUID],
        expiration: Optional[datetime] = None,
        ttl: Optional[timedelta] = None,
        secret: Optional[str] = None,
    ):
        from meerschaum.utils.dtypes import coerce_timezone
        now = datetime.now(timezone.utc)
        self.user_id = user_id
        if expiration is not None:
            self.expiration = coerce_timezone(expiration)
        elif ttl is not None:
            self.expiration = now + ttl
        else:
            self.expiration = None
        self.secret = secret
