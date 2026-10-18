#! /usr/bin/env python3
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

"""
The JWT login manager behind `meerschaum.api.manager` (`Depends(manager)` in `@api_plugin` routes).

v4 used `fastapi_login.LoginManager`. v5 keeps the same name, call signature and behavior, so a
plugin's `curr_user = fastapi.Depends(manager)` keeps working, without installing `fastapi-login`
or `python-jose`: the only third-party packages are `fastapi` (+ `starlette`, `anyio`) and `PyJWT`,
all in the `api` extra. Tokens are the HS256 JWTs the Rust server issues (`sub` = username or token
ID, `scopes`, `exp`), validated with the secret file both sides share.

Adapted from `fastapi-login` (MIT, MushroomMaula).
"""

from __future__ import annotations

import inspect
from datetime import datetime, timedelta, timezone

from typing import Any, Callable, Collection, Dict, Optional, Union

import jwt
from anyio.to_thread import run_sync
from fastapi import FastAPI, HTTPException, Request, Response
from fastapi.security import OAuth2PasswordBearer, SecurityScopes
from starlette.status import HTTP_400_BAD_REQUEST, HTTP_401_UNAUTHORIZED

InvalidCredentialsException = HTTPException(
    status_code=HTTP_401_UNAUTHORIZED,
    detail="Invalid credentials",
    headers={"WWW-Authenticate": "Bearer"},
)

InsufficientScopeException = HTTPException(
    status_code=HTTP_400_BAD_REQUEST,
    detail="Insufficient scope",
    headers={"WWW-Authenticate": "Bearer"},
)


class LoginManager(OAuth2PasswordBearer):
    """
    Resolve the user of a request from its bearer JWT (or cookie).
    Use it as a dependency (`fastapi.Depends(manager)`); register the user loader with
    `@manager.user_loader()`.
    """

    def __init__(
        self,
        secret: Union[str, bytes],
        token_url: str,
        algorithm: str = "HS256",
        use_cookie: bool = False,
        use_header: bool = True,
        cookie_name: str = "access-token",
        not_authenticated_exception: Union[type, Exception] = InvalidCredentialsException,
        default_expiry: timedelta = timedelta(minutes=15),
        scopes: Optional[Dict[str, str]] = None,
        out_of_scope_exception: Union[type, Exception] = InsufficientScopeException,
    ):
        if use_cookie is False and use_header is False:
            raise AttributeError(
                "use_cookie and use_header are both False one of them needs to be True"
            )
        if isinstance(secret, str):
            secret = secret.encode()

        self.secret = secret
        self.algorithm = algorithm
        self.oauth_scheme = None
        self.use_cookie = use_cookie
        self.use_header = use_header
        self.cookie_name = cookie_name
        self.default_expiry = default_expiry

        self._user_callback: Optional[Callable] = None
        self._user_callback_is_async: bool = False
        self._not_authenticated_exception = not_authenticated_exception
        self._out_of_scope_exception = out_of_scope_exception

        ### The manager raises its own exception instead of `OAuth2PasswordBearer`'s.
        super().__init__(tokenUrl=token_url, auto_error=False, scopes=scopes)

    @property
    def out_of_scope_exception(self):
        """The exception raised when the token lacks a required scope."""
        return self._out_of_scope_exception

    @property
    def not_authenticated_exception(self):
        """The exception raised when no valid token is present."""
        return self._not_authenticated_exception

    def user_loader(self, *args, **kwargs) -> Callable:
        """
        Decorator: set the callback that turns a token's `sub` into the user (or `None`).
        Positional and keyword arguments are passed on to the callback after the identifier.
        """
        def decorator(callback: Callable):
            def _bound(identifier):
                return callback(identifier, *args, **kwargs)
            self._user_callback = _bound
            self._user_callback_is_async = inspect.iscoroutinefunction(callback)
            return callback

        return decorator

    def _get_payload(self, token: str) -> Dict[str, Any]:
        try:
            return jwt.decode(token, self.secret, algorithms=[self.algorithm])
        except jwt.PyJWTError:
            raise self.not_authenticated_exception

    def _has_scopes(
        self,
        payload: Dict[str, Any],
        required_scopes: Optional[SecurityScopes],
    ) -> bool:
        if required_scopes is None or not required_scopes.scopes:
            return True
        provided_scopes = payload.get("scopes", [])
        if len(provided_scopes) < len(required_scopes.scopes):
            return False
        return not any(scope not in provided_scopes for scope in required_scopes.scopes)

    def has_scopes(self, token: str, required_scopes: SecurityScopes) -> bool:
        """Return whether a token carries every required scope."""
        return self._has_scopes(self._get_payload(token), required_scopes)

    async def _load_user(self, identifier: Any):
        if self._user_callback is None:
            raise Exception("Missing user_loader callback")
        if self._user_callback_is_async:
            return await self._user_callback(identifier)
        return await run_sync(self._user_callback, identifier)

    async def _get_current_user(self, payload: Dict[str, Any]):
        user_identifier = payload.get("sub")
        if user_identifier is None:
            raise self.not_authenticated_exception
        user = await self._load_user(user_identifier)
        if user is None:
            raise self.not_authenticated_exception
        return user

    async def get_current_user(self, token: str) -> Any:
        """Return the user a token belongs to."""
        return await self._get_current_user(self._get_payload(token))

    def create_access_token(
        self,
        *,
        data: dict,
        expires: Optional[timedelta] = None,
        scopes: Optional[Collection[str]] = None,
    ) -> str:
        """Encode `data` (plus `exp` and `scopes`) with the manager's secret."""
        to_encode = data.copy()
        to_encode.update({"exp": datetime.now(timezone.utc) + (expires or self.default_expiry)})
        if scopes is not None:
            to_encode.update({"scopes": list(set(scopes))})
        return jwt.encode(to_encode, self.secret, self.algorithm)

    def set_cookie(self, response: Response, token: str) -> None:
        """Set the access-token cookie on a response."""
        response.set_cookie(key=self.cookie_name, value=token, httponly=True)

    def _token_from_cookie(self, request: Request) -> Optional[str]:
        return request.cookies.get(self.cookie_name) or None

    async def _get_token(self, request: Request) -> str:
        token = None
        if self.use_cookie:
            token = self._token_from_cookie(request)
        if not token and self.use_header:
            token = await super().__call__(request)
        if not token:
            raise self.not_authenticated_exception
        return token

    async def __call__(
        self,
        request: Request,
        security_scopes: SecurityScopes = None,  # type: ignore
    ) -> Any:
        """The dependency: return the request's user, or raise the not-authenticated exception."""
        token = await self._get_token(request)
        payload = self._get_payload(token)
        if not self._has_scopes(payload, security_scopes):
            raise self._out_of_scope_exception
        return await self._get_current_user(payload)

    async def optional(self, request: Request, security_scopes: SecurityScopes = None):  # type: ignore
        """A dependency that returns `None` instead of raising."""
        try:
            return await self.__call__(request, security_scopes)
        except Exception:
            return None

    def attach_middleware(self, app: FastAPI) -> None:
        """Add a middleware that stores the user (or `None`) on `request.state.user`."""
        from starlette.middleware.base import BaseHTTPMiddleware

        async def __set_user(request: Request, call_next):
            try:
                request.state.user = await self.__call__(request)
            except Exception:
                request.state.user = None
            return await call_next(request)

        app.add_middleware(BaseHTTPMiddleware, dispatch=__set_user)
