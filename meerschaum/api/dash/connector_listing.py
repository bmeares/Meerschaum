#! /usr/bin/env python3
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

"""
The connectors the console's Connectors page (`/dash/connectors`) lists, with every secret masked,
and its admin-only connection test.

A connector comes from the configuration (`mrsm edit config`; `Default` when it is one of the
built-in connectors left as shipped), an environment variable (`MRSM_<TYPE>_<LABEL>`, or a
`MRSM_CONFIG` / `MRSM_PATCH` patch), or a plugin (a data plugin's `plugin:<name>`, or a connector
a plugin created while running).

Nothing returned here holds a password, token, key or secret. The page shows the values of a
connector's connection settings only (`PUBLIC_ATTRIBUTES`: flavor, host, port, database, username,
driver, timeouts, ...) and masks every other attribute's value, whatever its name. A URI or a
connection string (`URI_NAMES`) keeps its scheme, user, host, port, database and public settings,
with its password and everything else masked. The values of secret-looking names and the
passwords of URIs are then scrubbed from every field shown, and every masked value from the
connection test's errors.
"""

from __future__ import annotations

import os
import re
import json
import copy
import time
from urllib.parse import urlparse, unquote

import meerschaum as mrsm
from meerschaum.utils.typing import Any, Dict, List, Optional, Tuple

#: What a masked value shows.
MASK: str = '••••••'


def _norm(name: Any) -> str:
    """A name as the lists below compare it: lowercase letters and digits only."""
    return re.sub(r'[^a-z0-9]', '', str(name).lower())


#: The attributes whose values the page shows: where and how a connector connects. Every other
#: attribute's value is masked whatever its name, so a secret under a name nobody listed (`pass`,
#: `pw`, `ssh_passphrase`, `cookie`) never reaches the browser. Names are compared by `_norm`
#: (`TrustServerCertificate` is `trust_server_certificate`), also for the settings inside URIs and
#: connection strings (`Server=h;Uid=u;Pwd=p`).
PUBLIC_ATTRIBUTES: frozenset = frozenset(_norm(name) for name in (
    ### Where.
    'flavor', 'protocol', 'host', 'hostname', 'hostaddr', 'server', 'data_source', 'port',
    'database', 'dbname', 'db', 'initial_catalog', 'schema', 'service_name', 'sid', 'region',
    ### Who (a user's name; never its password).
    'username', 'user', 'uid',
    ### How.
    'driver', 'dialect', 'sslmode', 'ssl', 'tls', 'encrypt', 'verify', 'trust_server_certificate',
    'use_fmtonly', 'mars_connection', 'application_name', 'charset', 'encoding', 'timezone',
    'isolation_level', 'autocommit', 'echo', 'connect_timeout', 'timeout', 'socket_timeout',
    'command_timeout', 'pool_size', 'max_overflow', 'pool_recycle', 'pool_pre_ping', 'pool_timeout',
    'fast_executemany', 'use_insertmanyvalues', 'use_setinputsizes',
    'ignore_no_transaction_on_rollback', 'protected',
    ### A connector's own keys (a running connector's `label` and `type`).
    'label', 'type',
))

#: Attributes that hold a URI or a connection string: shown by `mask_connection_string`.
URI_NAMES: frozenset = frozenset(_norm(name) for name in (
    'uri', 'dsn', 'connection_string', 'conn_str', 'odbc_connect', 'options',
))

#: The connector types whose URI path is their database (`sqlite:////path/to.db`,
#: `redis://host:6379/0`). Any other type's path is masked: a webhook URL's path is its secret.
DATABASE_PATH_TYPES = frozenset({'sql', 'valkey'})

#: Words that make a name look like a secret's (`password`, `api_key`, `ssh_passphrase`): its value
#: is masked in any text the page shows (`Pwd=...` in an error message), it is scrubbed from the
#: other fields, and a group of settings under such a name is masked whole.
SECRET_WORDS: Tuple[str, ...] = (
    'password', 'passwd', 'pass', 'pwd', 'pw', 'secret', 'token', 'key', 'credential', 'private',
    'auth', 'phrase', 'cookie', 'session', 'sig', 'salt', 'sas', 'jwt', 'bearer', 'otp',
)

#: Schemes whose bare user information (`https://<token>@host`) is usually a token.
TOKEN_USERINFO_SCHEMES = frozenset({'http', 'https', 'ws', 'wss', 'git+https'})

#: Values too common to scrub from the text around them (`verify: 'true'` would mask every "true").
COMMON_VALUES = frozenset({'true', 'false', 'none', 'null', 'yes', 'off'})

### `scheme://user:password@` (the password runs to the last `@` of the address, so one holding an
### unencoded `@`, `/` or `#` is masked whole) and `https://<token>@`.
_URI_CREDENTIALS = re.compile(
    r'(?P<scheme>[A-Za-z][A-Za-z0-9+.\-]*)://(?P<user>[^:/@\s?#]*)(?::(?P<password>\S*))?@'
)
_SECRET_NAME = r'[\w.\-]*(?:' + '|'.join(SECRET_WORDS) + r')[\w.\-]*'
### A secret-looking setting in any text, at any position: `password=...`, `;Pwd=...`, `?sig=...`,
### `"api_key": "..."`, `token: ...`.
_TEXT_SECRETS = re.compile(
    r'(?P<name>(?<![\w.\-])["\']?' + _SECRET_NAME + r'["\']?\s*(?:=|:\s*))'
    r'(?P<value>\{[^}]*\}?|"[^"]*"?|\'[^\']*\'?|[^\s&;,)\]}]+)',
    flags=re.IGNORECASE,
)
_HTTP_CREDENTIALS = re.compile(r'\b(?P<scheme>Bearer|Basic)\s+[A-Za-z0-9._~+/=\-]{6,}', flags=re.IGNORECASE)
_URI_START = re.compile(r'\s*(?P<scheme>[A-Za-z][A-Za-z0-9+.\-]*)://(?P<rest>.*)\Z', flags=re.DOTALL)
_URI_ADDRESS = re.compile(
    r'(?P<hostport>[^/?#]*)(?P<path>/[^?#]*)?(?:\?(?P<query>[^#]*))?(?:#(?P<fragment>.*))?\Z',
    flags=re.DOTALL,
)

#: The order connector types are listed in.
TYPE_ORDER: Tuple[str, ...] = ('sql', 'api', 'valkey')

#: Meerschaum's own environment variables that look like `MRSM_<TYPE>_<LABEL>` connectors: the API
#: host's settings (`MRSM_API_HOST`, `MRSM_API_HOST_CONFIG`, `MRSM_API_HOST_PROXY_TOKEN`, ...).
INTERNAL_ENV_PREFIXES: Tuple[str, ...] = ('MRSM_API_HOST',)


def is_internal_connector(typ: str, label: str) -> bool:
    """
    Whether `typ:label` is not a connector but one of Meerschaum's own environment variables read
    as one (`api:host` from `MRSM_API_HOST=1` in the API host's process).
    """
    from meerschaum._internal.static import STATIC_CONFIG
    variable = f"MRSM_{typ}_{label}".upper()
    if variable not in os.environ:
        return False
    if variable in STATIC_CONFIG['environment'].values():
        return True
    return any(variable.startswith(prefix) for prefix in INTERNAL_ENV_PREFIXES)


def is_secret_name(name: Any) -> bool:
    """
    Whether a name looks like a secret's (`password`, `api_key`, `client_secret`, `pass`, `pw`,
    `ssh_passphrase`, `cookie`, ...). The page masks more than these: see `is_public_name`.
    """
    lowered = str(name).lower()
    return any(word in lowered for word in SECRET_WORDS)


def is_public_name(name: Any) -> bool:
    """Whether the page shows the value of an attribute (or setting) of this name."""
    return _norm(name) in PUBLIC_ATTRIBUTES


def mask_text(text: Any) -> str:
    """
    Any text with what looks like a secret masked: URI credentials (`scheme://user:••••••@host`,
    a bare `https://••••••@host`), secret-looking settings anywhere (`password=••••••`,
    `Pwd=••••••;`, `?sig=••••••`, `"api_key": ••••••`) and `Bearer` / `Basic` credentials.
    """
    def _credentials(match: 're.Match') -> str:
        scheme, user, password = match.group('scheme'), match.group('user'), match.group('password')
        if password is not None:
            return f"{scheme}://{user}:{MASK}@"
        if user and scheme.lower() in TOKEN_USERINFO_SCHEMES:
            return f"{scheme}://{MASK}@"
        return match.group(0)

    masked = _HTTP_CREDENTIALS.sub(lambda match: f"{match.group('scheme')} {MASK}", str(text))
    masked = _URI_CREDENTIALS.sub(_credentials, masked)
    return _TEXT_SECRETS.sub(lambda match: match.group('name') + MASK, masked)


#: `mask_text` by its first name.
mask_uri = mask_text


def _split_settings(text: str, separator: str) -> List[str]:
    """
    `text` split on `separator`: `'&'` (a URL query), `';'` outside braces (ODBC,
    `Driver={a;b};...`) or `' '`, any whitespace outside quotes (libpq, `password='a b'`).
    """
    if separator == '&':
        return text.split('&')
    parts: List[str] = []
    current: List[str] = []
    quote, depth = None, 0
    for char in text:
        if quote is not None:
            current.append(char)
            if char == quote:
                quote = None
            continue
        if separator == ' ' and char in ('"', "'"):
            quote = char
        elif separator == ';' and char == '{':
            depth += 1
        elif separator == ';' and char == '}' and depth:
            depth -= 1
        elif depth == 0 and (char.isspace() if separator == ' ' else char == separator):
            parts.append(''.join(current))
            current = []
            continue
        current.append(char)
    parts.append(''.join(current))
    return parts


def _mask_settings(text: str, separator: Optional[str] = None) -> Tuple[str, List[str]]:
    """
    `key=value` settings (`a=1&b=2`, `Driver={x};Pwd=p;`, `host=h password='p q'`) with the value
    of every key that is not public masked, and any text that is not a setting masked too:
    `(masked text, hidden values)`.
    """
    if separator is None:
        separator = '&' if '&' in text else (';' if ';' in text else ' ')
    masked, hidden = [], []
    for part in _split_settings(text, separator):
        if not part.strip():
            masked.append(part)
            continue
        key, equals, value = part.partition('=')
        if equals and is_public_name(key):
            masked.append(part)
        elif equals:
            masked.append(f"{key}={MASK}" if value.strip() else part)
            if value.strip():
                hidden.append(value.strip())
        else:
            masked.append(MASK)
            hidden.append(part.strip())
    return (' ' if separator == ' ' else separator).join(masked), hidden


def _mask_connection(text: str, typ: Optional[str] = None) -> Tuple[str, List[str]]:
    """`mask_connection_string()` and the values it hid."""
    if not text.strip():
        return text, []
    match = _URI_START.match(text)
    if match is None:
        if '=' in text:
            return _mask_settings(text)
        return MASK, [text.strip()]

    scheme, rest = match.group('scheme'), match.group('rest')
    hidden: List[str] = []
    userinfo = ''
    ### The address starts after the last `@`: a password with an unencoded `@` is hidden whole.
    at = rest.rfind('@')
    if at >= 0:
        credentials, rest = rest[:at], rest[at + 1:]
        user, colon, password = credentials.partition(':')
        if any(char in user for char in '/?#'):
            userinfo, hidden = MASK, hidden + [credentials]
        elif colon:
            userinfo, hidden = f"{user}:{MASK}", hidden + [password]
        elif user and scheme.lower() in TOKEN_USERINFO_SCHEMES:
            userinfo, hidden = MASK, hidden + [user]
        else:
            userinfo = user
        userinfo += '@'
    address = _URI_ADDRESS.match(rest)
    if address is None:
        return f"{scheme}://{userinfo}{MASK}", hidden + [rest]
    hostport = address.group('hostport') or ''
    host, colon, port = hostport.rpartition(':')
    if colon and port and not port.isdigit() and not hostport.endswith(']'):
        hostport = f"{host}:{MASK}"
        hidden.append(port)
    path = address.group('path') or ''
    if path.strip('/') and typ not in DATABASE_PATH_TYPES:
        hidden.append(path.strip('/'))
        path = '/' + MASK
    shown = f"{scheme}://{userinfo}{hostport}{path}"
    query = address.group('query')
    if query is not None:
        masked_query, query_hidden = _mask_settings(query, '&')
        shown += '?' + masked_query
        hidden.extend(query_hidden)
    if address.group('fragment'):
        shown += '#' + MASK
        hidden.append(address.group('fragment'))
    return shown, hidden


def mask_connection_string(text: Any, typ: Optional[str] = None) -> str:
    """
    A URI or connection string as the page shows it. A URI keeps its scheme, its user, its host
    and port, its path when that is the database (`sql:` and `valkey:` connectors) and its public
    settings (`?sslmode=require`); a `key=value` string (`host=h user=u password=p`,
    `Driver={...};Server=h;Pwd=p`) keeps its public settings. Everything else is masked: the
    password, a token, any other setting, a fragment, and text that is neither.
    """
    return _mask_connection(str(text), typ)[0]


def _uri_secrets(text: str) -> List[str]:
    """The passwords and tokens of the URIs in `text`, and its secret-looking settings' values."""
    secrets = []
    for match in _URI_CREDENTIALS.finditer(text):
        password = match.group('password')
        if password:
            secrets.append(password)
        elif match.group('user') and match.group('scheme').lower() in TOKEN_USERINFO_SCHEMES:
            secrets.append(match.group('user'))
    for match in _TEXT_SECRETS.finditer(text):
        value = match.group('value').strip('\'"{}')
        if value:
            secrets.append(value)
    return secrets


def collect_secrets(attributes: Any, name: str = '') -> List[str]:
    """
    The secrets in a connector's attributes (nested values included): the values of
    secret-looking names (`is_secret_name`), the passwords and tokens of URIs and the values of
    secret-looking settings in strings. `scrub()` masks them in any other text.
    """
    secrets: List[str] = []
    if isinstance(attributes, dict):
        for key, value in attributes.items():
            if is_secret_name(key) and isinstance(value, (dict, list, tuple, set)):
                ### A group of settings under a secret's name is a secret whole.
                secrets.extend(_leaves(value))
                continue
            secrets.extend(collect_secrets(value, str(key)))
        return secrets
    if isinstance(attributes, (list, tuple, set)):
        for value in attributes:
            secrets.extend(collect_secrets(value, name))
        return secrets
    if attributes is None or isinstance(attributes, bool):
        return secrets
    text = str(attributes)
    if is_secret_name(name) and text:
        secrets.append(text)
    secrets.extend(_uri_secrets(text))
    return secrets


def hidden_values(attributes: Any, name: str = '', typ: Optional[str] = None) -> List[str]:
    """
    Every value the page masks in a connector's attributes (`mask_attributes`), its secrets
    (`collect_secrets`) included: what a connection test's error must not show either.
    """
    hidden: List[str] = []
    _mask_tree(name, attributes, typ, None, hidden)
    return hidden + collect_secrets(attributes, name)


def scrub(text: Any, secrets: List[str]) -> str:
    """`text` with what looks like a secret (`mask_text`) and every known secret value masked."""
    text = mask_text(str(text))
    candidates = set()
    for secret in secrets or []:
        secret = str(secret).strip()
        ### Also the decoded form of a percent-encoded password and the parts of one holding an
        ### `@` (a URI parser may split it there): an error may quote any of them.
        forms = {secret, unquote(secret)}
        for candidate in forms | {part for form in forms for part in form.split('@')}:
            candidate = candidate.strip()
            if len(candidate) >= 3 and candidate.lower() not in COMMON_VALUES and candidate != MASK:
                candidates.add(candidate)
    ### Longest first, so a secret containing another is masked whole. Very short values (a
    ### one-character password) would mask ordinary letters and are covered by the name rules.
    for secret in sorted(candidates, key=len, reverse=True):
        text = text.replace(secret, MASK)
    return text


def _mask_tree(
    name: str,
    value: Any,
    typ: Optional[str],
    secrets: Optional[List[str]],
    hidden: List[str],
) -> Any:
    """
    `value` (of attribute `name`) as the page shows it; each value it masks is added to `hidden`.
    Shown text is scrubbed of `secrets` (unless `None`).
    """
    if isinstance(value, dict):
        if not value:
            return {}
        if is_secret_name(name) and not is_public_name(name):
            hidden.extend(_leaves(value))
            return MASK
        return {
            str(key): _mask_tree(str(key), val, typ, secrets, hidden)
            for key, val in value.items()
            if not str(key).startswith('_')
        }
    if value is None or isinstance(value, bool) or (isinstance(value, str) and not value):
        return value
    if _norm(name) in URI_NAMES and isinstance(value, str):
        shown, values = _mask_connection(value, typ)
        hidden.extend(values)
        return scrub(shown, secrets) if secrets is not None else shown
    if not is_public_name(name):
        hidden.extend(_leaves(value))
        return MASK
    if isinstance(value, (list, tuple, set)):
        return [_mask_tree(name, item, typ, secrets, hidden) for item in value]
    if isinstance(value, (int, float)):
        return value
    if isinstance(value, str):
        return scrub(value, secrets) if secrets is not None else value
    ### Objects (an engine, a client): their type only, never their representation.
    return f"<{type(value).__name__}>"


def _leaves(value: Any) -> List[str]:
    """
    The texts of the plain values inside `value` (a dict's values, a list's items). Objects (an
    engine, a client) are left out: the page never shows them, only their type's name.
    """
    if isinstance(value, dict):
        return [text for item in value.values() for text in _leaves(item)]
    if isinstance(value, (list, tuple, set)):
        return [text for item in value for text in _leaves(item)]
    if isinstance(value, bool) or not isinstance(value, (str, int, float)):
        return []
    text = str(value).strip()
    return [text] if text else []


def mask_value(
    name: str,
    value: Any,
    secrets: Optional[List[str]] = None,
    typ: Optional[str] = None,
) -> Any:
    """
    A JSON-safe copy of attribute `name`'s value as the page shows it: the value of a public
    name (`PUBLIC_ATTRIBUTES`) scrubbed of `secrets`, a URI or connection string masked by
    `mask_connection_string`, a group of settings with each setting judged by its own name, and
    anything else masked.
    """
    return _mask_tree(str(name), value, typ, list(secrets or []), [])


def mask_attributes(
    attributes: Dict[str, Any],
    typ: Optional[str] = None,
    secrets: Optional[List[str]] = None,
) -> Dict[str, Any]:
    """A connector's attributes with every value the page does not show masked."""
    secrets = collect_secrets(attributes) if secrets is None else secrets
    return {
        str(key): mask_value(str(key), value, secrets, typ)
        for key, value in attributes.items()
        if not str(key).startswith('_')
    }


def _parse_connection(typ: str, attributes: Dict[str, Any]) -> Dict[str, Any]:
    """Connection settings from attributes or their URI, without opening a connection."""
    parsed: Dict[str, Any] = {}
    uri = attributes.get('uri', None)
    if isinstance(uri, str) and uri:
        if typ == 'sql':
            try:
                from meerschaum.connectors.sql import SQLConnector
                parsed = dict(SQLConnector.parse_uri(uri) or {})
            except Exception:
                parsed = {}
        if not parsed:
            try:
                url = urlparse(uri)
                parsed = {
                    'flavor': url.scheme or None,
                    'host': url.hostname,
                    'port': url.port,
                    ### Only a database's URI path is its database (a webhook's is its secret).
                    'database': (
                        ((url.path or '').lstrip('/') or None)
                        if typ in DATABASE_PATH_TYPES else None
                    ),
                }
            except ValueError:
                parsed = {}
    connection = {}
    for field in ('flavor', 'host', 'port', 'database', 'schema'):
        value = attributes.get(field, None)
        if (value is None or value == '') and parsed:
            value = parsed.get(field, None)
        connection[field] = value
    ### A host never holds user information: an unencoded `@` in a URI's password leaves part of
    ### the password before the host for a parser that splits at the first `@`.
    if isinstance(connection['host'], str) and '@' in connection['host']:
        connection['host'] = connection['host'].rsplit('@', 1)[-1] or None
    if typ == 'api' and not connection.get('flavor'):
        connection['flavor'] = attributes.get('protocol', None) or parsed.get('flavor', None)
    if typ == 'sql':
        from meerschaum._internal.static import STATIC_CONFIG
        defaults = STATIC_CONFIG['sql']['create_engine_flavors'].get(connection.get('flavor'), {}).get('defaults', {})
        for field in ('host', 'port', 'database', 'schema'):
            if connection.get(field) in (None, ''):
                connection[field] = defaults.get(field)
    database = connection.get('database', None)
    if database == '{SQLITE_DB_PATH}':
        try:
            from meerschaum.config import paths
            connection['database'] = paths.SQLITE_DB_PATH.as_posix()
        except Exception:
            pass
    return connection


def connector_fields(typ: str, attributes: Dict[str, Any], flavor: Any = None) -> Dict[str, Any]:
    """Declared required and common settings. Reading metadata never constructs a connector."""
    required, supported = set(), {'uri', 'teams'}
    if typ == 'sql':
        from meerschaum._internal.static import STATIC_CONFIG
        config = STATIC_CONFIG['sql']['create_engine_flavors'].get(str(flavor or ''), {})
        required.update(config.get('requirements') or [])
        required.add('flavor')
        supported.update(required)
        supported.update(config.get('defaults') or {})
        supported.update(('host', 'port', 'database', 'schema', 'username', 'password',
                          'flavor', 'connect_timeout', 'pool_size', 'max_overflow',
                          'pool_timeout', 'pool_recycle', 'sslmode', 'options', 'protected'))
    elif typ in ('api', 'valkey'):
        required.add('host')
        supported.update(('host', 'port', 'username', 'password'))
        if typ == 'api':
            supported.update(('protocol', 'verify', 'api_key', 'client_id', 'client_secret'))
        else:
            supported.update(('db', 'socket_timeout'))
    try:
        from meerschaum.connectors import types
        cls = types.get(typ)
        for base in getattr(cls, '__mro__', ()):
            for key, target in (('REQUIRED_ATTRIBUTES', required), ('OPTIONAL_ATTRIBUTES', supported)):
                names = vars(base).get(key)
                if isinstance(names, (list, tuple, set, frozenset)):
                    target.update(name for name in names if isinstance(name, str))
    except Exception:
        pass
    supported.update(required)
    return {'required': sorted(required), 'supported': sorted(supported),
            'uri_alternative': typ in ('sql', 'api', 'valkey')}


def connector_visible(attributes: Dict[str, Any], viewer: Any = None) -> bool:
    """Apply the connector's team allowlist from plain settings, without instantiating it."""
    if viewer is None or viewer.no_auth or viewer.is_admin:
        return True
    if not viewer.signed_in or not viewer.can('connectors:read'):
        return False
    return _teams_allow(attributes, viewer)


def _teams_allow(attributes: Dict[str, Any], viewer: Any) -> bool:
    """The API's pure connector-team rule, using only plain settings."""
    if viewer.no_auth or viewer.is_admin:
        return True
    teams = attributes.get('teams')
    if teams is not None and not isinstance(teams, (str, list, tuple, set, frozenset)):
        return False
    from meerschaum.api.dash import access
    principal = viewer.principal or access._principal(viewer)
    return access.core().can_use_connector(principal, teams)


def connector_team_allows(keys: str, viewer: Any) -> bool:
    """Check a connector's config/runtime team restriction without constructing it."""
    if viewer.no_auth or viewer.is_admin:
        return True
    from meerschaum.connectors.parse import parse_connector_keys
    configured = parse_connector_keys(str(keys), construct=False)
    attributes = dict(configured) if isinstance(configured, dict) else {}
    typ, _, label = str(keys).partition(':')
    from meerschaum.connectors import connectors as runtime_connectors
    conn = (runtime_connectors.get(typ) or {}).get(label)
    runtime = vars(conn) if conn is not None else {}
    if not isinstance(configured, dict) and 'teams' in runtime:
        attributes['teams'] = runtime['teams']
    return _teams_allow(attributes, viewer)


def _env_patches() -> List[Tuple[str, Dict[str, Any]]]:
    """The connector parts of `MRSM_CONFIG` and `MRSM_PATCH`: `[(variable, {type: {label: ...}})]`."""
    from meerschaum._internal.static import STATIC_CONFIG
    from meerschaum.utils.misc import string_to_dict
    patches = []
    for variable in (STATIC_CONFIG['environment']['config'], STATIC_CONFIG['environment']['patch']):
        text = os.environ.get(variable, None)
        if not text:
            continue
        try:
            patch = string_to_dict(str(text).strip())
        except Exception:
            continue
        connectors = (
            (patch.get('meerschaum', None) or {}).get('connectors', None)
            if isinstance(patch, dict) else None
        )
        if isinstance(connectors, dict):
            patches.append((variable, connectors))
    return patches


def connector_source(
    typ: str,
    label: str,
    attributes: Dict[str, Any],
    defaults: Dict[str, Any],
    patches: Optional[List[Tuple[str, Dict[str, Any]]]] = None,
) -> Tuple[str, Optional[str]]:
    """
    Where a configured connector comes from: `('Environment', 'MRSM_SQL_FOO')`, `('Default', None)`
    (a built-in connector left as shipped) or `('Config', None)`.
    """
    variable = f"MRSM_{typ}_{label}".upper()
    if variable in os.environ:
        return 'Environment', variable
    for patch_variable, connectors in (patches if patches is not None else _env_patches()):
        if isinstance(connectors.get(typ, None), dict) and label in connectors[typ]:
            return 'Environment', patch_variable
    default = (defaults.get(typ, None) or {}).get(label, None)
    if isinstance(default, dict) and default == attributes:
        return 'Default', None
    return 'Config', None


def _type_plugin(typ: str) -> Optional[str]:
    """The plugin that registered a connector type (`@make_connector`), if any."""
    try:
        from meerschaum.connectors import plugins_types, _native_type_plugins
        for plugin_name, types_ in (plugins_types or {}).items():
            if typ in (types_ or []):
                return str(plugin_name)
        native = (_native_type_plugins or {}).get(typ, None)
        return str(native) if native else None
    except Exception:
        return None


def _testable(typ: str) -> bool:
    """Whether a connector type can be tested (it answers `test_connection()`)."""
    if typ in TYPE_ORDER:
        return True
    if typ == 'plugin':
        return False
    try:
        from meerschaum.connectors import types
        cls = types.get(typ, None)
    except Exception:
        cls = None
    return bool(cls is not None and callable(getattr(cls, 'test_connection', None)))


def _row(
    typ: str,
    label: str,
    attributes: Dict[str, Any],
    source: str,
    source_detail: Optional[str] = None,
) -> Dict[str, Any]:
    secrets = collect_secrets(attributes)
    connection = _parse_connection(typ, attributes)
    return {
        'keys': f"{typ}:{label}",
        'type': typ,
        'label': label,
        'plugin': _type_plugin(typ),
        'source': source,
        'source_detail': source_detail,
        **{
            field: mask_value(field, connection.get(field, None), secrets, typ)
            for field in ('flavor', 'host', 'port', 'database', 'schema')
        },
        'fields': connector_fields(typ, attributes, connection.get('flavor')),
        'attributes': mask_attributes(attributes, typ, secrets),
        'testable': _testable(typ),
    }


def _sort_key(row: Dict[str, Any]) -> Tuple[int, str, str]:
    typ = row['type']
    rank = TYPE_ORDER.index(typ) if typ in TYPE_ORDER else (len(TYPE_ORDER) + (1 if typ == 'plugin' else 0))
    return rank, typ, row['label']


def list_connectors(viewer: Any = None) -> List[Dict[str, Any]]:
    """
    Every connector the server knows, each `{keys, type, label, plugin, source, source_detail,
    flavor, host, port, database, attributes, testable}`, secrets masked.
    """
    if viewer is not None and not (viewer.no_auth or viewer.is_admin) and (
        not viewer.signed_in or not viewer.can('connectors:read')
    ):
        return []
    from meerschaum.config import get_config
    from meerschaum.config._default import default_meerschaum_config
    configured = copy.deepcopy(get_config('meerschaum', 'connectors', warn=False) or {})
    defaults = default_meerschaum_config.get('connectors', {})
    patches = _env_patches()
    rows: List[Dict[str, Any]] = []
    seen = set()
    for typ, labels in configured.items():
        if not isinstance(labels, dict):
            continue
        inherited = labels.get('default', None) if isinstance(labels.get('default', None), dict) else {}
        for label, attributes in labels.items():
            if label == 'default' or not isinstance(attributes, dict):
                continue
            if is_internal_connector(typ, str(label)):
                continue
            # A hidden configured key must not reappear through a cached runtime connector.
            seen.add(f"{typ}:{label}")
            combined = {**inherited, **attributes}
            if not connector_visible(combined, viewer):
                continue
            source, detail = connector_source(typ, label, attributes, defaults, patches)
            rows.append(_row(typ, str(label), combined, source, detail))

    ### Data plugins are connectors (`plugin:<name>`).
    try:
        from meerschaum.utils.misc import get_connector_labels
        for keys in get_connector_labels('plugin'):
            if keys in seen:
                continue
            typ, label = keys.split(':', 1)
            rows.append(_row(typ, label, {}, 'Plugin', label))
            seen.add(keys)
    except Exception:
        pass

    ### Connectors a plugin (or the server) created while running.
    try:
        from meerschaum.connectors import connectors as runtime_connectors
        for typ, labels in list(runtime_connectors.items()):
            for label, conn in list((labels or {}).items()):
                keys = f"{typ}:{label}"
                if keys in seen or conn is None or is_internal_connector(typ, str(label)):
                    continue
                ### Executors (`systemd:main`) run jobs; they are not data connectors.
                if type(conn).__name__.lower().endswith('executor'):
                    continue
                attributes = {
                    key: value
                    for key, value in getattr(conn, '__dict__', {}).items()
                    if not str(key).startswith('_') and key not in ('type', 'label')
                }
                if not connector_visible(attributes, viewer):
                    continue
                plugin = _type_plugin(typ)
                rows.append(_row(typ, str(label), attributes, 'Plugin' if plugin else 'Runtime', plugin))
                seen.add(keys)
    except Exception:
        pass

    return sorted(rows, key=_sort_key)


def connect_timeout(attributes: Optional[Dict[str, Any]] = None) -> float:
    """
    The connection timeout in seconds: `MRSM_CONNECT_TIMEOUT`, else the connector's
    `connect_timeout`, else 5.
    """
    for value in (os.environ.get('MRSM_CONNECT_TIMEOUT', None), (attributes or {}).get('connect_timeout', None)):
        try:
            seconds = float(value)
        except (TypeError, ValueError):
            continue
        if seconds > 0:
            return seconds
    return 5.0


def _probe_api(conn: Any, timeout: float) -> Optional[str]:
    """Why an API connector's server cannot be reached (`None` if `/version` answers)."""
    import urllib.request
    import urllib.error
    url = str(getattr(conn, 'url', '') or '').rstrip('/')
    if not url:
        return "The connector has no URL."
    try:
        with urllib.request.urlopen(url + '/version', timeout=timeout) as response:
            response.read(64)
        return None
    except urllib.error.HTTPError as e:
        return f"The server answered {e.code}."
    except Exception as e:
        return str(getattr(e, 'reason', None) or e)


def _test(keys: str, timeout: float) -> Tuple[bool, str]:
    from meerschaum.connectors import SQLConnector, APIConnector
    conn = mrsm.get_connector(keys)
    if isinstance(conn, SQLConnector):
        if conn._core_connector().test_connection():
            return True, "Connected."
        return False, "The database did not answer."
    if isinstance(conn, APIConnector):
        if conn.test_connection():
            return True, "Connected."
        reason = _probe_api(conn, timeout)
        if reason is None:
            return False, "The server answered but refused this connector (check its credentials)."
        return False, f"Could not reach {getattr(conn, 'url', keys)}: {reason}"
    test_connection = getattr(conn, 'test_connection', None)
    if not callable(test_connection):
        return False, f"Connectors of type '{conn.type}' cannot be tested."
    result = test_connection()
    if isinstance(result, tuple) and result:
        return bool(result[0]), (str(result[1]) if len(result) > 1 else '')
    return (True, "Connected.") if result else (False, "The connection test failed.")


def test_connector(keys: str) -> Dict[str, Any]:
    """
    Try to connect: `{'success', 'message', 'seconds'}`. It gives up after the connector's
    `connect_timeout` (plus a little), and the message never holds a secret.
    """
    import concurrent.futures
    typ, _, label = str(keys).partition(':')
    try:
        from meerschaum.config import get_config
        attributes = dict(
            ((get_config('meerschaum', 'connectors', warn=False) or {}).get(typ, None) or {}).get(label, None)
            or {}
        )
    except Exception:
        attributes = {}
    try:
        from meerschaum.connectors import connectors as runtime_connectors
        conn = (runtime_connectors.get(typ, None) or {}).get(label, None)
        if conn is not None:
            attributes.update({
                key: value for key, value in conn.__dict__.items() if not str(key).startswith('_')
            })
    except Exception:
        pass
    ### An error may echo any attribute: everything the page masks stays masked in it.
    secrets = hidden_values(attributes, typ=typ)
    timeout = connect_timeout(attributes)
    started = time.perf_counter()
    executor = concurrent.futures.ThreadPoolExecutor(max_workers=1, thread_name_prefix='mrsm-test-conn')
    try:
        future = executor.submit(_test, keys, timeout)
        success, message = future.result(timeout=timeout + 2.0)
    except concurrent.futures.TimeoutError:
        success, message = False, f"No answer within {timeout:g} seconds."
    except Exception as e:
        success, message = False, str(e) or type(e).__name__
    finally:
        executor.shutdown(wait=False)
    from meerschaum.utils.misc import remove_ansi
    return {
        'success': bool(success),
        'message': scrub(remove_ansi(str(message)), secrets),
        'seconds': round(time.perf_counter() - started, 3),
    }


def as_json(rows: List[Dict[str, Any]]) -> str:
    """The rows as JSON (what the page sends to the browser)."""
    return json.dumps(rows, default=str, ensure_ascii=False)
