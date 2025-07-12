# 🪙 Tokens

Meerschaum tokens are long-lived API keys that grant programmatic access to a Meerschaum instance. They are a secure way to authenticate for services like IoT devices or CI/CD pipelines without using your username and password.

Tokens are tied to a specific user and have a defined set of permissions, called **scopes**.

When you create a token, you receive three key pieces of information: a Client ID, a Client Secret, and an API Key.

!!! warning "Write it down!"
    The Client Secret is **only shown once** upon token creation, so be sure to copy it somewhere safe when registering tokens.

- **Client ID:** A public, unique identifier (`UUID`)
- **Client Secret:** A private secret (treat this like a password)
- **API Key:** To be used in the `Authorization` header (contains Client ID and Secret within).

Additionally, tokens have the following attributes:

- **Label:** A human-readable name to help you identify the token
- **Expiration:** An optional timestamp after which the token will become invalid  
  Set the `expiration` to `None` to prevent a token from expiring (use at your own risk!)
- **Scopes:** A list of permissions that define what actions the token can perform
- **User:** The user account that owns the token

---

## ➕ Registering

You can create new tokens via the action `register token`:

```bash
mrsm register token
```

This will prompt you for the owner of the token and print the Client ID, Client Secret and API Key. Registering to an `api` instance will assign the token the connector's configured user.

## 🐍 Python API

In addition to the CLI, uou can manage tokens directly from [instance connectors](/reference/connector/instance-connectors).

### Creating a Token

To create a token, instantiate a `meerschaum.core.Token` object and call its `register()` method.

??? example "Creating a new token"
    ```python
    import meerschaum as mrsm
    from meerschaum.core import Token, User

    # Tokens must be associated with a user.
    user = User('myuser', instance='sql:main')

    # The token will be managed by the 'sql:main' instance connector.
    token = Token(
        label='my-iot-device',
        user=user,
        instance='sql:main',
    )

    # The register() method generates the credentials and saves the token.
    success, msg = token.register()
    if not success:
        print(msg)
    else:
        print(f"Successfully registered token '{token.label}'")
        print(f"  Client ID: {token.id}")
        print(f"  Client Secret: {token.secret}")
        print(f"  API Key: {token.get_api_key()}")
    ```

### Fetching Tokens

You can retrieve a list of all tokens for a user from an instance connector.

```python
conn = mrsm.get_connector('sql:main')
user = mrsm.User('myuser', instance=conn)

# Admins can see all tokens, including those without a user.
# Regular users only see their own tokens.
tokens = conn.get_tokens(user=user)

for token in tokens:
    print(token)
```

### Editing a Token

You can modify a token's label, expiration date, and scopes.

```python
from datetime import datetime, timedelta, timezone

# Get the first token from the list.
token = tokens[0]

# Extend the expiration by 30 days.
if token.expiration:
    token.expiration += timedelta(days=30)
else:
    token.expiration = datetime.now(timezone.utc) + timedelta(days=30)

# Limit the token's scopes.
token.scopes = ['read:pipes', 'write:pipes']

success, msg = token.edit()
```

### Invalidating and Deleting Tokens

If a token is compromised, you can invalidate it immediately. An invalidated token cannot be used but remains in the system. Deleting a token removes it permanently.

```python
# Invalidate the token.
success, msg = token.invalidate()

# Delete the token permanently.
success, msg = token.delete()
```

---

## 🌐 Web UI

You can manage tokens from the Meerschaum dashboard by navigating to `Settings` > `Tokens`.

-   Create new tokens using the `+` button.
-   Edit, invalidate, or delete existing tokens from the context menu (`⠇`).

When you create a new token, a dialog will appear prompting you for a label, expiration date, and scopes. After registration, you **must** copy the generated credentials, as the secret will not be shown again.

---

## ⚙️ Authorization

There are two ways to use a token to authenticate with the API:

1. Post the **Client ID** and **Client Secret** to `/login`.  
  The standard OAuth flow now accepts `client_id` and `client_secret` (in addition to `username` and `password`). This returns short-lived bearer tokens.
2. Set the **API Key** directly in the `Authorization` header.  
  For situations where the standard OAuth flow isn't feasible (e.g. some IoT workloads), you can directly embed the API Key in the `Authorization` header. The API Key wraps the Client ID and Secret and is authenticated on every request.

??? example "Authenticating with `curl`"
    ```bash
    # Replace <API_KEY> with your generated key.
    API_KEY="mrsm-key:YTg1ZTI3NDEtZ..."

    curl -X GET http://localhost:8000/api/pipes \
      -H "Authorization: $API_KEY"
    ```
