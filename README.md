# spark-connect-remote

[![Python 3.13+](https://img.shields.io/badge/python-3.13%2B-blue.svg)](https://www.python.org/downloads/)
[![codecov](https://codecov.io/github/BERDataLakehouse/spark_connect_remote/graph/badge.svg)](https://codecov.io/github/BERDataLakehouse/spark_connect_remote)

KBase authentication for Apache Spark Connect.

This library provides helpers for authenticating Spark Connect sessions using KBase authentication tokens. It automatically resolves the username from the token to build the correct Spark Connect URL for multi-tenant environments.

## Installation

```bash
# Install directly from GitHub
pip install "git+https://github.com/BERDataLakehouse/spark_connect_remote.git"

# For development (from source)
git clone https://github.com/BERDataLakehouse/spark_connect_remote.git
cd spark_connect_remote
pip install -e .

# With development dependencies
pip install -e ".[dev]"
```

### via pyproject.toml

```toml
# Add to pyproject.toml dependencies
spark-connect-remote = { git = "https://github.com/BERDataLakehouse/spark_connect_remote.git", rev = "main" }
```

## Quick Start

### Basic Usage

The library automatically resolves the username from your KBase token to build the correct URL:

```python
from spark_connect_remote import create_spark_session

# Create an authenticated Spark session
# The {username} placeholder is replaced with the username from the token
spark = create_spark_session(
    host_template="spark-connect-{username}.jupyterhub-dev.svc.cluster.local",
    kbase_token="your-kbase-token",
)
# If token belongs to user "alice", connects to:
# sc://spark-connect-alice.jupyterhub-dev.svc.cluster.local:15002

# Use the session
df = spark.sql("SELECT * FROM my_database.my_table")
df.show()
```

### Using Environment Variables

```python
from spark_connect_remote import create_spark_session

# Set environment variables:
# - KBASE_AUTH_TOKEN: Your KBase token
# - KBASE_AUTH_URL: KBase Auth2 service URL (optional)

spark = create_spark_session(
    host_template="spark-connect-{username}.namespace",
)
```

### Full Configuration

```python
from spark_connect_remote import create_spark_session

spark = create_spark_session(
    host_template="spark-connect-{username}.jupyterhub-dev.svc.cluster.local",
    port=15002,
    kbase_token="your-kbase-token",
    kbase_auth_url="https://kbase.us/services/auth/",
    use_ssl=False,
    app_name="MySparkApp",
    spark_config={
        "spark.sql.shuffle.partitions": "200",
    },
)
```

### Getting User Info

Use `KBaseAuthClient` to validate tokens or retrieve user information:

```python
from spark_connect_remote import KBaseAuthClient

client = KBaseAuthClient()

# Get just the username
username = client.get_username("your-token")

# Get full token info
token_info = client.validate_token("your-token")
print(f"User: {token_info.user}")
print(f"Expires: {token_info.expires}")
print(f"Roles: {token_info.custom_roles}")

# Get detailed user info
user_info = client.get_user_info("your-token")
print(f"Display name: {user_info['display']}")
```

## API Reference

### Session Helpers

#### `create_spark_session()`

Create an authenticated SparkSession with KBase authentication.

```python
def create_spark_session(
    host_template: str = "spark-connect-{username}",
    port: int = 15002,
    kbase_token: str | None = None,
    kbase_auth_url: str | None = None,
    use_ssl: bool = False,
    app_name: str | None = None,
    spark_config: dict[str, Any] | None = None,
) -> SparkSession
```

**Parameters:**
- `host_template`: Host template with `{username}` placeholder. The placeholder is replaced with the username from the token.
- `port`: Spark Connect server port (default: 15002)
- `kbase_token`: KBase auth token. Falls back to `KBASE_AUTH_TOKEN` env var.
- `kbase_auth_url`: KBase Auth2 service URL. Falls back to `KBASE_AUTH_URL` env var.
- `use_ssl`: Whether to use SSL/TLS for the connection
- `app_name`: Optional application name for the Spark session
- `spark_config`: Optional dictionary of Spark configuration options

#### `get_spark_session()`

Convenience wrapper around `create_spark_session()`.

```python
def get_spark_session(
    host_template: str = "spark-connect-{username}",
    kbase_token: str | None = None,
    **kwargs,
) -> SparkSession
```

### KBase Auth Client

#### `KBaseAuthClient`

Client for the KBase Auth2 service. Use this to validate tokens or retrieve user information.

```python
class KBaseAuthClient:
    def __init__(
        self,
        auth_url: str = "https://kbase.us/services/auth/",
        timeout: float = 10.0,
    ): ...

    def validate_token(self, token: str) -> KBaseTokenInfo: ...
    def get_username(self, token: str) -> str: ...
    def get_user_info(self, token: str) -> dict[str, Any]: ...
```

#### `KBaseTokenInfo`

Information about a validated KBase token.

```python
@dataclass
class KBaseTokenInfo:
    user: str
    token_id: str
    created: datetime
    expires: datetime
    token_type: str
    custom_roles: list[str]

    def is_expired(self) -> bool: ...
    def has_role(self, role: str) -> bool: ...
```

#### `KBaseAuthError`

Exception raised for KBase authentication errors.

```python
class KBaseAuthError(Exception): ...
```

## Environment Variables

| Variable | Description | Default |
|----------|-------------|---------|
| `KBASE_AUTH_TOKEN` | KBase authentication token | (none) |
| `KBASE_AUTH_URL` | KBase Auth2 service URL | `https://kbase.us/services/auth/` |

## Authentication Flow

1. **Client provides token** and host template (optionally with `{username}` placeholder)
2. **Library validates token** with KBase Auth2 service (fail-fast for invalid tokens)
3. **URL is constructed** by replacing `{username}` with actual username (if present)
4. **Connection is established** with token in URL: `sc://host:port/;token=<token>`
5. **Server validates token** via `KBaseAuthServerInterceptor`
6. **Server verifies** token username matches pod owner

## Server-Side Configuration

For the server to validate KBase tokens, configure the `KBaseAuthServerInterceptor` in `spark-defaults.conf`:

```properties
spark.connect.grpc.interceptor.classes=us.kbase.spark.KBaseAuthServerInterceptor
```

The server-side interceptor requires these environment variables:
- `KBASE_AUTH_URL`: KBase Auth2 service URL (required)
- `USER`: Pod owner username (for multi-tenant validation)

## Security Considerations

- **Client-Side Validation**: Token is validated client-side for fail-fast behavior (invalid/expired tokens fail immediately)
- **Server-Side Validation**: Token is also validated server-side by `KBaseAuthServerInterceptor`
- **User Isolation**: Server verifies token username matches pod owner
- **SSL/TLS**: Use `use_ssl=True` for production deployments
- **Token Expiry**: Both client and server validate token expiry

## Development

```bash
# Install dev dependencies
pip install -e ".[dev]"

# Run tests
pytest

# Run linter
ruff check .

# Format code
ruff format .

# Type checking
mypy src/
```
