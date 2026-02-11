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

### BERDL JupyterHub Connection (Remote)

To connect to your personal Spark cluster running on BERDL JupyterHub from a local machine or external service:

1. **Login to BERDL JupyterHub**: Ensure you are logged in at [https://hub.berdl.kbase.us/](https://hub.berdl.kbase.us/).
2. **Start a Notebook**: You **must** have at least one notebook server running. This automatically starts your personal Spark Connect service.
3. **Get your Token**: In your BERDL notebook, run this cell to print your token:

   ```python
   print(get_settings().KBASE_AUTH_TOKEN)
   ```

4. **Connect**: Copy the token and use it in your local script:

   ```python
   from spark_connect_remote import create_spark_session

   # Connect via the public proxy (routes to your personal cluster)
   spark = create_spark_session(
       host_template="spark.berdl.kbase.us",
       port=443,
       use_ssl=True,
       kbase_token="PASTE_YOUR_TOKEN_HERE",
   )

   # Verify connection
   spark.sql("SHOW DATABASES").show()
   ```

## Authentication Flow

1. **Client Connects**: The client connects to `spark.berdl.kbase.us:443` with the KBase token.
2. **Proxy Validation**: The internal proxy (`spark-connect-proxy`) validates the token with KBase Auth2 service.
3. **Routing**: If valid, the proxy resolves the user's username and routes the connection to their personal Spark Connect service (`jupyter-{username}`).
4. **Backend Verification**: The backend Spark Connect service verifies the token matches the pod owner.

## Development

```bash
# Install dev dependencies
uv sync --dev

# Run tests
uv run pytest

# Check linting
uv run ruff check .

# Format code
uv run ruff format .
```
