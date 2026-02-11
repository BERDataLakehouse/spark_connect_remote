# spark-connect-remote

[![Python 3.13+](https://img.shields.io/badge/python-3.13%2B-blue.svg)](https://www.python.org/downloads/)
[![codecov](https://codecov.io/github/BERDataLakehouse/spark_connect_remote/graph/badge.svg)](https://codecov.io/github/BERDataLakehouse/spark_connect_remote)

**The official Python client for connecting to the BERDL Spark Cluster.**

This library provides a secure, zero-configuration connection to your personal Spark session on the BERDL JupyterHub. It handles:
- **KBase Authentication**: Automatically validates your token.
- **Secure Tunneling**: Connects via TLS (HTTPS) to the cluster Ingress.
- **Smart Routing**: Routes your session to your personal Spark driver pod.

## Installation

```bash
uv pip install "git+https://github.com/BERDataLakehouse/spark_connect_remote.git"
```

## Usage

### BERDL JupyterHub Connection (Remote)

To connect to your personal Spark cluster running on BERDL JupyterHub from a local machine or external service:

1. Log in to [BERDL JupyterHub](https://hub.berdl.kbase.us/).
2. **Ensure you have opened a notebook.** Open any notebook in JupyterLab. This automatically starts your personal Spark Connect service.

> [!WARNING]
> **Session Timeout**: Your BERDL JupyterHub session will automatically terminate after a certain period of inactivity. If you cannot connect to your personal Spark Connect server, please log in again to restart your notebook server.

3. **Get your Token**: In your BERDL notebook, run this cell to print your token:

   ```python
   print(get_settings().KBASE_AUTH_TOKEN)
   ```

4. **Connect**: Copy the token and use it in your local script:

   ```python
   from spark_connect_remote import create_spark_session

   # Connect via the public proxy (routes to your personal spark cluster)
   spark = create_spark_session(
       kbase_token="PASTE_YOUR_TOKEN_HERE",
   )

   # Verify connection
   spark.sql("SHOW DATABASES").show()
   ```

### Manual Configuration

For local development (e.g., via `kubectl port-forward`):

```python
spark = create_spark_session(
    host_template="localhost",
    port=15002,
    use_ssl=False,
    kbase_token="...",
    kbase_auth_url="https://ci.kbase.us/services/auth/", # Optional: Override auth service
)
```

## Architecture

The client connects to the **Spark Connect Proxy** via the cluster Ingress (`spark.berdl.kbase.us:443`). The proxy authenticates the request using the KBase Auth2 service and routes it to the user's specific Spark driver pod.

## development

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
