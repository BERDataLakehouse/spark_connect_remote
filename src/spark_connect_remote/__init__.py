"""
KBase authentication for Apache Spark Connect.

This module provides helpers for authenticating Spark Connect sessions
using KBase authentication tokens. It automatically resolves the username
from the token to build the correct Spark Connect URL.

Example usage:

    from spark_connect_remote import create_spark_session

    # Create an authenticated Spark session (defaults to Production)
    # Requires KBASE_AUTH_TOKEN env var, or pass kbase_token="..."
    # Connects to spark.berdl.kbase.us:443
    spark = create_spark_session()

    # Or customize for local dev/testing (e.g. via kubectl port-forward):
    spark = create_spark_session(
        host_template="localhost",
        port=15002,
        use_ssl=False,
        kbase_token="...",
        kbase_auth_url="https://ci.kbase.us/services/auth/",
    )

    # Use the session as normal
    df = spark.sql("SELECT * FROM my_table")

For client-side token validation (e.g., to get user info):

    from spark_connect_remote import KBaseAuthClient

    client = KBaseAuthClient()
    token_info = client.validate_token("your-token")
    print(f"User: {token_info.user}")
    print(f"Expires: {token_info.expires}")
"""

from spark_connect_remote.kbase_client import (
    KBaseAuthClient,
    KBaseAuthError,
    KBaseTokenInfo,
)
from spark_connect_remote.session import (
    DEFAULT_AUTH_URL,
    DEFAULT_HOST_TEMPLATE,
    DEFAULT_PORT,
    create_spark_session,
    get_spark_session,
)

__version__ = "0.2.0"

__all__ = [
    # Auth Client
    "KBaseAuthClient",
    "KBaseAuthError",
    "KBaseTokenInfo",
    # Session Helpers
    "create_spark_session",
    "get_spark_session",
    # Defaults
    "DEFAULT_PORT",
    "DEFAULT_HOST_TEMPLATE",
    "DEFAULT_AUTH_URL",
]
