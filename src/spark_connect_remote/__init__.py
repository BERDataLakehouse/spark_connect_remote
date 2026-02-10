"""
KBase authentication for Apache Spark Connect.

This module provides helpers for authenticating Spark Connect sessions
using KBase authentication tokens. It automatically resolves the username
from the token to build the correct Spark Connect URL.

Example usage:

    from spark_connect_remote import create_spark_session

    # Create an authenticated Spark session
    # The {username} placeholder is replaced with the username from the token
    spark = create_spark_session(
        host_template="spark-connect-{username}.jupyterhub-dev.svc.cluster.local",
        kbase_token="your-kbase-token",  # Optional if KBASE_AUTH_TOKEN is set
    )
    # If token belongs to user "alice", connects to:
    # sc://spark-connect-alice.jupyterhub-dev.svc.cluster.local:15002

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
]


# Deprecated exports - keep for backward compatibility
def __getattr__(name: str):
    """Lazy loading for deprecated modules."""
    if name == "KBaseChannelBuilder":
        import warnings

        from spark_connect_remote.channel_builder import KBaseChannelBuilder

        warnings.warn(
            "KBaseChannelBuilder is deprecated. Use create_spark_session() instead.",
            DeprecationWarning,
            stacklevel=2,
        )
        return KBaseChannelBuilder
    if name == "KBaseAuthInterceptor":
        import warnings

        from spark_connect_remote.interceptors import KBaseAuthInterceptor

        warnings.warn(
            "KBaseAuthInterceptor is deprecated. Use create_spark_session() instead.",
            DeprecationWarning,
            stacklevel=2,
        )
        return KBaseAuthInterceptor
    if name == "KBaseAuthMetadata":
        import warnings

        from spark_connect_remote.interceptors import KBaseAuthMetadata

        warnings.warn(
            "KBaseAuthMetadata is deprecated. Use create_spark_session() instead.",
            DeprecationWarning,
            stacklevel=2,
        )
        return KBaseAuthMetadata
    if name == "create_channel_builder":
        import warnings

        from spark_connect_remote.session import create_channel_builder

        warnings.warn(
            "create_channel_builder is deprecated. Use create_spark_session() instead.",
            DeprecationWarning,
            stacklevel=2,
        )
        return create_channel_builder
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
