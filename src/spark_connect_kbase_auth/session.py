"""
Helper functions for creating authenticated Spark Connect sessions.

This module provides convenient functions for creating SparkSession
instances with KBase authentication. It automatically resolves the
username from the token to build the correct Spark Connect URL.
"""

import logging
import os
from typing import TYPE_CHECKING, Any

from spark_connect_kbase_auth.kbase_client import KBaseAuthClient

if TYPE_CHECKING:
    from pyspark.sql import SparkSession

logger = logging.getLogger(__name__)


# Environment variable names
ENV_KBASE_AUTH_TOKEN = "KBASE_AUTH_TOKEN"
ENV_KBASE_AUTH_URL = "KBASE_AUTH_URL"

# Default Spark Connect port
DEFAULT_PORT = 15002

# Default host template - {username} will be replaced with the actual username
DEFAULT_HOST_TEMPLATE = "spark-connect-{username}"


def create_authenticated_session(
    host_template: str = DEFAULT_HOST_TEMPLATE,
    port: int = DEFAULT_PORT,
    kbase_token: str | None = None,
    kbase_auth_url: str | None = None,
    use_ssl: bool = False,
    app_name: str | None = None,
    spark_config: dict[str, Any] | None = None,
) -> "SparkSession":
    """
    Create an authenticated SparkSession connected to a Spark Connect server.

    This function automatically resolves the username from the KBase token
    and builds the Spark Connect URL. The URL is constructed by replacing
    {username} in the host_template with the actual username.

    Args:
        host_template: Host template with {username} placeholder.
            Example: "spark-connect-{username}.jupyterhub.svc.cluster.local"
            If no {username} placeholder, the host is used as-is.
        port: Spark Connect server port (default: 15002).
        kbase_token: KBase auth token. Falls back to KBASE_AUTH_TOKEN env var.
        kbase_auth_url: KBase Auth2 service URL. Falls back to KBASE_AUTH_URL env var.
        use_ssl: Whether to use SSL/TLS for the connection.
        app_name: Optional application name for the Spark session.
        spark_config: Optional dictionary of Spark configuration options.

    Returns:
        A SparkSession connected to the Spark Connect server with KBase
        authentication configured.

    Raises:
        ImportError: If pyspark is not installed.
        ValueError: If no token is provided and KBASE_AUTH_TOKEN is not set.
        KBaseAuthError: If token validation fails.

    Example:
        # Automatic URL resolution from token
        spark = create_authenticated_session(
            host_template="spark-connect-{username}.jupyterhub-dev.svc.cluster.local",
            kbase_token="your-token",
        )
        # If token belongs to user "alice", connects to:
        # sc://spark-connect-alice.jupyterhub-dev.svc.cluster.local:15002

        # With environment variables (KBASE_AUTH_TOKEN, KBASE_AUTH_URL)
        spark = create_authenticated_session(
            host_template="spark-connect-{username}.namespace",
        )

        # Use the session
        df = spark.sql("SELECT * FROM my_table")
    """
    from pyspark.sql import SparkSession

    # Get token from environment if not provided
    if kbase_token is None:
        kbase_token = os.environ.get(ENV_KBASE_AUTH_TOKEN)
        if not kbase_token:
            raise ValueError(
                f"No token provided and {ENV_KBASE_AUTH_TOKEN} environment variable is not set"
            )
        logger.debug(f"Using KBase token from {ENV_KBASE_AUTH_TOKEN} environment variable")

    # Get auth URL from environment if not provided
    if kbase_auth_url is None:
        kbase_auth_url = os.environ.get(ENV_KBASE_AUTH_URL)

    # Always validate token client-side for fail-fast behavior
    # This gives immediate feedback if the token is invalid or expired
    client = KBaseAuthClient(auth_url=kbase_auth_url) if kbase_auth_url else KBaseAuthClient()
    username = client.get_username(kbase_token)
    logger.debug(f"Validated token for user '{username}'")

    # Resolve host from template if it contains {username}
    if "{username}" in host_template:
        host = host_template.format(username=username)
        logger.info(f"Resolved host to {host} for user '{username}'")
    else:
        host = host_template

    # Build Spark Connect URL with token
    # Format: sc://host:port/;token=<token> sends Authorization: Bearer <token> header
    protocol = "scs" if use_ssl else "sc"
    spark_connect_url = f"{protocol}://{host}:{port}/;token={kbase_token}"

    # Build the session
    builder = SparkSession.builder.remote(spark_connect_url)

    if app_name:
        builder = builder.appName(app_name)

    if spark_config:
        for key, value in spark_config.items():
            builder = builder.config(key, str(value))

    session = builder.getOrCreate()

    logger.info(f"Created authenticated SparkSession connected to {host}:{port}")

    return session


def get_authenticated_spark(
    host_template: str = DEFAULT_HOST_TEMPLATE,
    kbase_token: str | None = None,
    **kwargs: Any,
) -> "SparkSession":
    """
    Get or create an authenticated SparkSession.

    This is a simple wrapper around create_authenticated_session() that
    uses environment variables by default.

    Args:
        host_template: Host template with {username} placeholder.
        kbase_token: Optional KBase token (uses KBASE_AUTH_TOKEN if not provided).
        **kwargs: Additional arguments passed to create_authenticated_session().

    Returns:
        An authenticated SparkSession.

    Example:
        spark = get_authenticated_spark(
            host_template="spark-connect-{username}.namespace"
        )
    """
    return create_authenticated_session(
        host_template=host_template,
        kbase_token=kbase_token,
        **kwargs,
    )


# Keep old function signature for backward compatibility
def create_channel_builder(
    host: str,
    port: int = DEFAULT_PORT,
    kbase_token: str | None = None,
    use_ssl: bool = False,
) -> Any:
    """
    Deprecated: Use create_authenticated_session() instead.

    This function is kept for backward compatibility but now uses
    the simpler URL-based authentication approach internally.
    """
    import warnings

    from spark_connect_kbase_auth.channel_builder import KBaseChannelBuilder

    warnings.warn(
        "create_channel_builder is deprecated. Use create_authenticated_session() instead.",
        DeprecationWarning,
        stacklevel=2,
    )

    if kbase_token is None:
        kbase_token = os.environ.get(ENV_KBASE_AUTH_TOKEN, "")

    return KBaseChannelBuilder(
        host=host,
        port=port,
        kbase_token=kbase_token,
        use_ssl=use_ssl,
    )
