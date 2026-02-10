"""
Custom ChannelBuilder for KBase-authenticated Spark Connect sessions.

This module provides a ChannelBuilder subclass that integrates KBase
authentication into the Spark Connect gRPC channel.

The client validates tokens against the KBase Auth2 service and adds
authentication metadata to all gRPC requests.
"""

import logging
from typing import Any

import grpc
from pyspark.sql.connect.client.core import ChannelBuilder

from spark_connect_remote.interceptors import KBaseAuthInterceptor

logger = logging.getLogger(__name__)


class KBaseChannelBuilder(ChannelBuilder):
    """
    Channel builder for KBase-authenticated Spark Connect connections.

    This class wraps PySpark's DefaultChannelBuilder and adds KBase
    authentication via gRPC interceptors.

    Example:
        builder = KBaseChannelBuilder(
            host="spark-server.example.com",
            port=15002,
            kbase_token="your-kbase-token",
        )

        spark = SparkSession.builder.channelBuilder(builder).getOrCreate()

    Note:
        This class provides a compatible interface with PySpark's
        ChannelBuilder but implements KBase-specific authentication.
    """

    # Default Spark Connect port
    DEFAULT_PORT = 15002

    # gRPC configuration defaults
    GRPC_MAX_MESSAGE_LENGTH_DEFAULT = 128 * 1024 * 1024  # 128 MB

    # Parameter keys (compatible with PySpark's ChannelBuilder)
    PARAM_USE_SSL = "use_ssl"
    PARAM_TOKEN = "token"
    PARAM_USER_ID = "user_id"
    PARAM_USER_AGENT = "user_agent"
    PARAM_SESSION_ID = "session_id"

    def __init__(
        self,
        host: str,
        port: int = DEFAULT_PORT,
        kbase_token: str = "",
        use_ssl: bool = False,
        channel_options: list[tuple[str, Any]] | None = None,
    ):
        """
        Initialize the KBase channel builder.

        Args:
            host: Hostname of the Spark Connect server.
            port: Port of the Spark Connect server (default: 15002).
            kbase_token: The KBase authentication token.
            use_ssl: Whether to use SSL/TLS for the connection.
            channel_options: Optional gRPC channel options.
        """
        # Initialize parent class
        super().__init__(channelOptions=channel_options)

        self._host = host
        self._port = port
        self._kbase_token = kbase_token
        self._use_ssl = use_ssl
        self._channel_options = channel_options or []

        # Add KBase auth interceptors if token provided
        if kbase_token:
            self._add_kbase_interceptors()

    def _add_kbase_interceptors(self) -> None:
        """Add KBase authentication interceptors."""
        # Primary interceptor for unary-stream (most Spark Connect calls)
        unary_stream_interceptor = KBaseAuthInterceptor(token=self._kbase_token)
        self._interceptors.append(unary_stream_interceptor)

        logger.debug(f"Added KBase auth interceptor for connection to {self._host}:{self._port}")

    @property
    def endpoint(self) -> str:
        """Get the endpoint string (host:port)."""
        return f"{self._host}:{self._port}"

    @property
    def host(self) -> str:
        """Get the host."""
        return self._host

    @property
    def port(self) -> int:
        """Get the port."""
        return self._port

    @property
    def secure(self) -> bool:
        """Check if the connection should use SSL."""
        return self._use_ssl

    def get(self, key: str) -> str | None:
        """Get a parameter value."""
        return self._params.get(key)

    def set(self, key: str, value: str) -> None:
        """Set a parameter value."""
        self._params[key] = value

    def getDefault(self, key: str, default: str) -> str:
        """Get a parameter value with default."""
        return self._params.get(key, default)

    def setChannelOption(self, key: str, value: Any) -> "KBaseChannelBuilder":
        """Set a gRPC channel option."""
        self._channel_options.append((key, value))
        return self

    def add_interceptor(
        self, interceptor: grpc.UnaryStreamClientInterceptor
    ) -> "KBaseChannelBuilder":
        """
        Add a custom gRPC interceptor.

        Args:
            interceptor: The gRPC interceptor to add.

        Returns:
            Self for method chaining.
        """
        self._interceptors.append(interceptor)
        return self

    def metadata(self) -> list[tuple[str, str]]:
        """
        Get metadata to include in gRPC calls.

        Returns:
            List of (key, value) tuples for metadata.
        """
        result: list[tuple[str, str]] = []

        # Add custom parameters as metadata (except reserved ones)
        reserved = {
            self.PARAM_USE_SSL,
            self.PARAM_TOKEN,
            self.PARAM_USER_ID,
            self.PARAM_USER_AGENT,
            self.PARAM_SESSION_ID,
        }

        for key, value in self._params.items():
            if key not in reserved:
                result.append((key, value))

        return result

    def toChannel(self) -> grpc.Channel:
        """
        Create and return a gRPC channel with KBase authentication.

        Returns:
            A configured gRPC channel with interceptors.
        """
        target = self.endpoint

        # Build channel options
        options = [
            ("grpc.max_send_message_length", self.GRPC_MAX_MESSAGE_LENGTH_DEFAULT),
            ("grpc.max_receive_message_length", self.GRPC_MAX_MESSAGE_LENGTH_DEFAULT),
        ]
        options.extend(self._channel_options)

        # Create the channel
        if self._use_ssl:
            # SSL channel
            ssl_creds = grpc.ssl_channel_credentials()
            channel = grpc.secure_channel(target, ssl_creds, options=options)
            logger.debug(f"Created secure gRPC channel to {target}")
        else:
            # Insecure channel (for local development)
            channel = grpc.insecure_channel(target, options=options)
            logger.debug(f"Created insecure gRPC channel to {target}")

        # Apply interceptors
        if self._interceptors:
            channel = grpc.intercept_channel(channel, *self._interceptors)
            logger.debug(f"Applied {len(self._interceptors)} interceptor(s) to channel")

        return channel

    @property
    def userId(self) -> str | None:
        """Get the user ID if set."""
        return self._params.get(self.PARAM_USER_ID)

    @property
    def userAgent(self) -> str:
        """Get the user agent string."""
        return self._params.get(self.PARAM_USER_AGENT, "spark-connect-remote")

    @property
    def sessionId(self) -> str | None:
        """Get the session ID if set."""
        return self._params.get(self.PARAM_SESSION_ID)


def create_kbase_channel(
    host: str,
    port: int = KBaseChannelBuilder.DEFAULT_PORT,
    kbase_token: str = "",
    use_ssl: bool = False,
) -> grpc.Channel:
    """
    Convenience function to create a KBase-authenticated gRPC channel.

    This is useful when you need the raw gRPC channel instead of a
    SparkSession.

    Args:
        host: Hostname of the Spark Connect server.
        port: Port of the Spark Connect server.
        kbase_token: The KBase authentication token.
        use_ssl: Whether to use SSL/TLS.

    Returns:
        A configured gRPC channel.
    """
    builder = KBaseChannelBuilder(
        host=host,
        port=port,
        kbase_token=kbase_token,
        use_ssl=use_ssl,
    )
    return builder.toChannel()
