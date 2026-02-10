"""Tests for KBase channel builder."""

from unittest.mock import MagicMock, patch

import grpc

from spark_connect_remote.channel_builder import (
    KBaseChannelBuilder,
    create_kbase_channel,
)


class TestKBaseChannelBuilder:
    """Tests for KBaseChannelBuilder."""

    @patch("spark_connect_remote.channel_builder.KBaseAuthInterceptor")
    def test_init_basic(self, mock_interceptor_class: MagicMock):
        """Test basic initialization."""
        builder = KBaseChannelBuilder(
            host="spark-server",
            port=15002,
            kbase_token="test-token",
        )

        assert builder.host == "spark-server"
        assert builder.port == 15002
        assert builder.endpoint == "spark-server:15002"
        assert not builder.secure
        mock_interceptor_class.assert_called_once_with(token="test-token")

    def test_init_with_ssl(self):
        """Test initialization with SSL."""
        builder = KBaseChannelBuilder(
            host="spark-server",
            use_ssl=True,
        )

        assert builder.secure

    def test_default_port(self):
        """Test default port is 15002."""
        builder = KBaseChannelBuilder(host="spark-server")
        assert builder.port == 15002

    def test_set_get_params(self):
        """Test setting and getting parameters."""
        builder = KBaseChannelBuilder(host="spark-server")
        builder.set("custom-param", "custom-value")

        assert builder.get("custom-param") == "custom-value"
        assert builder.get("nonexistent") is None

    def test_get_default(self):
        """Test getDefault with fallback."""
        builder = KBaseChannelBuilder(host="spark-server")

        assert builder.getDefault("nonexistent", "default") == "default"
        builder.set("existing", "value")
        assert builder.getDefault("existing", "default") == "value"

    def test_set_channel_option(self):
        """Test setting channel options."""
        builder = KBaseChannelBuilder(host="spark-server")
        result = builder.setChannelOption("grpc.max_send_message_length", 256 * 1024 * 1024)

        # Should return self for chaining
        assert result is builder

    def test_add_interceptor(self):
        """Test adding custom interceptor."""
        builder = KBaseChannelBuilder(host="spark-server")
        mock_interceptor = MagicMock()

        result = builder.add_interceptor(mock_interceptor)

        # Should return self for chaining
        assert result is builder
        assert mock_interceptor in builder._interceptors

    def test_metadata(self):
        """Test metadata generation."""
        builder = KBaseChannelBuilder(host="spark-server")
        builder.set("x-custom-header", "custom-value")
        builder.set("x-tenant-id", "tenant-123")

        metadata = builder.metadata()

        assert ("x-custom-header", "custom-value") in metadata
        assert ("x-tenant-id", "tenant-123") in metadata

    def test_metadata_excludes_reserved(self):
        """Test that reserved parameters are excluded from metadata."""
        builder = KBaseChannelBuilder(host="spark-server")
        builder.set("use_ssl", "true")  # Reserved
        builder.set("token", "secret")  # Reserved
        builder.set("x-custom", "value")  # Not reserved

        metadata = builder.metadata()

        assert not any(key == "use_ssl" for key, _ in metadata)
        assert not any(key == "token" for key, _ in metadata)
        assert ("x-custom", "value") in metadata

    def test_to_channel_insecure(self):
        """Test creating insecure channel."""
        builder = KBaseChannelBuilder(
            host="localhost",
            port=15002,
            use_ssl=False,
        )

        channel = builder.toChannel()
        assert channel is not None

    def test_to_channel_secure(self):
        """Test creating secure channel."""
        builder = KBaseChannelBuilder(
            host="spark-server.example.com",
            port=15002,
            use_ssl=True,
        )

        channel = builder.toChannel()
        assert channel is not None

    def test_user_agent(self):
        """Test user agent string."""
        builder = KBaseChannelBuilder(host="spark-server")

        # Default user agent
        assert "spark-connect-remote" in builder.userAgent

        # Custom user agent
        builder.set("user_agent", "MyApp/1.0")
        assert builder.userAgent == "MyApp/1.0"

    def test_session_id(self):
        """Test session ID."""
        builder = KBaseChannelBuilder(host="spark-server")

        assert builder.sessionId is None

        builder.set("session_id", "session-123")
        assert builder.sessionId == "session-123"

    def test_user_id(self):
        """Test user ID."""
        builder = KBaseChannelBuilder(host="spark-server")

        assert builder.userId is None

        builder.set("user_id", "user-123")
        assert builder.userId == "user-123"


class TestCreateKBaseChannel:
    """Tests for create_kbase_channel function."""

    @patch("spark_connect_remote.channel_builder.KBaseAuthInterceptor")
    def test_create_channel(self, mock_interceptor_class: MagicMock):
        """Test creating channel via convenience function."""
        # Make mock properly implement gRPC interceptor interface
        mock_interceptor = MagicMock(spec=grpc.UnaryStreamClientInterceptor)
        mock_interceptor_class.return_value = mock_interceptor

        channel = create_kbase_channel(
            host="localhost",
            port=15002,
            kbase_token="test-token",
        )

        assert channel is not None
        mock_interceptor_class.assert_called_once()

    @patch("spark_connect_remote.channel_builder.KBaseAuthInterceptor")
    def test_create_channel_with_ssl(self, mock_interceptor_class: MagicMock):
        """Test creating SSL channel via convenience function."""
        mock_interceptor = MagicMock(spec=grpc.UnaryStreamClientInterceptor)
        mock_interceptor_class.return_value = mock_interceptor

        channel = create_kbase_channel(
            host="spark-server.example.com",
            port=15002,
            kbase_token="test-token",
            use_ssl=True,
        )

        assert channel is not None
