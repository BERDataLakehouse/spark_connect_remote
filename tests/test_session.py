"""Tests for session helper functions."""

import os
from unittest.mock import MagicMock, patch

import pytest

from spark_connect_kbase_auth.session import (
    DEFAULT_HOST_TEMPLATE,
    DEFAULT_PORT,
    ENV_KBASE_AUTH_TOKEN,
    ENV_KBASE_AUTH_URL,
    create_authenticated_session,
    get_authenticated_spark,
)


class TestCreateAuthenticatedSession:
    """Tests for create_authenticated_session function."""

    @patch("pyspark.sql.SparkSession")
    @patch("spark_connect_kbase_auth.session.KBaseAuthClient")
    def test_resolves_username_from_token(
        self, mock_auth_client_class: MagicMock, mock_spark_session: MagicMock
    ):
        """Test that username is resolved from token when host_template contains {username}."""
        mock_client = MagicMock()
        mock_client.get_username.return_value = "alice"
        mock_auth_client_class.return_value = mock_client

        mock_builder = MagicMock()
        mock_spark_session.builder.remote.return_value = mock_builder
        mock_builder.getOrCreate.return_value = MagicMock()

        create_authenticated_session(
            host_template="spark-connect-{username}.namespace",
            kbase_token="test-token",
        )

        # Verify username was fetched
        mock_client.get_username.assert_called_once_with("test-token")

        # Verify URL was built with resolved username
        mock_spark_session.builder.remote.assert_called_once()
        url = mock_spark_session.builder.remote.call_args[0][0]
        assert "spark-connect-alice.namespace" in url
        assert "token=test-token" in url

    @patch("pyspark.sql.SparkSession")
    @patch("spark_connect_kbase_auth.session.KBaseAuthClient")
    def test_validates_token_without_username_placeholder(
        self, mock_auth_client_class: MagicMock, mock_spark_session: MagicMock
    ):
        """Test that token is validated even when no {username} placeholder."""
        mock_client = MagicMock()
        mock_client.get_username.return_value = "alice"
        mock_auth_client_class.return_value = mock_client

        mock_builder = MagicMock()
        mock_spark_session.builder.remote.return_value = mock_builder
        mock_builder.getOrCreate.return_value = MagicMock()

        create_authenticated_session(
            host_template="fixed-host.namespace",
            kbase_token="test-token",
        )

        # Verify token was validated (even without {username})
        mock_client.get_username.assert_called_once_with("test-token")

        # Verify URL uses the fixed host (not resolved username)
        url = mock_spark_session.builder.remote.call_args[0][0]
        assert "fixed-host.namespace" in url
        assert "alice" not in url  # Username not used in host

    @patch("pyspark.sql.SparkSession")
    @patch("spark_connect_kbase_auth.session.KBaseAuthClient")
    def test_uses_custom_auth_url(
        self, mock_auth_client_class: MagicMock, mock_spark_session: MagicMock
    ):
        """Test that custom auth URL is passed to KBaseAuthClient."""
        mock_client = MagicMock()
        mock_client.get_username.return_value = "alice"
        mock_auth_client_class.return_value = mock_client

        mock_builder = MagicMock()
        mock_spark_session.builder.remote.return_value = mock_builder
        mock_builder.getOrCreate.return_value = MagicMock()

        create_authenticated_session(
            host_template="spark-connect-{username}.namespace",
            kbase_token="test-token",
            kbase_auth_url="https://custom-auth.example.com/",
        )

        mock_auth_client_class.assert_called_once_with(
            auth_url="https://custom-auth.example.com/"
        )

    @patch("pyspark.sql.SparkSession")
    @patch("spark_connect_kbase_auth.session.KBaseAuthClient")
    def test_uses_ssl_protocol(
        self, mock_auth_client_class: MagicMock, mock_spark_session: MagicMock
    ):
        """Test that SSL uses scs:// protocol."""
        mock_client = MagicMock()
        mock_client.get_username.return_value = "alice"
        mock_auth_client_class.return_value = mock_client

        mock_builder = MagicMock()
        mock_spark_session.builder.remote.return_value = mock_builder
        mock_builder.getOrCreate.return_value = MagicMock()

        create_authenticated_session(
            host_template="spark-connect-{username}.namespace",
            kbase_token="test-token",
            use_ssl=True,
        )

        url = mock_spark_session.builder.remote.call_args[0][0]
        assert url.startswith("scs://")

    @patch("pyspark.sql.SparkSession")
    @patch("spark_connect_kbase_auth.session.KBaseAuthClient")
    def test_uses_custom_port(
        self, mock_auth_client_class: MagicMock, mock_spark_session: MagicMock
    ):
        """Test that custom port is used in URL."""
        mock_client = MagicMock()
        mock_client.get_username.return_value = "alice"
        mock_auth_client_class.return_value = mock_client

        mock_builder = MagicMock()
        mock_spark_session.builder.remote.return_value = mock_builder
        mock_builder.getOrCreate.return_value = MagicMock()

        create_authenticated_session(
            host_template="spark-connect-{username}.namespace",
            kbase_token="test-token",
            port=15003,
        )

        url = mock_spark_session.builder.remote.call_args[0][0]
        assert ":15003/" in url

    @patch("pyspark.sql.SparkSession")
    @patch("spark_connect_kbase_auth.session.KBaseAuthClient")
    def test_sets_app_name(
        self, mock_auth_client_class: MagicMock, mock_spark_session: MagicMock
    ):
        """Test that app name is set on the session."""
        mock_client = MagicMock()
        mock_client.get_username.return_value = "alice"
        mock_auth_client_class.return_value = mock_client

        mock_builder = MagicMock()
        mock_spark_session.builder.remote.return_value = mock_builder
        mock_builder.appName.return_value = mock_builder
        mock_builder.getOrCreate.return_value = MagicMock()

        create_authenticated_session(
            host_template="spark-connect-{username}.namespace",
            kbase_token="test-token",
            app_name="TestApp",
        )

        mock_builder.appName.assert_called_once_with("TestApp")

    @patch("pyspark.sql.SparkSession")
    @patch("spark_connect_kbase_auth.session.KBaseAuthClient")
    def test_sets_spark_config(
        self, mock_auth_client_class: MagicMock, mock_spark_session: MagicMock
    ):
        """Test that spark config options are set."""
        mock_client = MagicMock()
        mock_client.get_username.return_value = "alice"
        mock_auth_client_class.return_value = mock_client

        mock_builder = MagicMock()
        mock_spark_session.builder.remote.return_value = mock_builder
        mock_builder.config.return_value = mock_builder
        mock_builder.getOrCreate.return_value = MagicMock()

        create_authenticated_session(
            host_template="spark-connect-{username}.namespace",
            kbase_token="test-token",
            spark_config={"spark.sql.shuffle.partitions": "200"},
        )

        mock_builder.config.assert_called_once_with(
            "spark.sql.shuffle.partitions", "200"
        )

    @patch.dict(os.environ, {ENV_KBASE_AUTH_TOKEN: "env-token"}, clear=False)
    @patch("pyspark.sql.SparkSession")
    @patch("spark_connect_kbase_auth.session.KBaseAuthClient")
    def test_token_from_env(
        self, mock_auth_client_class: MagicMock, mock_spark_session: MagicMock
    ):
        """Test token loaded from environment variable."""
        mock_client = MagicMock()
        mock_client.get_username.return_value = "alice"
        mock_auth_client_class.return_value = mock_client

        mock_builder = MagicMock()
        mock_spark_session.builder.remote.return_value = mock_builder
        mock_builder.getOrCreate.return_value = MagicMock()

        create_authenticated_session(
            host_template="spark-connect-{username}.namespace",
        )

        mock_client.get_username.assert_called_once_with("env-token")
        url = mock_spark_session.builder.remote.call_args[0][0]
        assert "token=env-token" in url

    def test_raises_without_token(self):
        """Test that ValueError is raised when no token is provided."""
        # Ensure env var is not set
        env = os.environ.copy()
        env.pop(ENV_KBASE_AUTH_TOKEN, None)

        with patch.dict(os.environ, env, clear=True):
            with pytest.raises(ValueError) as exc_info:
                create_authenticated_session(
                    host_template="spark-connect-{username}.namespace",
                )

            assert ENV_KBASE_AUTH_TOKEN in str(exc_info.value)


class TestGetAuthenticatedSpark:
    """Tests for get_authenticated_spark function."""

    @patch("spark_connect_kbase_auth.session.create_authenticated_session")
    def test_delegates_to_create_authenticated_session(self, mock_create: MagicMock):
        """Test that get_authenticated_spark delegates to create_authenticated_session."""
        mock_session = MagicMock()
        mock_create.return_value = mock_session

        result = get_authenticated_spark(
            host_template="spark-connect-{username}.namespace",
            kbase_token="test-token",
            port=15003,
        )

        assert result == mock_session
        mock_create.assert_called_once_with(
            host_template="spark-connect-{username}.namespace",
            kbase_token="test-token",
            port=15003,
        )


class TestDeprecatedCreateChannelBuilder:
    """Tests for deprecated create_channel_builder function."""

    def test_emits_deprecation_warning(self):
        """Test that create_channel_builder emits deprecation warning."""
        from spark_connect_kbase_auth.session import create_channel_builder

        with pytest.warns(DeprecationWarning, match="create_channel_builder is deprecated"):
            create_channel_builder(
                host="spark-server",
                kbase_token="test-token",
            )
