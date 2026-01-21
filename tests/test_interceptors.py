"""Tests for gRPC interceptors."""

from unittest.mock import MagicMock

from spark_connect_kbase_auth.interceptors import (
    AUTHORIZATION_METADATA_KEY,
    KBASE_TOKEN_METADATA_KEY,
    KBaseAuthInterceptor,
    KBaseAuthMetadata,
)


class TestKBaseAuthMetadata:
    """Tests for KBaseAuthMetadata."""

    def test_to_metadata(self):
        """Test metadata generation."""
        metadata = KBaseAuthMetadata(token="test-token")
        result = metadata.to_metadata()

        assert (AUTHORIZATION_METADATA_KEY, "Bearer test-token") in result
        assert (KBASE_TOKEN_METADATA_KEY, "test-token") in result
        assert len(result) == 2


class TestKBaseAuthInterceptor:
    """Tests for KBaseAuthInterceptor."""

    def test_init(self):
        """Test interceptor initialization."""
        interceptor = KBaseAuthInterceptor(token="my-token")

        assert interceptor.token == "my-token"

    def test_intercept_adds_metadata(self):
        """Test that intercept adds authentication metadata."""
        interceptor = KBaseAuthInterceptor(token="test-token")

        # Create mock call details and continuation
        mock_details = MagicMock()
        mock_details.method = "/spark.connect.SparkConnectService/ExecutePlan"
        mock_details.timeout = 30.0
        mock_details.metadata = None
        mock_details.credentials = None
        mock_details.wait_for_ready = None
        mock_details.compression = None

        mock_continuation = MagicMock()
        mock_request = MagicMock()

        # Call intercept
        interceptor.intercept_unary_stream(mock_continuation, mock_details, mock_request)

        # Verify continuation was called with updated metadata
        mock_continuation.assert_called_once()
        call_args = mock_continuation.call_args
        new_details = call_args[0][0]

        # Check metadata was added
        metadata_dict = dict(new_details.metadata)
        assert "authorization" in metadata_dict
        assert metadata_dict["authorization"] == "Bearer test-token"
        assert "x-kbase-token" in metadata_dict
        assert metadata_dict["x-kbase-token"] == "test-token"

    def test_intercept_preserves_existing_metadata(self):
        """Test that intercept preserves existing metadata."""
        interceptor = KBaseAuthInterceptor(token="test-token")

        # Create mock with existing metadata
        mock_details = MagicMock()
        mock_details.method = "/test"
        mock_details.timeout = None
        mock_details.metadata = (("existing-key", "existing-value"),)
        mock_details.credentials = None
        mock_details.wait_for_ready = None
        mock_details.compression = None

        mock_continuation = MagicMock()

        interceptor.intercept_unary_stream(mock_continuation, mock_details, MagicMock())

        # Check both existing and new metadata
        new_details = mock_continuation.call_args[0][0]
        metadata_dict = dict(new_details.metadata)
        assert metadata_dict["existing-key"] == "existing-value"
        assert "authorization" in metadata_dict
        assert "x-kbase-token" in metadata_dict
