"""Tests for the KBase Auth2 client."""

from datetime import datetime, timedelta
from unittest.mock import MagicMock, patch

import httpx
import pytest

from spark_connect_remote.kbase_client import (
    DEFAULT_AUTH_URL,
    KBaseAuthClient,
    KBaseAuthError,
    KBaseTokenInfo,
)


class TestKBaseTokenInfo:
    """Tests for KBaseTokenInfo dataclass."""

    def test_is_expired_false(self):
        """Test token that hasn't expired yet."""
        token_info = KBaseTokenInfo(
            user="testuser",
            token_id="token123",
            created=datetime.now() - timedelta(hours=1),
            expires=datetime.now() + timedelta(hours=1),
            token_type="Login",
            custom_roles=["BERDL_USER"],
        )
        assert not token_info.is_expired()

    def test_is_expired_true(self):
        """Test token that has expired."""
        token_info = KBaseTokenInfo(
            user="testuser",
            token_id="token123",
            created=datetime.now() - timedelta(hours=2),
            expires=datetime.now() - timedelta(hours=1),
            token_type="Login",
            custom_roles=[],
        )
        assert token_info.is_expired()

    def test_has_role_true(self):
        """Test user has specific role."""
        token_info = KBaseTokenInfo(
            user="testuser",
            token_id="token123",
            created=datetime.now(),
            expires=datetime.now() + timedelta(hours=1),
            token_type="Login",
            custom_roles=["BERDL_USER", "ADMIN"],
        )
        assert token_info.has_role("BERDL_USER")
        assert token_info.has_role("ADMIN")

    def test_has_role_false(self):
        """Test user doesn't have specific role."""
        token_info = KBaseTokenInfo(
            user="testuser",
            token_id="token123",
            created=datetime.now(),
            expires=datetime.now() + timedelta(hours=1),
            token_type="Login",
            custom_roles=["BERDL_USER"],
        )
        assert not token_info.has_role("ADMIN")


class TestKBaseAuthClient:
    """Tests for KBaseAuthClient."""

    def test_init_default_url(self):
        """Test initialization with default auth URL."""
        client = KBaseAuthClient()
        assert client.auth_url == DEFAULT_AUTH_URL

    def test_init_custom_url(self):
        """Test initialization with custom auth URL."""
        client = KBaseAuthClient(auth_url="https://kbase.us/services/auth")
        # Should normalize to end with /
        assert client.auth_url == "https://kbase.us/services/auth/"

    def test_init_custom_timeout(self):
        """Test initialization with custom timeout."""
        client = KBaseAuthClient(timeout=5.0)
        assert client._timeout == 5.0

    def test_validate_token_empty(self):
        """Test validation fails with empty token."""
        client = KBaseAuthClient()
        with pytest.raises(KBaseAuthError, match="Token cannot be empty"):
            client.validate_token("")

    def test_validate_token_whitespace(self):
        """Test validation fails with whitespace-only token."""
        client = KBaseAuthClient()
        with pytest.raises(KBaseAuthError, match="Token cannot be empty"):
            client.validate_token("   ")

    @patch("spark_connect_remote.kbase_client.httpx.get")
    def test_validate_token_success(self, mock_get: MagicMock):
        """Test successful token validation."""
        # Mock successful response
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.text = '{"user": "testuser"}'
        mock_response.json.return_value = {
            "user": "testuser",
            "id": "token123",
            "created": 1700000000000,  # milliseconds
            "expires": 1700100000000,
            "type": "Login",
            "customroles": ["BERDL_USER"],
        }
        mock_get.return_value = mock_response

        client = KBaseAuthClient()
        token_info = client.validate_token("valid-token")

        assert token_info.user == "testuser"
        assert token_info.token_id == "token123"
        assert token_info.token_type == "Login"
        assert "BERDL_USER" in token_info.custom_roles

        # Verify the request was made correctly
        mock_get.assert_called_once()
        call_args = mock_get.call_args
        assert "api/V2/token" in call_args[0][0]
        assert call_args[1]["headers"]["Authorization"] == "valid-token"

    @patch("spark_connect_remote.kbase_client.httpx.get")
    def test_validate_token_unauthorized(self, mock_get: MagicMock):
        """Test validation fails with 401 response."""
        mock_response = MagicMock()
        mock_response.status_code = 401
        mock_get.return_value = mock_response

        client = KBaseAuthClient()
        with pytest.raises(KBaseAuthError, match="Invalid or expired token"):
            client.validate_token("invalid-token")

    @patch("spark_connect_remote.kbase_client.httpx.get")
    def test_validate_token_server_error(self, mock_get: MagicMock):
        """Test validation fails with server error."""
        mock_response = MagicMock()
        mock_response.status_code = 500
        mock_get.return_value = mock_response

        client = KBaseAuthClient()
        with pytest.raises(KBaseAuthError, match="status 500"):
            client.validate_token("some-token")

    @patch("spark_connect_remote.kbase_client.httpx.get")
    def test_validate_token_empty_response(self, mock_get: MagicMock):
        """Test validation fails with empty response body."""
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.text = ""
        mock_get.return_value = mock_response

        client = KBaseAuthClient()
        with pytest.raises(KBaseAuthError, match="Empty response"):
            client.validate_token("some-token")

    @patch("spark_connect_remote.kbase_client.httpx.get")
    def test_validate_token_timeout(self, mock_get: MagicMock):
        """Test validation fails on timeout."""
        mock_get.side_effect = httpx.TimeoutException("Connection timed out")

        client = KBaseAuthClient()
        with pytest.raises(KBaseAuthError, match="timed out"):
            client.validate_token("some-token")

    @patch("spark_connect_remote.kbase_client.httpx.get")
    def test_validate_token_connection_error(self, mock_get: MagicMock):
        """Test validation fails on connection error."""
        mock_get.side_effect = httpx.ConnectError("Connection refused")

        client = KBaseAuthClient()
        with pytest.raises(KBaseAuthError, match="Failed to connect"):
            client.validate_token("some-token")

    @patch("spark_connect_remote.kbase_client.httpx.get")
    def test_validate_token_missing_field(self, mock_get: MagicMock):
        """Test validation fails when required field is missing."""
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.text = '{"user": "testuser"}'
        mock_response.json.return_value = {
            "user": "testuser",
            # Missing 'id', 'created', 'expires' fields
        }
        mock_get.return_value = mock_response

        client = KBaseAuthClient()
        with pytest.raises(KBaseAuthError, match="Missing required field"):
            client.validate_token("some-token")

    @patch("spark_connect_remote.kbase_client.httpx.get")
    def test_get_username(self, mock_get: MagicMock):
        """Test get_username convenience method."""
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.text = '{"user": "testuser"}'
        mock_response.json.return_value = {
            "user": "testuser",
            "id": "token123",
            "created": 1700000000000,
            "expires": 1700100000000,
            "type": "Login",
            "customroles": [],
        }
        mock_get.return_value = mock_response

        client = KBaseAuthClient()
        username = client.get_username("valid-token")

        assert username == "testuser"

    @patch("spark_connect_remote.kbase_client.httpx.get")
    def test_get_user_info_success(self, mock_get: MagicMock):
        """Test successful user info retrieval."""
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.text = '{"user": "testuser"}'
        mock_response.json.return_value = {
            "user": "testuser",
            "display": "Test User",
            "email": "test@example.com",
            "customroles": ["BERDL_USER"],
        }
        mock_get.return_value = mock_response

        client = KBaseAuthClient()
        user_info = client.get_user_info("valid-token")

        assert user_info["user"] == "testuser"
        assert user_info["display"] == "Test User"
        assert user_info["email"] == "test@example.com"

        # Verify the request was made to /me endpoint
        mock_get.assert_called_once()
        call_args = mock_get.call_args
        assert "api/V2/me" in call_args[0][0]

    def test_get_user_info_empty_token(self):
        """Test get_user_info fails with empty token."""
        client = KBaseAuthClient()
        with pytest.raises(KBaseAuthError, match="Token cannot be empty"):
            client.get_user_info("")

    @patch("spark_connect_remote.kbase_client.httpx.get")
    def test_get_user_info_unauthorized(self, mock_get: MagicMock):
        """Test get_user_info fails with 401 response."""
        mock_response = MagicMock()
        mock_response.status_code = 401
        mock_get.return_value = mock_response

        client = KBaseAuthClient()
        with pytest.raises(KBaseAuthError, match="Invalid or expired token"):
            client.get_user_info("invalid-token")
