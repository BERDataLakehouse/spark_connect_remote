"""
KBase Auth2 client for validating tokens and retrieving user information.

This module provides a client for interacting with the KBase Auth2 service
to validate tokens and retrieve user information such as username and roles.

Note: This client is primarily used for retrieving user information (e.g., username).
Server-side token validation is performed by KBaseAuthServerInterceptor in the
Spark Connect server.
"""

import logging
from dataclasses import dataclass
from datetime import datetime
from typing import Any

import httpx

logger = logging.getLogger(__name__)


# Default KBase Auth2 service URLs
DEFAULT_AUTH_URL = "https://kbase.us/services/auth/"


@dataclass
class KBaseTokenInfo:
    """
    Information about a validated KBase token.

    Attributes:
        user: The username associated with the token.
        token_id: The unique identifier for the token.
        created: When the token was created.
        expires: When the token expires.
        token_type: The type of token (e.g., 'Login', 'Developer', 'Service').
        custom_roles: List of custom roles assigned to the user.
    """

    user: str
    token_id: str
    created: datetime
    expires: datetime
    token_type: str
    custom_roles: list[str]

    def is_expired(self) -> bool:
        """Check if the token has expired."""
        return datetime.now() > self.expires

    def has_role(self, role: str) -> bool:
        """Check if the user has a specific custom role."""
        return role in self.custom_roles


class KBaseAuthError(Exception):
    """Exception raised for KBase authentication errors."""

    pass


class KBaseAuthClient:
    """
    Client for the KBase Auth2 service.

    This client provides methods for validating KBase authentication tokens
    and retrieving user information.

    Example:
        client = KBaseAuthClient()
        token_info = client.validate_token("my-kbase-token")
        print(f"User: {token_info.user}")

        # Or just get the username
        username = client.get_username("my-kbase-token")
    """

    DEFAULT_AUTH_URL = DEFAULT_AUTH_URL
    DEFAULT_TIMEOUT = 10.0

    def __init__(
        self,
        auth_url: str = DEFAULT_AUTH_URL,
        timeout: float = DEFAULT_TIMEOUT,
    ):
        """
        Initialize the KBase Auth client.

        Args:
            auth_url: URL of the KBase Auth2 service.
            timeout: Request timeout in seconds.
        """
        # Ensure auth_url ends with /
        self._auth_url = auth_url.rstrip("/") + "/"
        self._timeout = timeout

    def validate_token(self, token: str) -> KBaseTokenInfo:
        """
        Validate a KBase token and retrieve token information.

        Args:
            token: The KBase authentication token.

        Returns:
            KBaseTokenInfo containing user information and token details.

        Raises:
            KBaseAuthError: If the token is invalid or the request fails.
        """
        if not token or not token.strip():
            raise KBaseAuthError("Token cannot be empty")

        url = f"{self._auth_url}api/V2/token"

        try:
            response = httpx.get(
                url,
                headers={"Authorization": token.strip()},
                timeout=self._timeout,
            )

            if response.status_code == 401:
                raise KBaseAuthError("Invalid or expired token")

            if response.status_code != 200:
                raise KBaseAuthError(
                    f"Authentication service returned status {response.status_code}"
                )

            if not response.text or not response.text.strip():
                raise KBaseAuthError("Empty response from authentication service")

            data = response.json()
            return self._parse_token_response(data)

        except httpx.TimeoutException as e:
            raise KBaseAuthError("Request to authentication service timed out") from e
        except httpx.HTTPError as e:
            raise KBaseAuthError(f"Failed to connect to authentication service: {e}") from e
        except ValueError as e:
            raise KBaseAuthError(f"Invalid response from authentication service: {e}") from e

    def _parse_token_response(self, data: dict[str, Any]) -> KBaseTokenInfo:
        """Parse the token validation response into a KBaseTokenInfo object."""
        try:
            # Parse timestamps (milliseconds since epoch)
            created = datetime.fromtimestamp(data["created"] / 1000)
            expires = datetime.fromtimestamp(data["expires"] / 1000)

            return KBaseTokenInfo(
                user=data["user"],
                token_id=data["id"],
                created=created,
                expires=expires,
                token_type=data.get("type", "Unknown"),
                custom_roles=data.get("customroles", []),
            )
        except KeyError as e:
            raise KBaseAuthError(f"Missing required field in token response: {e}") from e

    def get_username(self, token: str) -> str:
        """
        Get the username associated with a token.

        This is a convenience method that validates the token and returns
        just the username.

        Args:
            token: The KBase authentication token.

        Returns:
            The username associated with the token.

        Raises:
            KBaseAuthError: If the token is invalid or the request fails.
        """
        token_info = self.validate_token(token)
        return token_info.user

    def get_user_info(self, token: str) -> dict[str, Any]:
        """
        Get detailed user information for a token.

        Args:
            token: The KBase authentication token.

        Returns:
            Dictionary containing user information including:
            - user: username
            - display: display name
            - email: email address
            - customroles: list of custom roles

        Raises:
            KBaseAuthError: If the token is invalid or the request fails.
        """
        if not token or not token.strip():
            raise KBaseAuthError("Token cannot be empty")

        url = f"{self._auth_url}api/V2/me"

        try:
            response = httpx.get(
                url,
                headers={"Authorization": token.strip()},
                timeout=self._timeout,
            )

            if response.status_code == 401:
                raise KBaseAuthError("Invalid or expired token")

            if response.status_code != 200:
                raise KBaseAuthError(
                    f"Authentication service returned status {response.status_code}"
                )

            if not response.text or not response.text.strip():
                raise KBaseAuthError("Empty response from authentication service")

            return response.json()

        except httpx.TimeoutException as e:
            raise KBaseAuthError("Request to authentication service timed out") from e
        except httpx.HTTPError as e:
            raise KBaseAuthError(f"Failed to connect to authentication service: {e}") from e
        except ValueError as e:
            raise KBaseAuthError(f"Invalid response from authentication service: {e}") from e

    @property
    def auth_url(self) -> str:
        """Get the Auth2 service URL."""
        return self._auth_url
