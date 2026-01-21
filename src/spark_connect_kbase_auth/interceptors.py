"""
gRPC interceptors for KBase authentication with Spark Connect.

This module provides client-side gRPC interceptors that add KBase
authentication tokens to Spark Connect requests.

The interceptor adds authentication metadata to all gRPC requests.
Token validation should be done separately using KBaseAuthClient.
"""

import logging
from collections.abc import Callable
from dataclasses import dataclass
from typing import Any

import grpc

logger = logging.getLogger(__name__)


# Metadata keys for KBase authentication
KBASE_TOKEN_METADATA_KEY = "x-kbase-token"
AUTHORIZATION_METADATA_KEY = "authorization"


@dataclass
class KBaseAuthMetadata:
    """
    Metadata to be added to gRPC requests for KBase authentication.

    This class encapsulates the authentication metadata that will be
    sent with each Spark Connect gRPC request.
    """

    token: str
    """The KBase authentication token."""

    def to_metadata(self) -> list[tuple[str, str]]:
        """
        Convert to gRPC metadata tuples.

        Returns:
            List of (key, value) tuples for gRPC metadata.
        """
        return [
            (AUTHORIZATION_METADATA_KEY, f"Bearer {self.token}"),
            (KBASE_TOKEN_METADATA_KEY, self.token),
        ]


class _ClientCallDetails(grpc.ClientCallDetails):
    """Custom ClientCallDetails that allows setting metadata."""

    def __init__(
        self,
        method: str,
        timeout: float | None,
        metadata: list[tuple[str, str]] | None,
        credentials: grpc.CallCredentials | None,
        wait_for_ready: bool | None,
        compression: grpc.Compression | None,
    ):
        self.method = method
        self.timeout = timeout
        self.metadata = metadata
        self.credentials = credentials
        self.wait_for_ready = wait_for_ready
        self.compression = compression


class KBaseAuthInterceptor(grpc.UnaryStreamClientInterceptor):
    """
    gRPC client interceptor that adds KBase authentication to requests.

    This interceptor adds the authentication token to each gRPC request.
    Token validation should be done separately using KBaseAuthClient.

    Example:
        interceptor = KBaseAuthInterceptor(token="my-kbase-token")

        # Use with a ChannelBuilder
        builder = DefaultChannelBuilder("sc://spark-server:15002")
        builder.add_interceptor(interceptor)
    """

    def __init__(self, token: str):
        """
        Initialize the KBase auth interceptor.

        Args:
            token: The KBase authentication token.
        """
        self._token = token

    @property
    def token(self) -> str:
        """Get the KBase token."""
        return self._token

    def _build_metadata(
        self,
        existing_metadata: tuple[tuple[str, str], ...] | None = None,
    ) -> list[tuple[str, str]]:
        """
        Build complete metadata including auth information.

        Args:
            existing_metadata: Pre-existing metadata to preserve.

        Returns:
            Combined metadata list with authentication added.
        """
        metadata = list(existing_metadata) if existing_metadata else []

        # Add auth metadata
        auth_metadata = KBaseAuthMetadata(token=self._token)
        metadata.extend(auth_metadata.to_metadata())

        return metadata

    def intercept_unary_stream(
        self,
        continuation: Callable[..., grpc.Call],
        client_call_details: grpc.ClientCallDetails,
        request: Any,
    ) -> grpc.Call:
        """
        Intercept unary-stream calls to add authentication metadata.

        This is the primary method called for Spark Connect operations.

        Args:
            continuation: The next handler in the chain.
            client_call_details: Details about the call.
            request: The request message.

        Returns:
            The result of continuing the call.
        """
        # Build new metadata with auth
        new_metadata = self._build_metadata(client_call_details.metadata)

        # Create new call details with updated metadata
        new_details = _ClientCallDetails(
            method=client_call_details.method,
            timeout=client_call_details.timeout,
            metadata=new_metadata,
            credentials=client_call_details.credentials,
            wait_for_ready=client_call_details.wait_for_ready,
            compression=client_call_details.compression,
        )

        return continuation(new_details, request)
