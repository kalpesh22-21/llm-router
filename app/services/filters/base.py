"""
Base class for external URL content filters.
"""

from abc import ABC, abstractmethod
from enum import Enum
from typing import Any, Dict, List, Optional, Tuple


class SupportedExternalURLType(str, Enum):
    """Supported external URL types for content filtering."""

    LLM_GUARD = "llm_guard"
    PRESIDIO = "presidio"

    @classmethod
    def is_supported(cls, url_type: str) -> bool:
        """Check if the given URL type is supported."""
        return url_type in [member.value for member in cls]

    @classmethod
    def get_supported_types(cls) -> List[str]:
        """Get list of supported URL types."""
        return [member.value for member in cls]


class ExternalURLFilter(ABC):
    """
    Abstract base class for external URL content filters.

    Each implementation must provide:
    - form_payload: Method to create the request payload
    - convert_response: Method to convert API response to filter result
    """

    @property
    @abstractmethod
    def filter_type(self) -> str:
        """Return the filter type identifier."""
        pass

    @abstractmethod
    def form_payload(
        self, content: str, extra_params: Optional[Dict[str, Any]]
    ) -> Dict[str, Any]:
        """
        Form the request payload for the external API.

        Args:
            content: The text content to check
            extra_params: Additional parameters from the rule configuration

        Returns:
            Dictionary containing the request payload
        """
        pass

    @abstractmethod
    def convert_response(
        self, response_data: Dict[str, Any], default_message: Optional[str]
    ) -> Tuple[bool, Optional[str], Optional[str]]:
        """
        Convert external API response to filter result.

        Args:
            response_data: The JSON response from the external API
            default_message: Default message to use if not provided in response

        Returns:
            Tuple of (is_violation, matched_text, message)
        """
        pass
