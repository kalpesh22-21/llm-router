"""
LLM Guard external URL filter implementation.
"""

from typing import Any, Dict, Optional, Tuple

from app.services.filters.base import ExternalURLFilter, SupportedExternalURLType


class LLMGuardFilter(ExternalURLFilter):
    """
    LLM Guard API filter for content scanning.

    LLM Guard is an open-source security toolkit for LLM interactions.
    It provides input and output scanning capabilities.

    Expected payload format:
    {
        "text": "<content to scan>",
        ... additional parameters from extra_params
    }

    Expected response format:
    {
        "is_violation": bool,
        "message": "...",
        "matched_text": "..."
    }
    """

    @property
    def filter_type(self) -> str:
        """Return the filter type identifier."""
        return SupportedExternalURLType.LLM_GUARD.value

    def form_payload(
        self, content: str, extra_params: Optional[Dict[str, Any]]
    ) -> Dict[str, Any]:
        """
        Form payload for LLM Guard API.

        Args:
            content: The text content to scan
            extra_params: Additional parameters from the rule configuration

        Returns:
            Dictionary containing the request payload
        """
        payload = {"text": content}

        # Merge extra_params into payload, excluding 'type'
        if extra_params:
            for key, value in extra_params.items():
                if key != "type":
                    payload[key] = value

        return payload

    def convert_response(
        self, response_data: Dict[str, Any], default_message: Optional[str]
    ) -> Tuple[bool, Optional[str], Optional[str]]:
        """
        Convert LLM Guard response to filter result.

        Args:
            response_data: The JSON response from LLM Guard API
            default_message: Default message to use if not provided in response

        Returns:
            Tuple of (is_violation, matched_text, message)
        """
        is_violation = response_data.get("is_violation", False)
        message = response_data.get("message", default_message)
        matched_text = response_data.get("matched_text", None)

        return is_violation, matched_text, message
