"""
Microsoft Presidio external URL filter implementation.
"""

from typing import Any, Dict, List, Optional, Tuple

from app.services.filters.base import ExternalURLFilter, SupportedExternalURLType


class PresidioFilter(ExternalURLFilter):
    """
    Microsoft Presidio API filter for PII (Personally Identifiable Information) detection.

    Presidio is an open-source data protection and anonymization SDK.
    It detects PII entities in text such as names, phone numbers, email addresses, etc.

    Expected payload format:
    {
        "text": "<content to analyze>",
        "language": "en",
        ... additional parameters from extra_params
    }

    Expected response format:
    {
        "entities": [
            {
                "type": "PERSON",
                "start": 0,
                "end": 10,
                "score": 0.85
            }
        ]
    }
    """

    @property
    def filter_type(self) -> str:
        """Return the filter type identifier."""
        return SupportedExternalURLType.PRESIDIO.value

    def form_payload(
        self, content: str, extra_params: Optional[Dict[str, Any]]
    ) -> Dict[str, Any]:
        """
        Form payload for Presidio API.

        Args:
            content: The text content to analyze
            extra_params: Additional parameters from the rule configuration

        Returns:
            Dictionary containing the request payload
        """
        payload = {
            "text": content,
            "language": "en",  # Default language
        }

        # Merge extra_params into payload, excluding 'type'
        if extra_params:
            for key, value in extra_params.items():
                if key != "type":
                    payload[key] = value

        return payload

    def convert_response(
        self, entities: List[Dict[str, Any]], default_message: Optional[str]
    ) -> Tuple[bool, Optional[str], Optional[str]]:
        """
        Convert Presidio response to filter result.

        Args:
            response_data: The JSON response from Presidio API
            default_message: Default message to use if not provided in response

        Returns:
            Tuple of (is_violation, matched_text, message)
        """
        if len(entities) > 0:
            is_violation = True
            # Build matched_text from entity types
            entity_types = [entity.get("entity_type", "UNKNOWN") for entity in entities]
            matched_text = ", ".join(entity_types)
            message = default_message or f"PII detected: {matched_text}"
        else:
            is_violation = False
            matched_text = None
            message = default_message

        return is_violation, matched_text, message
