"""
Content filtering service for checking messages against filter rules.

Supports three types of filtering:
- REGEX: Pattern matching using regular expressions
- KEYWORD: Simple keyword matching (case-insensitive)
- EXTERNAL_URL: HTTP POST to external API for content checking
"""

from __future__ import annotations

import asyncio
from enum import Enum
import json
import logging
import re
from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Set, Tuple

import httpx
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.db.mysql import AsyncSessionLocal, ContentFilterRule
from app.core.config import settings
from app.core.httpx import get_http_client
from app.services.filters import (
    SupportedExternalURLType,
    LLMGuardFilter,
    PresidioFilter,
)

logger = logging.getLogger(__name__)
# ---------------------------------------------------------------------------
# Data classes
# --------------------------------------------------------------------------_


@dataclass
class FilterResult:
    """Result of content filtering check."""

    is_violation: bool
    rule_name: Optional[str] = None
    rule_type: Optional[str] = None
    message: Optional[str] = None
    matched_text: Optional[str] = None
    is_blocking: bool = False

    def __bool__(self) -> bool:
        return self.is_violation


# ---------------------------------------------------------------------------
# Abstract content checker
# ---------------------------------------------------------------------------


class ContentChecker(ABC):
    """Abstract base class for content checking implementations."""

    @abstractmethod
    async def check(
        self, content: str, rule: ContentFilterRule
    ) -> Tuple[bool, Optional[str], Optional[str]]:
        """
        Check if content violates the rule.

        Returns:
            Tuple of (is_violation, matched_text, violation_message)
        """
        pass


# ---------------------------------------------------------------------------
# Concrete content checkers
# ---------------------------------------------------------------------------


class RegexContentChecker(ContentChecker):
    """Check content against a regex pattern."""

    async def check(
        self, content: str, rule: ContentFilterRule
    ) -> Tuple[bool, Optional[str], Optional[str]]:
        if not rule.pattern:
            return False, None, rule.violation_message

        try:
            pattern = re.compile(rule.pattern, re.IGNORECASE)
            match = pattern.search(content)
            if match:
                return True, match.group(0), rule.violation_message
        except re.error as e:
            logger.error(f"Invalid regex pattern in rule '{rule.name}': {e}")
            return False, None, rule.violation_message

        return False, None, rule.violation_message


class KeywordContentChecker(ContentChecker):
    """Check content for a keyword (case-insensitive)."""

    async def check(
        self, content: str, rule: ContentFilterRule
    ) -> Tuple[bool, Optional[str], Optional[str]]:
        if not rule.pattern:
            return False, None, rule.violation_message

        # Simple case-insensitive keyword search
        keyword = rule.pattern.lower()
        content_lower = content.lower()

        # Find all occurrences
        idx = content_lower.find(keyword)
        if idx != -1:
            matched = content[idx : idx + len(rule.pattern)]
            return True, matched, rule.violation_message

        return False, None, rule.violation_message


class ExternalUrlContentChecker(ContentChecker):
    """
    Check content via external HTTP API.

    Supports two types of external URL services:
    - LLM_GUARD: LLM Guard API for content scanning
    - PRESIDIO: Microsoft Presidio for PII detection

    The type is determined by the 'type' field in extra_params.
    If not specified, defaults to 'llm_guard'.

    Each type has specific payload formation and response conversion methods
    implemented in separate filter classes.
    """

    def __init__(self, timeout: float = 5.0):
        self.timeout = timeout
        # Initialize filter instances
        self._filters = {
            SupportedExternalURLType.LLM_GUARD.value: LLMGuardFilter(),
            SupportedExternalURLType.PRESIDIO.value: PresidioFilter(),
        }

    def _get_url_type(self, extra_params: Optional[Dict[str, Any]]) -> str:
        """Get the URL type from extra_params, defaulting to llm_guard."""
        if not extra_params:
            return SupportedExternalURLType.LLM_GUARD.value
        return extra_params.get("type", SupportedExternalURLType.LLM_GUARD.value)

    def _validate_url_type(self, url_type: str, rule_name: str) -> bool:
        """Validate that the URL type is supported."""
        if not SupportedExternalURLType.is_supported(url_type):
            logger.error(
                f"Rule '{rule_name}': Unsupported external URL type '{url_type}'. "
                f"Supported types: {SupportedExternalURLType.get_supported_types()}"
            )
            return False
        return True

    def _get_filter(self, url_type: str):
        """Get the appropriate filter instance for the URL type."""
        return self._filters.get(
            url_type, self._filters[SupportedExternalURLType.LLM_GUARD.value]
        )

    async def check(
        self, content: str, rule: ContentFilterRule
    ) -> Tuple[bool, Optional[str], Optional[str]]:
        if not rule.external_url:
            logger.warning(f"Rule '{rule.name}' has type EXTERNAL_URL but no URL set")
            return False, None, rule.violation_message

        try:
            client = get_http_client()

            # Get and validate URL type
            url_type = self._get_url_type(rule.extra_params)

            # Get the appropriate filter and form payload
            filter_instance = self._get_filter(url_type)
            payload = filter_instance.form_payload(content, rule.extra_params)

            response = await client.post(
                rule.external_url,
                json=payload,
                headers={"Content-Type": "application/json"},
                timeout=self.timeout,
            )
            response.raise_for_status()

            result = response.json()

            # Convert response using the appropriate filter
            is_violation, matched_text, message = filter_instance.convert_response(
                result, rule.violation_message
            )

            return is_violation, matched_text, message

        except httpx.TimeoutException:
            logger.warning(f"Timeout calling external URL for rule '{rule.name}'")
            # Fail open: don't block on external service timeout
            return False, None, rule.violation_message
        except httpx.HTTPError as e:
            logger.error(f"HTTP error calling external URL for rule '{rule.name}': {e}")
            # Fail open: don't block on external service error
            return False, None, rule.violation_message
        except json.JSONDecodeError as e:
            logger.error(f"Invalid JSON from external URL for rule '{rule.name}': {e}")
            return False, None, rule.violation_message


# ---------------------------------------------------------------------------
# Content filter service
# ---------------------------------------------------------------------------
class SharedFiltersState:
    def __init__(self):
        self.rules: List[ContentFilterRule] = []
        self.lock = asyncio.Lock()


async def background_rules_updater(shared_filter_state: SharedFiltersState):
    while True:
        # Simulate I/O or delay
        await asyncio.sleep(settings.CONTENT_FILTER_REFRESH)
        async with AsyncSessionLocal() as db:
            result = await db.execute(
                select(ContentFilterRule)
                .where(ContentFilterRule.is_active == True)
                .order_by(ContentFilterRule.created_at.asc())
            )
        async with shared_filter_state.lock:  # Acquire lock before modification [6, 12]
            shared_filter_state.rules = list(result.scalars().all())
        logger.debug("Rules Updated.")


class ContentFilterService:
    """
    Service for managing and applying content filter rules.

    Rules are checked in order of creation. First match wins.
    """

    # Rule type to checker class mapping
    CHECKERS: Dict[str, type[ContentChecker]] = {
        "REGEX": RegexContentChecker,
        "KEYWORD": KeywordContentChecker,
        "EXTERNAL_URL": ExternalUrlContentChecker,
    }

    def __init__(self):
        self._checker_instances: Dict[str, ContentChecker] = {}
        self.shared_filter_state: SharedFiltersState = SharedFiltersState()
        self._background_task: Optional[asyncio.Task] = None

    def start_background_updater(self) -> None:
        """Start the background task to update filter rules periodically."""
        if self._background_task is None:
            self._background_task = asyncio.create_task(
                background_rules_updater(self.shared_filter_state)
            )

    def stop_background_updater(self) -> None:
        """Stop the background task."""
        if self._background_task is not None:
            self._background_task.cancel()
            self._background_task = None

    def _get_checker(self, rule_type: str) -> ContentChecker:
        """Get or create a checker instance for the given rule type."""
        if rule_type not in self._checker_instances:
            checker_class = self.CHECKERS.get(rule_type)
            if checker_class is None:
                raise ValueError(f"Unknown rule type: {rule_type}")
            self._checker_instances[rule_type] = checker_class()
        return self._checker_instances[rule_type]

    async def check_content(
        self, content: str, fields: List[str] = None
    ) -> FilterResult:
        """
        Check content against all active filter rules.

        Args:
            db: Database session
            content: Text content to check
            fields: Optional list of specific field names to check (default: all)

        Returns:
            FilterResult with violation details if any rule matches
        """
        async with self.shared_filter_state.lock:
            rules = self.shared_filter_state.rules.copy()

        if not rules:
            return FilterResult(is_violation=False)

        for rule in rules:
            # Skip rules that target specific fields if not in the list
            if fields and rule.fields_to_check:
                rule_fields = [f.strip() for f in rule.fields_to_check.split(",")]
                if not any(f in fields for f in rule_fields):
                    continue

            try:
                checker = self._get_checker(rule.rule_type)
                is_violation, matched_text, message = await checker.check(content, rule)

                if is_violation:
                    return FilterResult(
                        is_violation=True,
                        rule_name=rule.name,
                        rule_type=rule.rule_type,
                        is_blocking=rule.is_blocking,
                        message=message or f"Content violates rule: {rule.name}",
                        matched_text=matched_text,
                    )
            except Exception as e:
                logger.error(f"Error checking rule '{rule.name}': {e}")
                continue

        return FilterResult(is_violation=False)

    async def check_request_body(self, body: Dict[str, Any]) -> FilterResult:
        """
        Check a chat completion request body for content violations.

        Extracts content from messages array and checks each message's content.
        """
        messages = body.get("messages", [])
        if not messages:
            return FilterResult(is_violation=False)

        # Get all active rules to determine which fields to check
        async with self.shared_filter_state.lock:
            rules = self.shared_filter_state.rules.copy()
        if not rules:
            return FilterResult(is_violation=False)

        # Build set of fields that need checking
        fields_to_check: Set[str] = set()
        for rule in rules:
            if rule.fields_to_check:
                fields_to_check.update(
                    f.strip() for f in rule.fields_to_check.split(",")
                )
            else:
                fields_to_check.add("content")

        for message in messages:
            if not isinstance(message, dict):
                continue

            # Get the role and content from the message
            role = message.get("role", "")
            content = message.get("content", "")

            if not content:
                continue
            if isinstance(content, list):
                content_to_check: str = ""
                for text in content:
                    content_to_check += f"\n{text['text']}"
            elif isinstance(content, str):
                content_to_check = content
            else:
                continue
            # Check this message's content
            result = await self.check_content(
                content_to_check, fields=["content", f"{role}_content"]
            )
            if result.is_violation:
                return result

        return FilterResult(is_violation=False)

    # -----------------------------------------------------------------------
    # CRUD operations
    # -----------------------------------------------------------------------

    async def create_rule(
        self,
        db: AsyncSession,
        name: str,
        description: Optional[str],
        rule_type: str,
        pattern: Optional[str],
        external_url: Optional[str],
        extra_params: Optional[Dict[str, Any]],
        fields_to_check: Optional[str],
        is_active: bool,
        is_blocking: bool,
        violation_message: Optional[str],
        created_by: Optional[int] = None,
    ) -> ContentFilterRule:
        """Create a new content filter rule."""
        if rule_type not in self.CHECKERS:
            raise ValueError(f"Invalid rule type: {rule_type}")

        if extra_params:
            defined_type = extra_params.get("type", None)
            if not SupportedExternalURLType.is_supported(defined_type):
                raise ValueError(
                    f"{defined_type} is currently not supported. Only Supported Types are {SupportedExternalURLType.get_supported_types()}"
                )

        rule = ContentFilterRule(
            name=name,
            description=description,
            rule_type=rule_type,
            pattern=pattern,
            external_url=external_url,
            extra_params=extra_params,
            fields_to_check=fields_to_check or "content",
            is_active=is_active,
            is_blocking=is_blocking,
            violation_message=violation_message,
            created_by=created_by,
        )
        db.add(rule)
        await db.flush()
        return rule

    async def list_rules(self, db: AsyncSession) -> List[ContentFilterRule]:
        """Get all content filter rules."""
        result = await db.execute(
            select(ContentFilterRule).order_by(ContentFilterRule.created_at.desc())
        )
        return list(result.scalars().all())

    async def get_rule(
        self, db: AsyncSession, rule_id: int
    ) -> Optional[ContentFilterRule]:
        """Get a rule by ID."""
        result = await db.execute(
            select(ContentFilterRule).where(ContentFilterRule.id == rule_id)
        )
        return result.scalar_one_or_none()

    async def update_rule(
        self,
        db: AsyncSession,
        rule: ContentFilterRule,
        **kwargs,
    ) -> ContentFilterRule:
        """Update a content filter rule."""
        allowed_fields = {
            "name",
            "description",
            "rule_type",
            "pattern",
            "external_url",
            "extra_params",
            "fields_to_check",
            "is_active",
            "is_blocking",
            "violation_message",
        }

        for key, value in kwargs.items():
            if key in allowed_fields:
                setattr(rule, key, value)
        await db.flush()
        return rule

    async def delete_rule(self, db: AsyncSession, rule: ContentFilterRule) -> None:
        """Delete a content filter rule."""
        await db.delete(rule)
        await db.flush()

    async def get_rule_by_name(
        self, db: AsyncSession, name: str
    ) -> Optional[ContentFilterRule]:
        """Get a rule by name."""
        result = await db.execute(
            select(ContentFilterRule).where(ContentFilterRule.name == name)
        )
        return result.scalar_one_or_none()

    # -----------------------------------------------------------------------
    # Lifecycle
    # -----------------------------------------------------------------------

    async def close(self) -> None:
        """Close all checker instances."""
        for checker in self._checker_instances.values():
            if hasattr(checker, "close"):
                await checker.close()
        self._checker_instances.clear()


# Singleton instance
content_filter_service = ContentFilterService()
