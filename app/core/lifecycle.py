"""
Application lifecycle management for graceful shutdown.

This module provides:
- Signal handlers for SIGTERM/SIGINT
- Global shutdown state tracking
- Active request tracking
- Graceful shutdown middleware

When SIGTERM is received:
  1. Mark the application as shutting down
  2. Reject new requests with 503 Service Unavailable
  3. Wait for ongoing requests to complete (with timeout)
  4. Proceed with normal shutdown sequence
"""

from __future__ import annotations

import asyncio
import logging
import signal
from collections.abc import Callable
from types import FrameType
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from fastapi import FastAPI

logger = logging.getLogger(__name__)

# Global state for graceful shutdown
_shutting_down: bool = False
_active_requests: int = 0

_active_requests_lock: asyncio.Lock | None = None
_shutdown_event: asyncio.Event | None = None

_shutdown_timeout: float = 30.0

# Store original uvicorn signal handlers for chaining.
# We must call uvicorn's handlers to trigger the shutdown sequence that exits the lifespan.
# Type: callable function, SIG_IGN (ignore), SIG_DFL (default), or None
_original_sigterm_handler: Callable[[int, FrameType | None], Any] | int | None = None
_original_sigint_handler: Callable[[int, FrameType | None], Any] | int | None = None

# Reference to the event loop for signal handler use.
_loop: asyncio.AbstractEventLoop | None = None


def _get_lock() -> asyncio.Lock:
    """
    Lazily create and return the active-requests lock.

    Creating asyncio primitives at module level (before a running loop exists)
    raises RuntimeError in Python 3.12+. This helper ensures the lock is only
    ever created once the event loop is already running.
    """
    global _active_requests_lock
    if _active_requests_lock is None:
        _active_requests_lock = asyncio.Lock()
    return _active_requests_lock


def is_shutting_down() -> bool:
    """Check if the application is currently shutting down."""
    return _shutting_down


def get_active_requests() -> int:
    """Get the number of currently active requests."""
    return _active_requests


async def increment_active_requests() -> int:
    """
    Safely increment the active request counter.

    Returns:
        The new count after incrementing.
    """
    global _active_requests
    async with _get_lock():
        _active_requests += 1
        return _active_requests


async def decrement_active_requests() -> int:
    """
    Safely decrement the active request counter and signal if all complete.

    Returns:
        The new count after decrementing.
    """
    global _active_requests
    async with _get_lock():
        _active_requests -= 1
        new_count = _active_requests
        # Signal shutdown event if all requests are done during shutdown
        if _shutdown_event is not None and _active_requests == 0 and _shutting_down:
            _shutdown_event.set()
        return new_count


async def get_active_requests_safe() -> int:
    """
    Safely get the number of currently active requests.

    Returns:
        The current active request count.
    """
    async with _get_lock():
        return _active_requests


def _signal_handler(signum: int, frame: FrameType | None) -> None:
    """
    Handle SIGTERM/SIGINT signals with graceful request draining.

    Sets the global shutdown flag to signal that new requests should be rejected,
    schedules the drain coroutine non-blocking on the running event loop, then
    chains to uvicorn's original handler to trigger the actual shutdown.

    FIX 7 & 10: The previous implementation called future.result() on the main
    thread while the event loop also ran on the main thread, causing a deadlock.
    We now use call_soon_threadsafe + ensure_future so the event loop can drive
    the drain coroutine freely without blocking the signal handler.
    """
    global _shutting_down
    signame = signal.Signals(signum).name
    logger.warning("Received %s, starting graceful shutdown...", signame)
    _shutting_down = True

    original_handler = (
        _original_sigterm_handler if signum == signal.SIGTERM
        else _original_sigint_handler
    )

    if _loop is not None and _loop.is_running():
        logger.info(
            "Scheduling graceful drain before triggering uvicorn shutdown..."
        )

        async def _drain_then_chain() -> None:
            """Drain active requests, then chain to uvicorn's signal handler."""
            await wait_for_active_requests()
            logger.info(
                "Drain complete, chaining to original %s handler", signame
            )
            if callable(original_handler):
                original_handler(signum, frame)

        # Non-blocking: hand off to the running event loop.
        asyncio.run_coroutine_threadsafe(_drain_then_chain(),_loop)
    else:
        # No running loop — fall back to calling the original handler directly.
        logger.warning(
            "No running event loop found during signal handling; "
            "skipping drain and chaining immediately."
        )
        if callable(original_handler):
            original_handler(signum, frame)


def setup_signal_handlers() -> None:
    """
    Register signal handlers for graceful shutdown.

    FIX 9: Must be called from within a running coroutine (e.g. inside the
    lifespan startup) so that asyncio.get_running_loop() succeeds and _loop
    is correctly captured for use by the synchronous signal handler.
    """
    global _original_sigterm_handler, _original_sigint_handler, _loop
    _original_sigterm_handler = signal.signal(signal.SIGTERM, _signal_handler)
    _original_sigint_handler = signal.signal(signal.SIGINT, _signal_handler)
    # Capture the current event loop for use by the signal handler.
    try:
        _loop = asyncio.get_running_loop()
        logger.info(
            "Signal handlers registered for graceful shutdown (loop captured)"
        )
    except RuntimeError:
        # Should not happen when called from inside lifespan, but guard anyway.
        _loop = None
        logger.warning(
            "setup_signal_handlers() called with no running event loop; "
            "graceful drain will be skipped on shutdown signals."
        )


async def wait_for_active_requests(timeout: float | None = None) -> bool:
    """
    Wait for all active requests to complete.

    FIX 6: The shutdown event is now created BEFORE reading the active-request
    count so there is no window where a decrement can fire, find the event
    still None, and never wake up this waiter — which previously caused the
    function to hang until timeout even when no requests were active.

    Args:
        timeout: Maximum time to wait in seconds. None means use the default.

    Returns:
        True if all requests completed, False if timeout was reached.
    """
    global _shutdown_event

    if timeout is None:
        timeout = _shutdown_timeout

    # FIX 6: Create the event FIRST so decrement_active_requests() can always
    # set it, eliminating the race between count-check and event creation.
    _shutdown_event = asyncio.Event()

    # Now safely read the count while holding the lock.
    async with _get_lock():
        current_count = _active_requests
        # If already zero, fire the event immediately so wait_for returns at once.
        if current_count == 0:
            _shutdown_event.set()

    if current_count == 0:
        logger.info("No active requests to wait for")
        return True

    logger.info(
        "Waiting for %d active request(s) to complete (timeout: %.1fs)...",
        current_count, timeout
    )

    try:
        await asyncio.wait_for(_shutdown_event.wait(), timeout=timeout)
        logger.info("All active requests completed")
        return True
    except asyncio.TimeoutError:
        remaining_count = await get_active_requests_safe()
        logger.warning(
            "Graceful shutdown timeout reached after %.1fs, "
            "%d request(s) still active",
            timeout, remaining_count
        )
        return False