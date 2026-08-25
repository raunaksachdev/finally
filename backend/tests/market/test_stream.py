"""Tests for the SSE streaming endpoint (app/market/stream.py).

Note: full HTTP round-trip testing via httpx's ASGITransport (or FastAPI's
TestClient, which is built on the same buffering transport in the installed
dependency versions) is not viable here — both drive the ASGI app to
completion inside a single `await` before returning any response to the
caller, which deadlocks against `_generate_events`'s infinite loop. Instead
these tests drive the real generator and route-handler coroutine directly,
which exercises the same code paths without relying on a transport that
can stream partial responses.
"""

import asyncio
import json

import pytest
from fastapi.responses import StreamingResponse

from app.market.cache import PriceCache
from app.market.stream import _generate_events, create_stream_router


class _FakeClient:
    def __init__(self, host: str = "test-client") -> None:
        self.host = host


class _FakeRequest:
    """Minimal stand-in for fastapi.Request exposing what _generate_events uses."""

    def __init__(self, disconnect_after: int | None = None) -> None:
        self.client = _FakeClient()
        self._calls = 0
        self._disconnect_after = disconnect_after

    async def is_disconnected(self) -> bool:
        self._calls += 1
        if self._disconnect_after is not None and self._calls > self._disconnect_after:
            return True
        return False


@pytest.mark.asyncio
async def test_generate_events_yields_retry_directive_first():
    cache = PriceCache()
    request = _FakeRequest(disconnect_after=0)
    gen = _generate_events(cache, request, interval=0.01)

    first = await gen.__anext__()
    assert first == "retry: 1000\n\n"

    with pytest.raises(StopAsyncIteration):
        await gen.__anext__()


@pytest.mark.asyncio
async def test_generate_events_emits_prices_on_version_change():
    cache = PriceCache()
    cache.update("AAPL", 190.50)
    request = _FakeRequest(disconnect_after=1)
    gen = _generate_events(cache, request, interval=0.01)

    await gen.__anext__()  # retry directive
    data_event = await gen.__anext__()

    assert data_event.startswith("data: ")
    payload = json.loads(data_event.removeprefix("data: ").strip())
    assert payload["AAPL"]["price"] == 190.50

    with pytest.raises(StopAsyncIteration):
        await gen.__anext__()


@pytest.mark.asyncio
async def test_generate_events_sends_no_data_event_when_cache_empty():
    cache = PriceCache()
    request = _FakeRequest(disconnect_after=2)
    gen = _generate_events(cache, request, interval=0.01)

    events = [event async for event in gen]

    # Only the retry directive - no data event, since the cache never had prices
    assert events == ["retry: 1000\n\n"]


@pytest.mark.asyncio
async def test_generate_events_skips_unchanged_version():
    """A second tick with no cache write should not repeat the data event."""
    cache = PriceCache()
    cache.update("AAPL", 100.0)
    request = _FakeRequest(disconnect_after=2)
    gen = _generate_events(cache, request, interval=0.01)

    events = [event async for event in gen]

    # retry directive + exactly one data event (version unchanged on 2nd tick)
    assert len(events) == 2
    assert events[0] == "retry: 1000\n\n"
    assert events[1].startswith("data: ")


@pytest.mark.asyncio
async def test_generate_events_stops_on_cancellation():
    """_generate_events catches CancelledError internally (to log a clean
    disconnect) rather than propagating it, so the consuming task finishes
    normally instead of raising or hanging."""
    cache = PriceCache()
    cache.update("AAPL", 100.0)
    request = _FakeRequest()  # never disconnects on its own

    async def consume():
        async for _ in _generate_events(cache, request, interval=0.05):
            pass

    task = asyncio.create_task(consume())
    await asyncio.sleep(0.05)
    task.cancel()

    await asyncio.wait_for(task, timeout=1)
    assert task.done()
    assert not task.cancelled()


@pytest.mark.asyncio
async def test_stream_prices_route_returns_streaming_response():
    """create_stream_router wires the route to a StreamingResponse over _generate_events."""
    cache = PriceCache()
    cache.update("AAPL", 190.50)
    router = create_stream_router(cache)
    endpoint = router.routes[-1].endpoint

    response = await endpoint(_FakeRequest())
    try:
        assert isinstance(response, StreamingResponse)
        assert response.media_type == "text/event-stream"
        assert response.headers["cache-control"] == "no-cache"
        assert response.headers["x-accel-buffering"] == "no"

        first = await response.body_iterator.__anext__()
        assert first == "retry: 1000\n\n"
        second = await response.body_iterator.__anext__()
        assert second.startswith("data: ")
    finally:
        await response.body_iterator.aclose()
