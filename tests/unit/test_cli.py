"""
CLI helper functions: _fetch_robots() -- the robots_fetcher wired into
PostgresFrontier in production, covering the port-loss regression (a URL
on a non-standard port must resolve robots.txt against that same port,
not a default-port guess) -- _blob_store()'s resource lifecycle -- and
_create_index_with_retry()'s Meilisearch-unavailable-at-startup retry.
"""
import asyncio
import functools
import tempfile
import threading
from http.server import SimpleHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from unittest.mock import AsyncMock

import pytest

from crawler.cli import _blob_store, _create_index_with_retry, _fetch_robots
from crawler.normalize import robots_origin
from crawler.policy import parse_robots


@pytest.fixture
def robots_server():
    """A dedicated fixture server with its own docroot containing a real
    robots.txt -- deliberately separate from tests/fixtures/ (shared by
    fixture_server and relied on elsewhere to have NO robots.txt) so this
    doesn't change behavior for any other test."""
    with tempfile.TemporaryDirectory() as tmp:
        (Path(tmp) / "robots.txt").write_text(
            "User-agent: *\nDisallow: /private\nCrawl-delay: 2\n"
        )
        handler = functools.partial(SimpleHTTPRequestHandler, directory=tmp)
        server = ThreadingHTTPServer(("127.0.0.1", 0), handler)
        thread = threading.Thread(target=server.serve_forever, daemon=True)
        thread.start()
        try:
            yield f"http://127.0.0.1:{server.server_port}"
        finally:
            server.shutdown()
            thread.join(timeout=5)


async def test_fetch_robots_uses_given_origin_authority(fixture_server):
    # fixture_server is plain HTTP on an OS-assigned (non-standard) port,
    # e.g. "http://127.0.0.1:51127" -- exactly the shape that previously
    # broke when robots fetching assumed https on a default port.
    origin = fixture_server
    body, status = await _fetch_robots("127.0.0.1", origin)

    assert status == 404  # no robots.txt fixture -> not found, not a connection failure


async def test_fetch_robots_origin_derived_from_url_round_trips(fixture_server):
    url = f"{fixture_server}/static_page.html"
    origin = robots_origin(url)

    body, status = await _fetch_robots("127.0.0.1", origin)

    assert status == 404


async def test_fetch_robots_no_origin_falls_back_https_then_http(fixture_server):
    # No source URL available: guesses https first, then falls back to
    # http only after a connection-level failure -- the pre-existing
    # degenerate path, unchanged.
    host = fixture_server.removeprefix("http://")  # "127.0.0.1:PORT"

    body, status = await _fetch_robots(host)

    assert status == 404


async def test_fetch_robots_keeps_body_for_real_text_plain_response(robots_server):
    """robots.txt is legitimately served as text/plain -- the de-facto
    standard content-type, confirmed live against Amazon/Wikipedia/IANA --
    not text/html. HttpFetcher.fetch() drops the body of any non-HTML
    response by design (it's meant to skip PDFs/images during a crawl),
    and _fetch_robots() used to call it with that filter still active, so
    a real 200 robots.txt response with real Disallow rules came back
    with body=None -- which parse_robots() treats identically to "no
    robots.txt exists" (fully permissive), silently defeating robots
    enforcement against virtually every real site. _fetch_robots() must
    pass expect_html=False so the body survives regardless of
    content-type. robots_server's SimpleHTTPRequestHandler serves it with
    a real Content-Type: text/plain (verified:
    mimetypes.guess_type('robots.txt') == 'text/plain'), exercising the
    exact real-world shape of the bug."""
    body, status = await _fetch_robots("127.0.0.1", robots_server)

    assert status == 200
    assert body is not None
    assert "Disallow: /private" in body

    # And the policy actually built from it must deny the disallowed path
    # and allow everything else -- not just "the body is non-empty".
    policy = parse_robots("127.0.0.1", body, status)
    assert policy.check_allowed(f"{robots_server}/private/x") is False
    assert policy.check_allowed(f"{robots_server}/public") is True


async def test_blob_store_returns_a_closeable_context_manager():
    """_blob_store() previously entered the aioboto3 S3 client's async
    context manager (session.client(...).__aenter__()) but discarded the
    context manager itself, keeping only the client it yielded. Since
    cmd_crawl/cmd_scrape/cmd_index never got a handle back to call
    __aexit__ on, the underlying aiohttp ClientSession/TCPConnector was
    never closed on shutdown -- surfacing as "Unclosed client session" /
    "Unclosed connector" warnings from aiohttp at process exit. Entering
    and exiting the client needs no real network I/O (aiobotocore sets
    up/tears down local state lazily, only touching the network on the
    first actual request), so this runs as a plain unit test against
    whatever placeholder endpoint is configured -- no MinIO required."""
    store, store_cm = await _blob_store()
    assert store is not None
    # Must not raise -- this is the exact call cmd_crawl/cmd_scrape/
    # cmd_index now make on their shutdown paths.
    await store_cm.__aexit__(None, None, None)


class _FlakySearchClient:
    """create_index() fails twice with the real Meilisearch SDK's
    communication error, then succeeds -- reproducing Meilisearch simply
    not being reachable yet at indexer startup (a normal docker-compose
    race, or a transient outage)."""

    def __init__(self, fail_times: int):
        self.fail_times = fail_times
        self.calls = 0

    async def create_index(self, name, primary_key):
        from meilisearch_python_sdk.errors import MeilisearchCommunicationError

        self.calls += 1
        if self.calls <= self.fail_times:
            raise MeilisearchCommunicationError("connection refused")
        index = AsyncMock()
        index.update_settings = AsyncMock()
        return index


async def test_create_index_with_retry_recovers_after_meilisearch_comes_up():
    """cmd_index used to call search.create_index()/index.update_settings()
    unguarded, before Indexer.run() -- whose own per-batch loop already
    tolerates Meilisearch failures -- ever started. Meilisearch being down
    at that exact moment (confirmed live: a real MeilisearchCommunicationError
    from a stopped Meilisearch container) crashed the whole indexer process
    immediately, contradicting "the crawl must never block on Meilisearch
    being slow or down". Must retry with backoff instead."""
    search = _FlakySearchClient(fail_times=2)
    stop = asyncio.Event()

    index = await _create_index_with_retry(search, stop, delay=0.01)

    assert index is not None
    assert search.calls == 3  # failed twice, succeeded on the third attempt


async def test_create_index_with_retry_stops_promptly_on_shutdown():
    """If shutdown is requested while still waiting for Meilisearch, the
    retry loop must return None promptly rather than retrying forever --
    Ctrl+C during an outage must still work."""
    search = _FlakySearchClient(fail_times=10_000)  # never succeeds
    stop = asyncio.Event()

    async def stop_soon():
        await asyncio.sleep(0.05)
        stop.set()

    asyncio.create_task(stop_soon())
    index = await _create_index_with_retry(search, stop, delay=5.0)

    assert index is None
