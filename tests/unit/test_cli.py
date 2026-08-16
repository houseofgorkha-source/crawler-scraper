"""
CLI helper functions: _fetch_robots() -- the robots_fetcher wired into
PostgresFrontier in production, covering the port-loss regression (a URL
on a non-standard port must resolve robots.txt against that same port,
not a default-port guess) -- and _blob_store()'s resource lifecycle.
"""
from crawler.cli import _blob_store, _fetch_robots
from crawler.normalize import robots_origin


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
