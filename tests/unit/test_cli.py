"""
_fetch_robots() -- the robots_fetcher wired into PostgresFrontier in
production. Covers the port-loss regression: a URL on a non-standard port
must resolve robots.txt against that same port, not a default-port guess.
"""
from crawler.cli import _fetch_robots
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
