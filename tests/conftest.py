"""
Shared test fixtures.

Integration tests run against a DEDICATED database (crawler_test), never
the dev database -- this repo's history includes multiple incidents where
ad-hoc test scripts pointed at the real dev DSN and claimed real frontier
work. CRAWLER_TEST_PG_DSN defaults to a distinct database name specifically
so that mistake is structurally harder to repeat.
"""
from __future__ import annotations

import functools
import os
import threading
from http.server import SimpleHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from http import HTTPStatus

import asyncpg
import pytest
import pytest_asyncio
import redis.asyncio as aioredis

from crawler.frontier import PostgresFrontier

TEST_PG_DSN = os.environ.get(
    "CRAWLER_TEST_PG_DSN", "postgresql://postgres:crawler@localhost/crawler_test"
)
TEST_REDIS_URL = os.environ.get("CRAWLER_TEST_REDIS_URL", "redis://localhost:6379/15")

FIXTURES_DIR = Path(__file__).parent / "fixtures"


async def allow_all_robots(host, origin=None):
    return None, 404  # 404 -> no restrictions, crawlable

_TABLES = (
    "crawl_attempts", "links", "documents", "scraped_records",
    "scrape_targets", "scrape_specs", "urls", "domains",
)


@pytest_asyncio.fixture
async def db():
    """A clean database for a single test: fresh pool (avoids asyncpg
    connections crossing pytest-asyncio's per-test event loops), tables
    truncated first."""
    pool = await asyncpg.create_pool(TEST_PG_DSN, min_size=1, max_size=8)
    async with pool.acquire() as conn:
        await conn.execute(f"TRUNCATE {', '.join(_TABLES)} RESTART IDENTITY CASCADE")
    yield pool
    await pool.close()


@pytest.fixture
def anyio_backend():
    return "asyncio"


@pytest_asyncio.fixture
async def frontier(db):
    """A real PostgresFrontier wired to the isolated test db + an
    isolated Redis logical db, robots pre-approved so tests don't need
    to fake HTTP robots.txt fetches too."""
    redis = aioredis.from_url(TEST_REDIS_URL)
    await redis.flushdb()
    yield PostgresFrontier(db, redis, robots_fetcher=allow_all_robots)
    await redis.aclose()


@pytest_asyncio.fixture
async def renderer():
    from crawler.render import PlaywrightRenderer

    renderer = PlaywrightRenderer(TEST_PG_DSN)
    await renderer.start()
    try:
        yield renderer
    finally:
        await renderer.aclose()



class LabFixtureHandler(SimpleHTTPRequestHandler):
    """Static fixture server plus deterministic HTTP obstacle responses."""

    def do_GET(self):
        routes = {
            "/lab/403": (
                HTTPStatus.FORBIDDEN,
                b"<html><body><h1>Access Denied</h1><p>WAF lab fixture</p></body></html>",
                {"Content-Type": "text/html"},
            ),
            "/lab/429": (
                HTTPStatus.TOO_MANY_REQUESTS,
                b"<html><body><h1>Rate Limited</h1></body></html>",
                {"Content-Type": "text/html", "Retry-After": "1"},
            ),
            "/lab/js-challenge": (
                HTTPStatus.OK,
                b"""<html><body>
                    <h1>Just a moment...</h1>
                    <p>Checking your browser before accessing this site.</p>
                    <script>
                        setTimeout(() => {
                            document.body.innerHTML =
                                '<h1>JS Challenge Resolved</h1>' +
                                '<p>Authorized browser access granted.</p>';
                        }, 100);
                    </script>
                </body></html>""",
                {"Content-Type": "text/html"},
            ),
           "/lab/captcha": (
                HTTPStatus.OK,
                b"""<html><body>
                    <h1 id="challenge">Human verification</h1>
                    <div class="g-recaptcha">CAPTCHA</div>
                    <p>Please verify you are human.</p>
                    <script>
                        setTimeout(() => {
                            document.body.innerHTML =
                                '<h1>CAPTCHA Resolved</h1>' +
                                '<p>Authorized browser access granted.</p>';
                        }, 50);
                    </script>
                </body></html>""",
                {"Content-Type": "text/html"},
            ),
            "/lab/auth": (
                HTTPStatus.UNAUTHORIZED,
                b"""<html><body>
                    <h1>Authentication Required</h1>
                    <p>Please sign in to continue.</p>
                </body></html>""",
                {"Content-Type": "text/html", "WWW-Authenticate": 'Basic realm="lab"'},
            ),
        }

        route = routes.get(self.path)
        if route is None:
            return super().do_GET()

        status, body, headers = route

        self.send_response(status)
        for name, value in headers.items():
            self.send_header(name, value)
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)


@pytest.fixture
def fixture_server():
    """In-process static file server plus deterministic lab obstacle routes."""
    handler = functools.partial(LabFixtureHandler, directory=str(FIXTURES_DIR))
    server = ThreadingHTTPServer(("127.0.0.1", 0), handler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        yield f"http://127.0.0.1:{server.server_port}"
    finally:
        server.shutdown()
        thread.join(timeout=5)