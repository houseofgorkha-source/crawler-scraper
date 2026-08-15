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


async def allow_all_robots(host):
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


@pytest.fixture
def fixture_server():
    """In-process static file server for tests/fixtures/, bound to an
    OS-assigned free port -- no manual start/stop, no port collisions,
    replacing the manual scratchpad http.server dance used throughout
    this project's earlier live verification."""
    handler = functools.partial(SimpleHTTPRequestHandler, directory=str(FIXTURES_DIR))
    server = ThreadingHTTPServer(("127.0.0.1", 0), handler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        yield f"http://127.0.0.1:{server.server_port}"
    finally:
        server.shutdown()
        thread.join(timeout=5)
