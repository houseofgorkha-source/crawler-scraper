"""
Shared test fixtures.

Integration tests run against a DEDICATED database (crawler_test), never
the dev database -- this repo's history includes multiple incidents where
ad-hoc test scripts pointed at the real dev DSN and claimed real frontier
work. CRAWLER_TEST_PG_DSN defaults to a distinct database name specifically
so that mistake is structurally harder to repeat.
"""
from __future__ import annotations

import os

import asyncpg
import pytest
import pytest_asyncio

TEST_PG_DSN = os.environ.get(
    "CRAWLER_TEST_PG_DSN", "postgresql://postgres:crawler@localhost/crawler_test"
)

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
