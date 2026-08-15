"""
PlaywrightRenderer's cross-process advisory-lock mechanism, tested
directly at the _acquire_slot()/_release_slot() level -- no Playwright/
Chromium involved, so this runs in CI without needing browser binaries.
This is the same mechanism manually verified live earlier in this
project (two processes, pg_locks polling); this formalizes it so it
keeps being checked automatically.
"""
import asyncio

import asyncpg

from crawler.render import PlaywrightRenderer
from tests.conftest import TEST_PG_DSN


async def _renderer_with_pool(max_pages: int) -> PlaywrightRenderer:
    r = PlaywrightRenderer(TEST_PG_DSN, max_pages=max_pages)
    r._lock_pool = await asyncpg.create_pool(TEST_PG_DSN, min_size=max_pages, max_size=max_pages)
    return r


async def test_acquire_then_release_round_trips():
    r = await _renderer_with_pool(max_pages=2)
    conn, key = await r._acquire_slot()
    assert key in r._lock_keys
    await r._release_slot(conn, key)
    await r._lock_pool.close()


async def test_slot_is_reusable_after_release():
    r = await _renderer_with_pool(max_pages=1)
    conn1, key1 = await r._acquire_slot()
    await r._release_slot(conn1, key1)
    # must not hang: the only slot is free again
    conn2, key2 = await asyncio.wait_for(r._acquire_slot(), timeout=2)
    assert key2 == key1
    await r._release_slot(conn2, key2)
    await r._lock_pool.close()


async def test_two_holders_never_exceed_max_pages():
    """The actual cap: with max_pages=1, a second acquire must block until
    the first releases -- this is what makes it a genuine cross-process
    limit rather than decoration."""
    r1 = await _renderer_with_pool(max_pages=1)
    r2 = await _renderer_with_pool(max_pages=1)

    conn1, key1 = await r1._acquire_slot()

    second_acquired = asyncio.Event()

    async def try_acquire_r2():
        conn2, key2 = await r2._acquire_slot()
        second_acquired.set()
        await r2._release_slot(conn2, key2)

    task = asyncio.create_task(try_acquire_r2())
    await asyncio.sleep(0.3)
    assert not second_acquired.is_set()  # still blocked -- r1 holds the only slot

    await r1._release_slot(conn1, key1)
    await asyncio.wait_for(task, timeout=2)
    assert second_acquired.is_set()

    await r1._lock_pool.close()
    await r2._lock_pool.close()


async def test_crashed_holder_releases_lock_automatically():
    """A held advisory lock is tied to the connection, not to any
    explicit release call -- if the holder's connection drops (a crash),
    Postgres frees the lock on its own. No TTL, no reaper needed.

    Uses a raw connection + .terminate() rather than a pool: pool.close()
    waits for checked-out connections to be returned first, which would
    just be testing graceful shutdown, not a crash.
    """
    r = await _renderer_with_pool(max_pages=1)
    key = r._lock_keys[0]

    crashing_conn = await asyncpg.connect(TEST_PG_DSN)
    got = await crashing_conn.fetchval("SELECT pg_try_advisory_lock($1)", key)
    assert got is True

    crashing_conn.terminate()  # simulated crash: no graceful close, no unlock call

    conn, acquired_key = await asyncio.wait_for(r._acquire_slot(), timeout=2)
    assert acquired_key == key
    await r._release_slot(conn, acquired_key)
    await r._lock_pool.close()
