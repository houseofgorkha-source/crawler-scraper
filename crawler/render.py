"""
Tier 2: headless rendering. Deliberately scarce.

Design constraints:
  * ONE browser process, N contexts. Not N browsers.
  * A hard cap limits concurrent pages -- this pool is the system's
    bottleneck by design, and an unbounded one will OOM the box.
  * Contexts are destroyed after each page: no cookie/state bleed between
    unrelated hosts.
  * Images/fonts/media are aborted at the network layer. We want the DOM,
    not the pixels; this alone roughly halves render time.

Crawler and Scraper are independently runnable processes, each capable of
holding their own PlaywrightRenderer, so MAX_CONCURRENT_PAGES has to be a
genuinely cross-process limit, not an in-process asyncio.Semaphore (which
only ever bounded one process's pages, and silently doubles real
concurrency the moment a second process runs one too). The smallest
mechanism that gives that without a new datastore: a fixed set of
Postgres session-scoped advisory locks, one per page slot. pg_try_advisory_
lock is non-blocking (a real blocking pg_advisory_lock would tie up both
the connection and the coroutine), and a crashed holder's slot is released
automatically when its connection drops -- no TTL, no heartbeat, no reaper
needed, unlike an equivalent Redis-semaphore approach would require.
"""
from __future__ import annotations

import asyncio
import time

import asyncpg

from .contracts import CrawlTask, FetchOutcome, FetchResult, RenderMode, ScrapeTask
from .policy import USER_AGENT

BLOCKED_RESOURCES = {"image", "media", "font", "stylesheet"}
RENDER_TIMEOUT_MS = 20_000
# Beyond ~4 concurrent pages a single machine starts thrashing. Raise only
# with measured headroom.
MAX_CONCURRENT_PAGES = 4
# Namespaced base so these keys don't collide with any other advisory lock
# use in this database. Slot i uses key RENDER_LOCK_BASE_KEY + i.
RENDER_LOCK_BASE_KEY = 0x5A9C4E00
LOCK_POLL_INTERVAL_S = 0.1


class PlaywrightRenderer:
    def __init__(self, pg_dsn: str, max_pages: int = MAX_CONCURRENT_PAGES):
        self._pg_dsn = pg_dsn
        self._lock_keys = [RENDER_LOCK_BASE_KEY + i for i in range(max_pages)]
        self._lock_pool: asyncpg.Pool | None = None
        self._browser = None
        self._pw = None

    async def start(self) -> None:
        from playwright.async_api import async_playwright
        # Dedicated pool, sized exactly to the number of slots, so render
        # locks can never starve the frontier's own connections and vice
        # versa.
        self._lock_pool = await asyncpg.create_pool(
            self._pg_dsn, min_size=len(self._lock_keys), max_size=len(self._lock_keys)
        )
        self._pw = await async_playwright().start()
        self._browser = await self._pw.chromium.launch(
            args=["--no-sandbox", "--disable-dev-shm-usage", "--disable-gpu"],
        )

    async def _acquire_slot(self) -> tuple[asyncpg.Connection, int]:
        while True:
            for key in self._lock_keys:
                conn = await self._lock_pool.acquire()
                got = await conn.fetchval("SELECT pg_try_advisory_lock($1)", key)
                if got:
                    return conn, key
                await self._lock_pool.release(conn)
            await asyncio.sleep(LOCK_POLL_INTERVAL_S)

    async def _release_slot(self, conn: asyncpg.Connection, key: int) -> None:
        try:
            await conn.execute("SELECT pg_advisory_unlock($1)", key)
        finally:
            await self._lock_pool.release(conn)

    async def render(self, task: CrawlTask | ScrapeTask) -> FetchResult:
        assert self._browser is not None, "call start() first"
        started = time.perf_counter()

        lock_conn, lock_key = await self._acquire_slot()
        try:
            context = await self._browser.new_context(
                user_agent=USER_AGENT,
                java_script_enabled=True,
                ignore_https_errors=False,
            )
            try:
                await context.route(
                    "**/*",
                    lambda route: asyncio.ensure_future(
                        route.abort()
                        if route.request.resource_type in BLOCKED_RESOURCES
                        else route.continue_()
                    ),
                )
                page = await context.new_page()
                resp = await page.goto(
                    task.url, wait_until="networkidle", timeout=RENDER_TIMEOUT_MS
                )
                html = await page.content()
                final_url = page.url
                status = resp.status if resp else None

                return FetchResult(
                    task=task,
                    outcome=FetchOutcome.OK,
                    status_code=status,
                    final_url=final_url,
                    body=html.encode("utf-8"),
                    content_type="text/html",
                    encoding="utf-8",
                    render_mode=RenderMode.RENDERED,
                    duration_ms=int((time.perf_counter() - started) * 1000),
                )
            except Exception as exc:
                return FetchResult(
                    task, FetchOutcome.NETWORK_ERROR,
                    render_mode=RenderMode.RENDERED,
                    duration_ms=int((time.perf_counter() - started) * 1000),
                    error_class=type(exc).__name__,
                    error_detail=str(exc)[:500],
                )
            finally:
                await context.close()
        finally:
            await self._release_slot(lock_conn, lock_key)

    async def aclose(self) -> None:
        if self._browser:
            await self._browser.close()
        if self._pw:
            await self._pw.stop()
        if self._lock_pool:
            await self._lock_pool.close()
