"""
Tier 2: headless rendering. Deliberately scarce.

Design constraints:
  * ONE browser process, N contexts. Not N browsers.
  * A hard semaphore caps concurrent pages -- this pool is the system's
    bottleneck by design, and an unbounded one will OOM the box.
  * Contexts are destroyed after each page: no cookie/state bleed between
    unrelated hosts.
  * Images/fonts/media are aborted at the network layer. We want the DOM,
    not the pixels; this alone roughly halves render time.
"""
from __future__ import annotations

import asyncio
import time

from .contracts import CrawlTask, FetchOutcome, FetchResult, RenderMode
from .policy import USER_AGENT

BLOCKED_RESOURCES = {"image", "media", "font", "stylesheet"}
RENDER_TIMEOUT_MS = 20_000
# Beyond ~4 concurrent pages a single machine starts thrashing. Raise only
# with measured headroom.
MAX_CONCURRENT_PAGES = 4


class PlaywrightRenderer:
    def __init__(self, max_pages: int = MAX_CONCURRENT_PAGES):
        self._sem = asyncio.Semaphore(max_pages)
        self._browser = None
        self._pw = None

    async def start(self) -> None:
        from playwright.async_api import async_playwright
        self._pw = await async_playwright().start()
        self._browser = await self._pw.chromium.launch(
            args=["--no-sandbox", "--disable-dev-shm-usage", "--disable-gpu"],
        )

    async def render(self, task: CrawlTask) -> FetchResult:
        assert self._browser is not None, "call start() first"
        started = time.perf_counter()

        async with self._sem:
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

    async def aclose(self) -> None:
        if self._browser:
            await self._browser.close()
        if self._pw:
            await self._pw.stop()
