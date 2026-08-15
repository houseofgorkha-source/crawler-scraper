"""
The crawl worker loop.

Order of operations matters and is not arbitrary:

  claim -> robots recheck -> static fetch -> escalate? -> extract
        -> blob store -> postgres commit -> enqueue links -> mark index pending

The blob is written BEFORE the Postgres commit. If we crash between them we
leak an orphan object (cheap, sweepable). The reverse order would leave a
committed row pointing at content that does not exist -- an unrecoverable
inconsistency.

Indexing is never done inline. It is a flag on the row, drained by a
separate process. Search being down must not stop the crawl.
"""
from __future__ import annotations

import asyncio
import logging
import uuid

from .contracts import FetchOutcome, FetchResult
from .extract import HtmlExtractor
from .fetch import HttpFetcher, needs_render
from .metrics import CRAWL_TASKS, FETCH_DURATION_SECONDS
from .render import PlaywrightRenderer

log = logging.getLogger(__name__)

BATCH_SIZE = 20
LEASE_SECONDS = 300
IDLE_SLEEP = 2.0
MAX_FAILURES = 5           # then the URL is retired
MAX_DEPTH = 6


class CrawlWorker:
    def __init__(self, frontier, store, fetcher=None, renderer=None,
                 extractor=None, worker_id: str | None = None,
                 feed_scraper: bool = False):
        self.frontier = frontier
        self.store = store
        self.fetcher = fetcher or HttpFetcher()
        self.renderer = renderer
        self.extractor = extractor or HtmlExtractor()
        self.worker_id = worker_id or f"worker-{uuid.uuid4().hex[:8]}"
        # Crawler -> Scraper feed, opt-in only (--feed-scraper). Off by
        # default, so plain `crawl` behaves exactly as before.
        self.feed_scraper = feed_scraper
        self._running = False

    async def run(self) -> None:
        self._running = True
        log.info("worker %s started", self.worker_id)
        while self._running:
            tasks = await self.frontier.claim(self.worker_id, BATCH_SIZE, LEASE_SECONDS)
            if not tasks:
                await asyncio.sleep(IDLE_SLEEP)
                continue
            results = await asyncio.gather(*(self._handle(t) for t in tasks),
                                           return_exceptions=True)
            for task, result in zip(tasks, results):
                if isinstance(result, Exception):
                    log.error("unhandled error processing %s", task.url,
                             exc_info=result)
                    CRAWL_TASKS.labels(outcome="unhandled_error").inc()

    def stop(self) -> None:
        self._running = False

    async def _handle(self, task) -> None:
        # Second robots enforcement point. The claim query already gated on
        # domain policy, but that read may be minutes stale.
        policy = await self.frontier.policy_for(task.host, task.url)
        if policy.is_stale:
            policy = await self.frontier.refresh_robots(task.host, task.url)
        if not policy.check_allowed(task.url):
            await self.frontier.skip(task, reason="robots_denied")
            CRAWL_TASKS.labels(outcome="robots_denied").inc()
            return

        result = await self._fetch_with_escalation(task)
        await self.frontier.record_attempt(result, self.worker_id)
        FETCH_DURATION_SECONDS.labels(
            subsystem="crawler", render_mode=result.render_mode.value
        ).observe(result.duration_ms / 1000)

        if result.outcome is FetchOutcome.NOT_MODIFIED:
            await self.frontier.reschedule(task, unchanged=True)
            CRAWL_TASKS.labels(outcome="not_modified").inc()
            return
        if result.outcome is not FetchOutcome.OK:
            await self.frontier.fail(result, max_failures=MAX_FAILURES)
            CRAWL_TASKS.labels(outcome=result.outcome.value).inc()
            return

        doc = self.extractor.extract(result)
        if doc is None:
            await self.frontier.skip(task, reason="no_content")
            CRAWL_TASKS.labels(outcome="no_content").inc()
            return

        # Blob first, then the row that points at it.
        raw_key = await self.store.put_raw(task.host, task.url_id, result.body)
        text_key = await self.store.put_text(task.host, task.url_id, doc.text)

        await self.frontier.complete(result, doc, raw_key=raw_key, text_key=text_key)
        CRAWL_TASKS.labels(outcome="done").inc()

        if self.feed_scraper:
            # Best-effort: Scraper being unreachable/misconfigured must
            # never break the Crawler's own completion, same principle as
            # indexing never blocking the crawl.
            try:
                await self.frontier.enroll_scrape_targets(task.url, task.host)
            except Exception:
                log.exception("scrape enrollment failed for %s", task.url)

        if task.depth < MAX_DEPTH and doc.links:
            n = await self.frontier.add(doc.links, from_url_id=task.url_id,
                                        depth=task.depth + 1)
            log.debug("%s -> %d new urls", task.url, n)

    async def _fetch_with_escalation(self, task) -> FetchResult:
        # Known-SPA host: skip the static attempt entirely, it is pure waste.
        if task.js_required and self.renderer is not None:
            return await self.renderer.render(task)

        result = await self.fetcher.fetch(task)

        if (result.has_body and self.renderer is not None
                and needs_render(result.body)):
            rendered = await self.renderer.render(task)
            if rendered.has_body:
                # Record the evidence so this host stops paying the static
                # round-trip after a few confirmations.
                await self.frontier.mark_js_required(task.host)
                return rendered

        return result
