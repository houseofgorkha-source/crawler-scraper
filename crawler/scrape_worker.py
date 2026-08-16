"""
The scrape worker loop -- structurally parallel to CrawlWorker, but a
distinct component with its own claim function and completion path:

    claim -> robots recheck -> fetch (static, or per-spec render policy)
          -> structured extraction -> optional raw-HTML snapshot
          -> scraped_records commit -> optional feed-forward to the crawl
             frontier (only if the spec asks for it)

Scraper is a peer of Crawler, not a mode of it: it shares HttpFetcher,
PlaywrightRenderer, policy.check_allowed(), and BlobStore unchanged, but
never touches `urls`/`documents` unless a spec explicitly opts into
feeding the crawl frontier.
"""
from __future__ import annotations

import asyncio
import dataclasses
import logging
import uuid

from .contracts import FetchOutcome, FetchResult
from .fetch import HttpFetcher, needs_render
from .metrics import FETCH_DURATION_SECONDS, SCRAPE_TASKS
from .scrape_extract import HtmlRecordExtractor, ScrapeSpec

log = logging.getLogger(__name__)

BATCH_SIZE = 20
LEASE_SECONDS = 300
IDLE_SLEEP = 2.0
MAX_FAILURES = 5


class ScrapeWorker:
    def __init__(self, frontier, store, fetcher=None, renderer=None,
                 extractor=None, worker_id: str | None = None):
        self.frontier = frontier
        self.store = store
        self.fetcher = fetcher or HttpFetcher()
        self.renderer = renderer
        self.extractor = extractor or HtmlRecordExtractor()
        self.worker_id = worker_id or f"scraper-{uuid.uuid4().hex[:8]}"
        self._running = False

    async def run(self) -> None:
        self._running = True
        log.info("scrape worker %s started", self.worker_id)
        while self._running:
            tasks = await self.frontier.claim_scrape(self.worker_id, BATCH_SIZE, LEASE_SECONDS)
            if not tasks:
                await asyncio.sleep(IDLE_SLEEP)
                continue
            results = await asyncio.gather(*(self._handle(t) for t in tasks),
                                           return_exceptions=True)
            for task, result in zip(tasks, results):
                if isinstance(result, Exception):
                    log.error("unhandled error processing %s", task.url,
                             exc_info=result)
                    SCRAPE_TASKS.labels(outcome="unhandled_error").inc()

    def stop(self) -> None:
        self._running = False

    async def _handle(self, task) -> None:
        policy = await self.frontier.policy_for(task.host, task.url)
        if policy.is_stale:
            policy = await self.frontier.refresh_robots(task.host, task.url)
        if not policy.check_allowed(task.url):
            await self.frontier.skip_scrape(task, reason="robots_denied")
            SCRAPE_TASKS.labels(outcome="robots_denied").inc()
            return

        spec = await self.frontier.get_scrape_spec(task.spec_id)
        if spec is None:
            await self.frontier.skip_scrape(task, reason="spec_missing")
            SCRAPE_TASKS.labels(outcome="spec_missing").inc()
            return
        # Re-checked here, not just at claim time, for the same reason
        # robots is re-checked at fetch time even though the claim query
        # already gated on it: the claim and this point can straddle a
        # spec being deactivated in between.
        if not spec.is_active:
            await self.frontier.skip_scrape(task, reason="spec_inactive")
            SCRAPE_TASKS.labels(outcome="spec_inactive").inc()
            return

        if spec.render_mode == "always" and self.renderer is None:
            # Never silently substitute a different render mode than the
            # spec asked for -- skip explicitly instead of downgrading to
            # a static fetch that would look like a normal success.
            await self.frontier.skip_scrape(task, reason="render_required_unavailable")
            SCRAPE_TASKS.labels(outcome="render_required_unavailable").inc()
            return

        result = await self._fetch(task, spec)
        FETCH_DURATION_SECONDS.labels(
            subsystem="scraper", render_mode=result.render_mode.value
        ).observe(result.duration_ms / 1000)

        if result.outcome is FetchOutcome.NOT_MODIFIED:
            await self.frontier.reschedule_scrape(task)
            SCRAPE_TASKS.labels(outcome="not_modified").inc()
            return
        if result.outcome is not FetchOutcome.OK:
            await self.frontier.fail_scrape(task, result, max_failures=MAX_FAILURES)
            SCRAPE_TASKS.labels(outcome=result.outcome.value).inc()
            return

        record = self.extractor.extract(result, spec)
        if record is None:
            await self.frontier.skip_scrape(task, reason="no_content")
            SCRAPE_TASKS.labels(outcome="no_content").inc()
            return

        # Same reasoning as CrawlWorker: a failed blob write must not be
        # silently ignored (complete_scrape() would commit a record whose
        # raw_key points at content that was never stored) or left as an
        # uncaught exception (which skips fail_scrape()'s backoff/
        # MAX_FAILURES entirely, retrying at a fixed cadence forever).
        raw_key = None
        if result.body:
            try:
                raw_key = await self.store.put_raw(task.host, task.target_id, result.body)
            except Exception:
                log.exception("blob store write failed for %s", task.url)
                await self.frontier.fail_scrape(
                    task, dataclasses.replace(result, outcome=FetchOutcome.STORAGE_ERROR),
                    max_failures=MAX_FAILURES)
                SCRAPE_TASKS.labels(outcome="storage_error").inc()
                return

        await self.frontier.complete_scrape(task, result, record, raw_key=raw_key)
        SCRAPE_TASKS.labels(outcome="done").inc()

        if spec.feed_to_crawler and record.links:
            await self.frontier.feed_links_to_crawler(task.url, record.links)

    async def _fetch(self, task, spec: ScrapeSpec) -> FetchResult:
        # "always" with no renderer configured is handled by the caller
        # (skipped explicitly, never silently downgraded) before this is
        # ever reached, so reaching here with render_mode == "always"
        # guarantees self.renderer is not None.
        if spec.render_mode == "always":
            return await self.renderer.render(task)
        if spec.render_mode == "never" or self.renderer is None:
            return await self.fetcher.fetch(task)

        # "auto": same static-first, escalate-on-evidence heuristic the
        # Crawler uses, reused as-is rather than reinvented. Doesn't
        # promise rendering the way "always" does, so falling back to
        # static when no renderer is configured is correct here, not a
        # silent downgrade.
        result = await self.fetcher.fetch(task)
        if result.has_body and needs_render(result.body):
            rendered = await self.renderer.render(task)
            if rendered.has_body:
                return rendered
        return result
