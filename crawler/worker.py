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
from .render import PlaywrightRenderer

log = logging.getLogger(__name__)

BATCH_SIZE = 20
LEASE_SECONDS = 300
IDLE_SLEEP = 2.0
MAX_FAILURES = 5           # then the URL is retired
MAX_DEPTH = 6


class CrawlWorker:
    def __init__(self, frontier, store, fetcher=None, renderer=None,
                 extractor=None, worker_id: str | None = None):
        self.frontier = frontier
        self.store = store
        self.fetcher = fetcher or HttpFetcher()
        self.renderer = renderer
        self.extractor = extractor or HtmlExtractor()
        self.worker_id = worker_id or f"worker-{uuid.uuid4().hex[:8]}"
        self._running = False

    async def run(self) -> None:
        self._running = True
        log.info("worker %s started", self.worker_id)
        while self._running:
            tasks = await self.frontier.claim(self.worker_id, BATCH_SIZE, LEASE_SECONDS)
            if not tasks:
                await asyncio.sleep(IDLE_SLEEP)
                continue
            await asyncio.gather(*(self._handle(t) for t in tasks),
                                 return_exceptions=True)

    def stop(self) -> None:
        self._running = False

    async def _handle(self, task) -> None:
        # Second robots enforcement point. The claim query already gated on
        # domain policy, but that read may be minutes stale.
        policy = await self.frontier.policy_for(task.host)
        if policy.is_stale:
            policy = await self.frontier.refresh_robots(task.host)
        if not policy.check_allowed(task.url):
            await self.frontier.skip(task, reason="robots_denied")
            return

        result = await self._fetch_with_escalation(task)
        await self.frontier.record_attempt(result, self.worker_id)

        if result.outcome is FetchOutcome.NOT_MODIFIED:
            await self.frontier.reschedule(task, unchanged=True)
            return
        if result.outcome is not FetchOutcome.OK:
            await self.frontier.fail(result, max_failures=MAX_FAILURES)
            return

        doc = self.extractor.extract(result)
        if doc is None:
            await self.frontier.skip(task, reason="no_content")
            return

        # Blob first, then the row that points at it.
        raw_key = await self.store.put_raw(task.host, task.url_id, result.body)
        text_key = await self.store.put_text(task.host, task.url_id, doc.text)

        await self.frontier.complete(result, doc, raw_key=raw_key, text_key=text_key)

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
