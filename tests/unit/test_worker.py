"""
CrawlWorker._handle() orchestration tests -- frontier/fetcher/renderer/
extractor/store are all mocked, so these verify control flow (which
branch runs, what gets called with what) without any live infra.
"""
from unittest.mock import AsyncMock, MagicMock

import pytest

from crawler.contracts import (
    CrawlTask, DiscoveredLink, ExtractedDoc, FetchOutcome, FetchResult, RenderMode,
)
from crawler.policy import parse_robots
from crawler.worker import MAX_DEPTH, CrawlWorker

ALLOW_ALL = parse_robots("example.com", None, 404)   # 404 -> crawlable, no restrictions
DENY_ALL = parse_robots("example.com", None, 403)    # 403 -> whole host disallowed


def _task(depth=0, js_required=False):
    return CrawlTask(url_id=1, url="https://example.com/p", host="example.com",
                     depth=depth, js_required=js_required)


def _fetch_ok(body=b"<html><body><main><p>content</p></main></body></html>"):
    return FetchResult(task=_task(), outcome=FetchOutcome.OK, status_code=200,
                       final_url="https://example.com/p", body=body,
                       render_mode=RenderMode.STATIC, duration_ms=10)


def _doc(links=()):
    return ExtractedDoc(
        url_id=1, canonical_url="https://example.com/p", title="T", description=None,
        text="content", lang="en", word_count=1, content_sha256=b"\x00" * 32,
        simhash=1, render_mode=RenderMode.STATIC, links=list(links),
    )


def _worker(feed_scraper=False, renderer=None):
    frontier = AsyncMock()
    frontier.policy_for.return_value = ALLOW_ALL
    fetcher = AsyncMock()
    fetcher.fetch.return_value = _fetch_ok()
    extractor = MagicMock()
    extractor.extract.return_value = _doc()
    store = AsyncMock()
    store.put_raw.return_value = "raw/k"
    store.put_text.return_value = "text/k"
    worker = CrawlWorker(frontier, store, fetcher=fetcher, renderer=renderer,
                         extractor=extractor, worker_id="w0", feed_scraper=feed_scraper)
    return worker, frontier, fetcher, extractor, store


async def test_robots_denied_skips_before_any_fetch():
    worker, frontier, fetcher, extractor, store = _worker()
    frontier.policy_for.return_value = DENY_ALL

    await worker._handle(_task())

    frontier.skip.assert_awaited_once_with(_task(), reason="robots_denied")
    fetcher.fetch.assert_not_awaited()


async def test_stale_policy_triggers_refresh():
    worker, frontier, fetcher, extractor, store = _worker()
    stale = parse_robots("example.com", None, 404)
    stale.fetched_at = None  # force is_stale True
    frontier.policy_for.return_value = stale
    frontier.refresh_robots.return_value = ALLOW_ALL

    await worker._handle(_task())

    frontier.refresh_robots.assert_awaited_once_with("example.com", "https://example.com/p")


async def test_not_modified_reschedules_without_extraction():
    worker, frontier, fetcher, extractor, store = _worker()
    fetcher.fetch.return_value = FetchResult(
        task=_task(), outcome=FetchOutcome.NOT_MODIFIED, status_code=304,
    )

    await worker._handle(_task())

    frontier.reschedule.assert_awaited_once()
    extractor.extract.assert_not_called()
    frontier.complete.assert_not_awaited()


async def test_fetch_error_fails_without_extraction():
    worker, frontier, fetcher, extractor, store = _worker()
    fetcher.fetch.return_value = FetchResult(
        task=_task(), outcome=FetchOutcome.HTTP_ERROR, status_code=500,
    )

    await worker._handle(_task())

    frontier.fail.assert_awaited_once()
    extractor.extract.assert_not_called()


async def test_no_content_skips():
    worker, frontier, fetcher, extractor, store = _worker()
    extractor.extract.return_value = None

    await worker._handle(_task())

    frontier.skip.assert_awaited_once_with(_task(), reason="no_content")
    frontier.complete.assert_not_awaited()


async def test_success_writes_blob_before_completing():
    worker, frontier, fetcher, extractor, store = _worker()

    await worker._handle(_task())

    store.put_raw.assert_awaited_once()
    store.put_text.assert_awaited_once()
    frontier.complete.assert_awaited_once()
    # raw_key/text_key from the store calls flow into complete()
    _, kwargs = frontier.complete.call_args
    assert kwargs["raw_key"] == "raw/k"
    assert kwargs["text_key"] == "text/k"


async def test_blob_store_failure_routes_through_fail_not_uncaught():
    """store.put_raw()/put_text() used to be uncaught: an exception (e.g.
    MinIO down) propagated straight out of _handle(), skipping
    frontier.fail() entirely, so the lease just sat until the reap
    timeout and retried at a fixed, unbackoff'd cadence forever -- wasting
    a real fetch against the target site on every retry, with no
    MAX_FAILURES retirement. It must be caught and routed through the same
    fail()/backoff path as any other fetch failure, not left to complete()
    (which would otherwise commit a row pointing at content that was
    never actually stored)."""
    worker, frontier, fetcher, extractor, store = _worker()
    store.put_raw.side_effect = ConnectionError("storage unreachable")

    await worker._handle(_task())

    frontier.fail.assert_awaited_once()
    frontier.complete.assert_not_awaited()
    (result,), kwargs = frontier.fail.call_args
    assert result.outcome is FetchOutcome.STORAGE_ERROR
    assert result.task.url_id == 1


async def test_discovered_links_added_at_depth_plus_one():
    worker, frontier, fetcher, extractor, store = _worker()
    links = [DiscoveredLink(url="https://example.com/other")]
    extractor.extract.return_value = _doc(links=links)

    await worker._handle(_task(depth=2))

    frontier.add.assert_awaited_once_with(links, from_url_id=1, depth=3)


async def test_max_depth_does_not_enqueue_links():
    worker, frontier, fetcher, extractor, store = _worker()
    links = [DiscoveredLink(url="https://example.com/other")]
    extractor.extract.return_value = _doc(links=links)

    await worker._handle(_task(depth=MAX_DEPTH))

    frontier.add.assert_not_awaited()


async def test_feed_scraper_off_by_default_does_not_enroll():
    worker, frontier, fetcher, extractor, store = _worker(feed_scraper=False)

    await worker._handle(_task())

    frontier.enroll_scrape_targets.assert_not_awaited()


async def test_feed_scraper_on_enrolls_after_complete():
    worker, frontier, fetcher, extractor, store = _worker(feed_scraper=True)

    await worker._handle(_task())

    frontier.enroll_scrape_targets.assert_awaited_once_with("https://example.com/p", "example.com")


async def test_feed_scraper_failure_does_not_break_completion():
    """Best-effort: enrollment failing must never break the Crawler's own
    completion (same principle as indexing never blocking the crawl)."""
    worker, frontier, fetcher, extractor, store = _worker(feed_scraper=True)
    frontier.enroll_scrape_targets.side_effect = RuntimeError("scraper db down")

    await worker._handle(_task())  # must not raise

    frontier.complete.assert_awaited_once()


async def test_js_required_skips_static_fetch_entirely():
    renderer = AsyncMock()
    renderer.render.return_value = _fetch_ok()
    worker, frontier, fetcher, extractor, store = _worker(renderer=renderer)

    await worker._handle(_task(js_required=True))

    renderer.render.assert_awaited_once()
    fetcher.fetch.assert_not_awaited()


async def test_needs_render_escalates_and_marks_js_required():
    renderer = AsyncMock()
    renderer.render.return_value = _fetch_ok(
        body=b"<html><body><main>" + b"word " * 200 + b"</main></body></html>"
    )
    worker, frontier, fetcher, extractor, store = _worker(renderer=renderer)
    fetcher.fetch.return_value = _fetch_ok(body=b'<html><body><div id="root"></div></body></html>')

    await worker._handle(_task())

    renderer.render.assert_awaited_once()
    frontier.mark_js_required.assert_awaited_once_with("example.com")
