"""
ScrapeWorker._handle() orchestration tests -- same approach as
test_worker.py: mocked frontier/fetcher/renderer/extractor/store,
verifying control flow without live infra.
"""
from unittest.mock import AsyncMock, MagicMock

from crawler.contracts import (
    DiscoveredLink, FetchOutcome, FetchResult, RenderMode, ScrapedRecord, ScrapeTask,
)
from crawler.policy import parse_robots
from crawler.scrape_extract import ScrapeSpec
from crawler.scrape_worker import ScrapeWorker

ALLOW_ALL = parse_robots("example.com", None, 404)
DENY_ALL = parse_robots("example.com", None, 403)


def _task(spec_id=1, render_mode="never"):
    return ScrapeTask(target_id=1, url="https://example.com/list", host="example.com",
                      spec_id=spec_id, render_mode=render_mode)


def _spec(render_mode="never", is_active=True, feed_to_crawler=False):
    return ScrapeSpec(id=1, name="s", version=1, fields=[], render_mode=render_mode,
                      is_active=is_active, feed_to_crawler=feed_to_crawler)


def _fetch_ok():
    return FetchResult(task=_task(), outcome=FetchOutcome.OK, status_code=200,
                       final_url="https://example.com/list", body=b"<html></html>",
                       render_mode=RenderMode.STATIC, duration_ms=10)


def _record(links=()):
    return ScrapedRecord(target_id=1, spec_id=1, data={"x": "y"}, links=list(links))


def _worker(renderer=None):
    frontier = AsyncMock()
    frontier.policy_for.return_value = ALLOW_ALL
    frontier.get_scrape_spec.return_value = _spec()
    fetcher = AsyncMock()
    fetcher.fetch.return_value = _fetch_ok()
    extractor = MagicMock()
    extractor.extract.return_value = _record()
    store = AsyncMock()
    store.put_raw.return_value = "raw/k"
    worker = ScrapeWorker(frontier, store, fetcher=fetcher, renderer=renderer,
                          extractor=extractor, worker_id="s0")
    return worker, frontier, fetcher, extractor, store


async def test_robots_denied_skips_before_any_fetch():
    worker, frontier, fetcher, extractor, store = _worker()
    frontier.policy_for.return_value = DENY_ALL

    await worker._handle(_task())

    frontier.skip_scrape.assert_awaited_once_with(_task(), reason="robots_denied")
    fetcher.fetch.assert_not_awaited()


async def test_missing_spec_skips():
    worker, frontier, fetcher, extractor, store = _worker()
    frontier.get_scrape_spec.return_value = None

    await worker._handle(_task())

    frontier.skip_scrape.assert_awaited_once_with(_task(), reason="spec_missing")
    fetcher.fetch.assert_not_awaited()


async def test_inactive_spec_skips_before_fetch():
    worker, frontier, fetcher, extractor, store = _worker()
    frontier.get_scrape_spec.return_value = _spec(is_active=False)

    await worker._handle(_task())

    frontier.skip_scrape.assert_awaited_once_with(_task(), reason="spec_inactive")
    fetcher.fetch.assert_not_awaited()


async def test_always_render_without_renderer_skips_never_fetches():
    """The fix: render_mode=always with no renderer configured must never
    silently downgrade to a static fetch."""
    worker, frontier, fetcher, extractor, store = _worker(renderer=None)
    frontier.get_scrape_spec.return_value = _spec(render_mode="always")

    await worker._handle(_task(render_mode="always"))

    frontier.skip_scrape.assert_awaited_once_with(
        _task(render_mode="always"), reason="render_required_unavailable"
    )
    fetcher.fetch.assert_not_awaited()


async def test_always_render_with_renderer_never_calls_static_fetch():
    renderer = AsyncMock()
    renderer.render.return_value = _fetch_ok()
    worker, frontier, fetcher, extractor, store = _worker(renderer=renderer)
    frontier.get_scrape_spec.return_value = _spec(render_mode="always")

    await worker._handle(_task(render_mode="always"))

    renderer.render.assert_awaited_once()
    fetcher.fetch.assert_not_awaited()


async def test_never_render_uses_static_fetch_even_with_renderer_available():
    renderer = AsyncMock()
    worker, frontier, fetcher, extractor, store = _worker(renderer=renderer)
    frontier.get_scrape_spec.return_value = _spec(render_mode="never")

    await worker._handle(_task(render_mode="never"))

    fetcher.fetch.assert_awaited_once()
    renderer.render.assert_not_awaited()


async def test_not_modified_reschedules():
    worker, frontier, fetcher, extractor, store = _worker()
    fetcher.fetch.return_value = FetchResult(
        task=_task(), outcome=FetchOutcome.NOT_MODIFIED, status_code=304,
    )

    await worker._handle(_task())

    frontier.reschedule_scrape.assert_awaited_once()
    extractor.extract.assert_not_called()


async def test_fetch_error_fails_without_extraction():
    worker, frontier, fetcher, extractor, store = _worker()
    fetcher.fetch.return_value = FetchResult(
        task=_task(), outcome=FetchOutcome.HTTP_ERROR, status_code=500,
    )

    await worker._handle(_task())

    frontier.fail_scrape.assert_awaited_once()
    extractor.extract.assert_not_called()


async def test_no_record_skips():
    worker, frontier, fetcher, extractor, store = _worker()
    extractor.extract.return_value = None

    await worker._handle(_task())

    frontier.skip_scrape.assert_awaited_once_with(_task(), reason="no_content")
    frontier.complete_scrape.assert_not_awaited()


async def test_success_completes_with_extracted_record():
    worker, frontier, fetcher, extractor, store = _worker()

    await worker._handle(_task())

    store.put_raw.assert_awaited_once()
    frontier.complete_scrape.assert_awaited_once()


async def test_blob_store_failure_routes_through_fail_scrape_not_uncaught():
    """Same reasoning as CrawlWorker's equivalent test: store.put_raw()
    used to be uncaught, skipping fail_scrape()'s backoff/MAX_FAILURES
    path entirely and leaving the lease to retry at a fixed cadence
    forever. It must route through fail_scrape() instead of
    complete_scrape() (which would otherwise commit a record whose
    raw_key points at content that was never stored)."""
    worker, frontier, fetcher, extractor, store = _worker()
    store.put_raw.side_effect = ConnectionError("storage unreachable")

    await worker._handle(_task())

    frontier.fail_scrape.assert_awaited_once()
    frontier.complete_scrape.assert_not_awaited()
    (task, result), kwargs = frontier.fail_scrape.call_args
    assert result.outcome is FetchOutcome.STORAGE_ERROR
    assert task.target_id == 1


async def test_feed_to_crawler_off_does_not_feed():
    worker, frontier, fetcher, extractor, store = _worker()
    frontier.get_scrape_spec.return_value = _spec(feed_to_crawler=False)
    extractor.extract.return_value = _record(
        links=[DiscoveredLink(url="https://example.com/next")]
    )

    await worker._handle(_task())

    frontier.feed_links_to_crawler.assert_not_awaited()


async def test_feed_to_crawler_on_feeds_discovered_links():
    worker, frontier, fetcher, extractor, store = _worker()
    frontier.get_scrape_spec.return_value = _spec(feed_to_crawler=True)
    links = [DiscoveredLink(url="https://example.com/next")]
    extractor.extract.return_value = _record(links=links)

    await worker._handle(_task())

    frontier.feed_links_to_crawler.assert_awaited_once_with("https://example.com/list", links)


async def test_feed_to_crawler_on_but_no_links_does_not_feed():
    worker, frontier, fetcher, extractor, store = _worker()
    frontier.get_scrape_spec.return_value = _spec(feed_to_crawler=True)
    extractor.extract.return_value = _record(links=[])

    await worker._handle(_task())

    frontier.feed_links_to_crawler.assert_not_awaited()
