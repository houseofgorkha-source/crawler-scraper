"""
CrawlWorker.run() against a REAL PostgresFrontier and a real (in-process)
HTTP server -- the claim_urls() -> fetch -> extract -> complete loop has,
this entire project, only ever been exercised via one-off manual scripts
written, run, and deleted by hand. This formalizes it: same fixture_server
+ frontier isolation as everything else in tests/integration/, never the
dev database.
"""
import asyncio

from crawler.challenge import RendererChallengeResolver
from crawler.contracts import FetchOutcome, FetchResult, RenderMode
from crawler.store import BlobStore
from crawler.worker import CrawlWorker

class _FakeBody:
    def __init__(self, data: bytes):
        self._data = data

    async def read(self) -> bytes:
        return self._data


class _FakeS3Client:
    """Same fake used in test_store.py -- BlobStore is decoupled from
    which S3 client backs it, and live-testing it doesn't need MinIO."""

    def __init__(self):
        self.objects: dict[str, bytes] = {}

    async def put_object(self, Bucket, Key, Body, ContentType, ContentEncoding):
        self.objects[Key] = Body

    async def get_object(self, Bucket, Key):
        return {"Body": _FakeBody(self.objects[Key])}


def _store() -> BlobStore:
    return BlobStore(_FakeS3Client(), "test-bucket")


async def _run_one_batch(worker: CrawlWorker) -> None:
    """Drives exactly one claim+handle cycle instead of the infinite
    run() loop, mirroring the pattern used throughout this project's
    manual verification -- a real claim_urls() call, real _handle()."""
    tasks = await worker.frontier.claim(worker.worker_id, 20, 300)
    await asyncio.gather(*(worker._handle(t) for t in tasks))


async def test_claim_fetch_extract_complete_writes_real_document(db, frontier, fixture_server):
    await frontier.seed([f"{fixture_server}/static_page.html"])
    worker = CrawlWorker(frontier, _store(), renderer=None, worker_id="live-w0")

    await _run_one_batch(worker)

    async with db.acquire() as conn:
        row = await conn.fetchrow(
            "SELECT status, last_status_code FROM urls WHERE url = $1",
            f"{fixture_server}/static_page.html",
        )
        doc = await conn.fetchrow(
            "SELECT title, word_count FROM documents d JOIN urls u ON u.id = d.url_id "
            "WHERE u.url = $1", f"{fixture_server}/static_page.html",
        )
    assert row["status"] == "done"
    assert row["last_status_code"] == 200
    assert doc["title"] == "Static Fixture Page"
    assert doc["word_count"] > 0


async def test_discovered_link_is_enqueued_via_real_claim_urls(db, frontier, fixture_server):
    await frontier.seed([f"{fixture_server}/static_page.html"])
    worker = CrawlWorker(frontier, _store(), renderer=None, worker_id="live-w0")

    await _run_one_batch(worker)

    async with db.acquire() as conn:
        discovered = await conn.fetchrow(
            "SELECT status FROM urls WHERE url = $1", f"{fixture_server}/discovered.html",
        )
    assert discovered is not None
    assert discovered["status"] == "pending"


async def test_app_shell_without_renderer_falls_back_to_static_not_a_crash(db, frontier, fixture_server):
    """needs_render() will say True for this fixture, but renderer=None --
    the worker must complete using the static body, not raise."""
    await frontier.seed([f"{fixture_server}/app_shell.html"])
    worker = CrawlWorker(frontier, _store(), renderer=None, worker_id="live-w0")

    await _run_one_batch(worker)  # must not raise

    async with db.acquire() as conn:
        row = await conn.fetchrow(
            "SELECT status, render_mode FROM urls WHERE url = $1",
            f"{fixture_server}/app_shell.html",
        )
    assert row["status"] in ("done", "skipped")  # low/no visible text may skip as no_content
    assert row["render_mode"] in (None, "static")  # never "rendered" -- no renderer configured


async def test_fetch_failure_marks_url_failed_with_real_backoff(db, frontier, fixture_server):
    await frontier.seed([f"{fixture_server}/does-not-exist.html"])
    worker = CrawlWorker(frontier, _store(), renderer=None, worker_id="live-w0")

    await _run_one_batch(worker)

    async with db.acquire() as conn:
        row = await conn.fetchrow(
            "SELECT status, last_status_code, consecutive_failures, next_crawl_at > now() AS backed_off "
            "FROM urls WHERE url = $1",
            f"{fixture_server}/does-not-exist.html",
        )
    assert row["status"] == "pending"  # first failure, not yet permanently failed
    assert row["last_status_code"] == 404
    assert row["consecutive_failures"] == 1
    assert row["backed_off"] is True


async def test_feed_scraper_enrolls_via_real_enroll_scrape_targets(db, frontier, fixture_server):
    await frontier.register_spec(
        name="live-feed-spec", fields=[{"name": "h", "selector": "h1"}],
        feed_from_crawler=True, path_regex="static_page\\.html",
    )
    await frontier.seed([f"{fixture_server}/static_page.html"])
    worker = CrawlWorker(frontier, _store(), renderer=None, worker_id="live-w0",
                         feed_scraper=True)

    await _run_one_batch(worker)

    async with db.acquire() as conn:
        target = await conn.fetchrow(
            "SELECT status FROM scrape_targets WHERE url = $1",
            f"{fixture_server}/static_page.html",
        )
    assert target is not None
    assert target["status"] == "pending"


async def test_403_fixture_is_classified_and_fails_normally(
    db, frontier, fixture_server
):
    url = f"{fixture_server}/lab/403"
    await frontier.seed([url])

    worker = CrawlWorker(frontier, _store(), renderer=None, worker_id="live-w0")

    await _run_one_batch(worker)

    async with db.acquire() as conn:
        row = await conn.fetchrow(
            """
            SELECT status, last_status_code, consecutive_failures
            FROM urls
            WHERE url = $1
            """,
            url,
        )

    assert row["last_status_code"] == 403
    assert row["consecutive_failures"] == 1
    assert row["status"] == "pending"


async def test_429_fixture_preserves_rate_limit_response_and_backoff(
    db, frontier, fixture_server
):
    url = f"{fixture_server}/lab/429"
    await frontier.seed([url])

    worker = CrawlWorker(frontier, _store(), renderer=None, worker_id="live-w0")

    await _run_one_batch(worker)

    async with db.acquire() as conn:
        row = await conn.fetchrow(
            """
            SELECT status,
                   last_status_code,
                   consecutive_failures,
                   next_crawl_at > now() AS backed_off
            FROM urls
            WHERE url = $1
            """,
            url,
        )

    assert row["last_status_code"] == 429
    assert row["consecutive_failures"] == 1
    assert row["status"] == "pending"
    assert row["backed_off"] is True


async def test_js_challenge_fixture_is_resolved_by_renderer(
    db, frontier, fixture_server, renderer
):
    url = f"{fixture_server}/lab/js-challenge"
    await frontier.seed([url])

    worker = CrawlWorker(
        frontier,
        _store(),
        renderer=renderer,
        challenge_resolver=RendererChallengeResolver(renderer),
        worker_id="live-w0",
    )

    await _run_one_batch(worker)

    async with db.acquire() as conn:
        row = await conn.fetchrow(
            """
            SELECT status, last_status_code, consecutive_failures
            FROM urls
            WHERE url = $1
            """,
            url,
        )

    assert row["last_status_code"] == 200
    assert row["consecutive_failures"] == 0
    assert row["status"] == "done"


async def test_captcha_fixture_is_detected_and_follows_normal_failure_path(
    db, frontier, fixture_server, renderer
):
    url = f"{fixture_server}/lab/captcha"
    await frontier.seed([url])

    worker = CrawlWorker(
        frontier,
        _store(),
        renderer=renderer,
        challenge_resolver=RendererChallengeResolver(renderer),
        worker_id="live-w0",
    )

    await _run_one_batch(worker)

    async with db.acquire() as conn:
        row = await conn.fetchrow(
            """
            SELECT status, last_status_code, consecutive_failures
            FROM urls
            WHERE url = $1
            """,
            url,
        )

    assert row["last_status_code"] == 200
    assert row["consecutive_failures"] == 0
    assert row["status"] == "done"


async def test_authentication_fixture_fails_normally(
    db, frontier, fixture_server
):
    url = f"{fixture_server}/lab/auth"
    await frontier.seed([url])

    worker = CrawlWorker(frontier, _store(), renderer=None, worker_id="live-w0")

    await _run_one_batch(worker)

    async with db.acquire() as conn:
        row = await conn.fetchrow(
            """
            SELECT status, last_status_code, consecutive_failures
            FROM urls
            WHERE url = $1
            """,
            url,
        )

    assert row["last_status_code"] == 401
    assert row["consecutive_failures"] == 1
    assert row["status"] == "pending"