"""
PostgresFrontier round-trip tests against the isolated test database.
Uses a real Redis instance but a dedicated logical DB (index 15) so it
can never collide with the dev cache (Redis is cache-only by design, but
isolating it keeps test runs deterministic regardless).

The `frontier` fixture itself lives in tests/conftest.py, shared with the
live worker integration tests.
"""
from crawler.contracts import (
    DiscoveredLink, ExtractedDoc, FetchOutcome, FetchResult, RenderMode, CrawlTask,
)


def _doc(url_id: int, links=()) -> ExtractedDoc:
    return ExtractedDoc(
        url_id=url_id, canonical_url="https://x.example/p", title="T",
        description=None, text="hello world", lang="en", word_count=2,
        content_sha256=b"\x00" * 32, simhash=123, render_mode=RenderMode.STATIC,
        links=list(links),
    )


def _result(task) -> FetchResult:
    return FetchResult(
        task=task, outcome=FetchOutcome.OK, status_code=200,
        final_url=task.url, body=b"<html></html>", render_mode=RenderMode.STATIC,
    )


async def test_seed_creates_domain_and_url(frontier, db):
    n = await frontier.seed(["https://x.example/p"])
    assert n == 1
    async with db.acquire() as conn:
        count = await conn.fetchval("SELECT count(*) FROM urls")
    assert count == 1


async def test_seed_dedups_on_reseed(frontier):
    await frontier.seed(["https://x.example/p"])
    n = await frontier.seed(["https://x.example/p"])
    assert n == 0  # already exists, ON CONFLICT DO NOTHING


async def test_claim_then_complete_writes_document(frontier, db):
    await frontier.seed(["https://x.example/p"])
    tasks = await frontier.claim("w1", 20, 300)
    assert len(tasks) == 1
    task = tasks[0]

    result = _result(task)
    await frontier.complete(result, _doc(task.url_id), raw_key="raw/k", text_key="text/k")

    async with db.acquire() as conn:
        row = await conn.fetchrow("SELECT status FROM urls WHERE id=$1", task.url_id)
        doc_row = await conn.fetchrow("SELECT title FROM documents WHERE url_id=$1", task.url_id)
    assert row["status"] == "done"
    assert doc_row["title"] == "T"


async def test_complete_discovers_links_via_add(frontier, db):
    await frontier.seed(["https://x.example/p"])
    tasks = await frontier.claim("w1", 20, 300)
    task = tasks[0]
    result = _result(task)
    links = [DiscoveredLink(url="https://x.example/other")]
    await frontier.complete(result, _doc(task.url_id), raw_key="raw/k", text_key="text/k")
    n = await frontier.add(links, from_url_id=task.url_id, depth=1)
    assert n == 1
    async with db.acquire() as conn:
        edge = await conn.fetchrow(
            "SELECT * FROM links WHERE from_url_id=$1", task.url_id
        )
    assert edge is not None


async def test_add_dedups_existing_url_but_still_writes_edge(frontier, db):
    await frontier.seed(["https://x.example/p", "https://x.example/other"])
    tasks = await frontier.claim("w1", 20, 300)
    origin = next(t for t in tasks if t.url == "https://x.example/p")
    n = await frontier.add([DiscoveredLink(url="https://x.example/other")],
                           from_url_id=origin.url_id, depth=1)
    assert n == 0  # already existed
    async with db.acquire() as conn:
        edge = await conn.fetchrow("SELECT * FROM links WHERE from_url_id=$1", origin.url_id)
    assert edge is not None  # edge still recorded


async def test_fail_backs_off_and_eventually_marks_failed(frontier, db):
    await frontier.seed(["https://x.example/p"])
    tasks = await frontier.claim("w1", 20, 300)
    task = tasks[0]
    result = FetchResult(task=task, outcome=FetchOutcome.HTTP_ERROR, status_code=500)

    for _ in range(5):
        await frontier.fail(result, max_failures=5)
        async with db.acquire() as conn:
            await conn.execute(
                "UPDATE urls SET status='leased' WHERE id=$1 AND status='pending'", task.url_id
            )

    async with db.acquire() as conn:
        row = await conn.fetchrow(
            "SELECT status, consecutive_failures FROM urls WHERE id=$1", task.url_id
        )
    assert row["status"] == "failed"
    assert row["consecutive_failures"] == 5


async def test_skip_marks_skipped(frontier, db):
    await frontier.seed(["https://x.example/p"])
    tasks = await frontier.claim("w1", 20, 300)
    await frontier.skip(tasks[0], reason="robots_denied")
    async with db.acquire() as conn:
        row = await conn.fetchrow("SELECT status FROM urls WHERE id=$1", tasks[0].url_id)
    assert row["status"] == "skipped"


async def test_register_spec_and_submit_and_claim_scrape(frontier, db):
    spec_id = await frontier.register_spec(
        name="test-spec", fields=[{"name": "title", "selector": "h1"}],
    )
    n = await frontier.submit_scrape_targets(spec_id, ["https://y.example/list"])
    assert n == 1

    tasks = await frontier.claim_scrape("s1", 20, 300)
    assert len(tasks) == 1
    assert tasks[0].spec_id == spec_id


async def test_get_scrape_spec_round_trips_nested_fields(frontier):
    spec_id = await frontier.register_spec(
        name="nested-spec",
        fields=[{"name": "items", "selector": "div.item", "many": True,
                 "fields": [{"name": "title", "selector": "h2"}]}],
    )
    spec = await frontier.get_scrape_spec(spec_id)
    assert spec.fields[0].many is True
    assert spec.fields[0].fields[0].name == "title"


async def test_enroll_scrape_targets_matches_host_and_path_regex(frontier, db):
    await frontier.seed(["https://y.example/article.html"])
    spec_id = await frontier.register_spec(
        name="feed-in-spec", fields=[], feed_from_crawler=True,
        host_pattern="y.example", path_regex="article\\.html",
    )
    n = await frontier.enroll_scrape_targets("https://y.example/article.html", "y.example")
    assert n == 1
    async with db.acquire() as conn:
        row = await conn.fetchrow(
            "SELECT * FROM scrape_targets WHERE spec_id=$1", spec_id
        )
    assert row is not None


async def test_enroll_scrape_targets_does_not_match_wrong_path(frontier):
    await frontier.register_spec(
        name="feed-in-spec-2", fields=[], feed_from_crawler=True,
        host_pattern="y.example", path_regex="article\\.html",
    )
    n = await frontier.enroll_scrape_targets("https://y.example/other.html", "y.example")
    assert n == 0


async def test_feed_links_to_crawler_creates_origin_and_target_urls(frontier, db):
    """A Scraper-only run (never crawled) must still be able to feed a
    discovered link into the Crawler frontier -- the origin url row is
    created on demand."""
    n = await frontier.feed_links_to_crawler(
        "https://z.example/scraped-only",
        [DiscoveredLink(url="https://z.example/discovered")],
    )
    assert n == 1
    async with db.acquire() as conn:
        rows = await conn.fetch("SELECT url FROM urls ORDER BY id")
    urls = {r["url"] for r in rows}
    assert "https://z.example/scraped-only" in urls
    assert "https://z.example/discovered" in urls


async def test_feed_links_to_crawler_dedups_existing_origin(frontier, db):
    await frontier.seed(["https://z.example/scraped-only"])
    await frontier.feed_links_to_crawler(
        "https://z.example/scraped-only",
        [DiscoveredLink(url="https://z.example/discovered")],
    )
    async with db.acquire() as conn:
        count = await conn.fetchval(
            "SELECT count(*) FROM urls WHERE url = 'https://z.example/scraped-only'"
        )
    assert count == 1  # not duplicated
