"""
PostgresFrontier round-trip tests against the isolated test database.
Uses a real Redis instance but a dedicated logical DB (index 15) so it
can never collide with the dev cache (Redis is cache-only by design, but
isolating it keeps test runs deterministic regardless).

The `frontier` fixture itself lives in tests/conftest.py, shared with the
live worker integration tests.
"""
import asyncpg
import pytest
import redis.asyncio as aioredis

from crawler.contracts import (
    DiscoveredLink, ExtractedDoc, FetchOutcome, FetchResult, RenderMode, CrawlTask,
)
from crawler.frontier import PostgresFrontier

from ..conftest import TEST_REDIS_URL


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


async def test_refresh_robots_preserves_port_in_origin_not_in_domain_key(db):
    """A non-standard-port URL must resolve robots.txt against its own
    authority (port included), while the domains row it's cached under
    stays keyed by the registrable, port-free host -- the same shared
    politeness key the Crawler and Scraper both gate on."""
    calls = []

    async def spy_fetcher(host, origin=None):
        calls.append((host, origin))
        return None, 404  # no restrictions

    redis_client = aioredis.from_url(TEST_REDIS_URL)
    await redis_client.flushdb()
    frontier = PostgresFrontier(db, redis_client, robots_fetcher=spy_fetcher)
    try:
        policy = await frontier.refresh_robots(
            "127.0.0.1", "http://127.0.0.1:51127/products.html"
        )
    finally:
        await redis_client.aclose()

    assert calls == [("127.0.0.1", "http://127.0.0.1:51127")]
    assert policy.is_crawlable

    async with db.acquire() as conn:
        row = await conn.fetchrow("SELECT host FROM domains WHERE host = $1", "127.0.0.1")
    assert row["host"] == "127.0.0.1"  # politeness key stays port-free


async def test_add_sorts_links_by_host_before_locking(frontier, db):
    """The AB-BA deadlock fix relies on add() acquiring domain/url locks in
    a deterministic order across concurrent callers. That determinism comes
    from sorting discovered links by host before the transaction starts --
    assert that ordering directly rather than only the end-to-end insert
    result, so a regression that silently drops the sort (but still inserts
    everything correctly) is still caught."""
    await frontier.seed(["https://origin.example/p"])
    tasks = await frontier.claim("w1", 20, 300)
    origin = tasks[0]

    links = [
        DiscoveredLink(url="https://zzz.example/a"),
        DiscoveredLink(url="https://aaa.example/b"),
        DiscoveredLink(url="https://mmm.example/c"),
    ]
    seen_hosts = []
    real_add_once = frontier._add_once

    async def spy_add_once(sorted_links, from_url_id, depth):
        seen_hosts.extend(l.url for l in sorted_links)
        return await real_add_once(sorted_links, from_url_id, depth)

    frontier._add_once = spy_add_once
    try:
        await frontier.add(links, from_url_id=origin.url_id, depth=1)
    finally:
        frontier._add_once = real_add_once

    assert seen_hosts == [
        "https://aaa.example/b", "https://mmm.example/c", "https://zzz.example/a",
    ]


async def test_add_retries_on_deadlock_then_succeeds(frontier, db):
    """The residual first-insert race (two transactions racing to INSERT
    the same brand-new domain) can't be prevented by lock ordering alone --
    add() must retry a bounded number of times rather than silently
    dropping the page's discovered links."""
    await frontier.seed(["https://origin2.example/p"])
    tasks = await frontier.claim("w1", 20, 300)
    origin = tasks[0]

    real_add_once = frontier._add_once
    calls = {"n": 0}

    async def flaky_add_once(links, from_url_id, depth):
        calls["n"] += 1
        if calls["n"] < 3:
            raise asyncpg.exceptions.DeadlockDetectedError("deadlock detected")
        return await real_add_once(links, from_url_id, depth)

    frontier._add_once = flaky_add_once
    try:
        n = await frontier.add(
            [DiscoveredLink(url="https://new-domain.example/x")],
            from_url_id=origin.url_id, depth=1,
        )
    finally:
        frontier._add_once = real_add_once

    assert calls["n"] == 3
    assert n == 1


async def test_add_gives_up_after_bounded_retries(frontier, db):
    """Retrying forever would hide a genuinely stuck transaction; the
    bound must actually be enforced, not just present as a loop with an
    unreachable exit."""
    await frontier.seed(["https://origin3.example/p"])
    tasks = await frontier.claim("w1", 20, 300)
    origin = tasks[0]

    calls = {"n": 0}

    async def always_deadlocks(links, from_url_id, depth):
        calls["n"] += 1
        raise asyncpg.exceptions.DeadlockDetectedError("deadlock detected")

    frontier._add_once = always_deadlocks
    with pytest.raises(asyncpg.exceptions.DeadlockDetectedError):
        await frontier.add(
            [DiscoveredLink(url="https://still-new.example/x")],
            from_url_id=origin.url_id, depth=1,
        )

    assert calls["n"] == 3


async def test_complete_updates_domain_before_url(frontier, db):
    """complete()'s lock order (domain row, then url row) must match
    add()'s to avoid the AB-BA cycle between the two functions. The two
    statements run back-to-back inside one transaction on one connection,
    so there's no externally observable side effect to assert on (no
    intermediate commit, no separate connection to race) -- the ordering
    itself only exists in source order. Assert against that directly: a
    regression that silently swaps the two UPDATEs back to urls-then-domains
    would reintroduce the AB-BA cycle with add() without changing any
    row's final value, so a result-only test can't catch it."""
    import inspect

    from crawler import frontier as frontier_module

    source = inspect.getsource(frontier_module.PostgresFrontier.complete)
    domains_pos = source.index("UPDATE domains SET pages_crawled")
    urls_pos = source.index("UPDATE urls SET")
    assert domains_pos < urls_pos, (
        "complete() must update domains before urls to match add()'s lock "
        "order -- see the comment above these statements in frontier.py"
    )

    # Also confirm the statements still do what they claim, so this test
    # fails if either UPDATE is ever removed rather than just reordered.
    await frontier.seed(["https://order.example/p"])
    tasks = await frontier.claim("w1", 20, 300)
    task = tasks[0]
    await frontier.complete(_result(task), _doc(task.url_id), raw_key="raw/k", text_key="text/k")
    async with db.acquire() as conn:
        url_row = await conn.fetchrow("SELECT status FROM urls WHERE id=$1", task.url_id)
        domain_row = await conn.fetchrow(
            "SELECT pages_crawled FROM domains WHERE host=$1", task.host
        )
    assert url_row["status"] == "done"
    assert domain_row["pages_crawled"] == 1


async def test_refresh_robots_without_url_omits_origin(db):
    """No task URL available (e.g. a bare-host caller) -- the fetcher gets
    no origin and falls back to its own https/http guess, matching
    pre-existing behavior for callers that only have a host."""
    calls = []

    async def spy_fetcher(host, origin=None):
        calls.append((host, origin))
        return None, 404

    redis_client = aioredis.from_url(TEST_REDIS_URL)
    await redis_client.flushdb()
    frontier = PostgresFrontier(db, redis_client, robots_fetcher=spy_fetcher)
    try:
        await frontier.refresh_robots("example.com")
    finally:
        await redis_client.aclose()

    assert calls == [("example.com", None)]


async def test_policy_for_survives_redis_outage(db):
    """Redis is a cache only (CLAUDE.md: "Postgres is the source of truth
    ... do not make anything durable depend on Redis"), but
    _cache_policy() used to call redis.set() unconditionally on every
    policy_for() call -- including the Postgres-cache-hit path, not just a
    genuine refresh -- with no error handling. A Redis outage propagated
    out of policy_for(), which worker.py calls as the first step of every
    task; since the failure landed before frontier.fail()/skip()/complete(),
    the lease just sat until reap timeout and reclaimed into the same
    failure again, forever, with none of the normal backoff -- a Redis
    blip silently stalled the whole crawl instead of merely losing a
    cache. Point the frontier at a connection that can never succeed
    (an unroutable address, not just a wrong port -- a wrong port can
    still fail fast with connection-refused; TEST-NET-1 guarantees a
    connect timeout, the same failure mode a real network partition
    produces) to reproduce the outage deterministically, and prove both
    the fresh-refresh path and the cache-hit path complete anyway."""
    async def robots_fetcher(host, origin=None):
        return "User-agent: *\nDisallow: /blocked\n", 200

    unreachable_redis = aioredis.from_url(
        "redis://192.0.2.1:6379/0", socket_connect_timeout=1, socket_timeout=1,
    )
    frontier = PostgresFrontier(db, unreachable_redis, robots_fetcher=robots_fetcher)

    # Fresh-refresh path (no Postgres row yet for this host).
    policy = await frontier.policy_for("redis-outage.example", "http://redis-outage.example/p")
    assert policy.is_crawlable is True
    assert policy.check_allowed("http://redis-outage.example/p") is True
    assert policy.check_allowed("http://redis-outage.example/blocked") is False

    # Postgres-cache-hit path -- the row now exists and is fresh, so this
    # call takes the OTHER branch that also used to touch Redis unguarded.
    policy2 = await frontier.policy_for("redis-outage.example", "http://redis-outage.example/p")
    assert policy2.is_crawlable is True
    assert policy2.check_allowed("http://redis-outage.example/blocked") is False

    # And the rest of the frontier (which never touched Redis) is
    # unaffected, proving Postgres-backed progress continues normally.
    await frontier.seed(["http://redis-outage.example/p"])
    tasks = await frontier.claim("w1", 5, 300)
    assert len(tasks) == 1

    await unreachable_redis.aclose()
