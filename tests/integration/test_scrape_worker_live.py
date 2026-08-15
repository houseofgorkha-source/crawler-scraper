"""
ScrapeWorker.run() against a REAL PostgresFrontier + real HTTP server --
the claim_scrape_targets() -> fetch -> extract -> complete_scrape loop,
formalizing what was previously only exercised via one-off manual scripts.
"""
import asyncio

from crawler.scrape_worker import ScrapeWorker
from tests.integration.test_worker_live import _store


async def _run_one_batch(worker: ScrapeWorker) -> None:
    tasks = await worker.frontier.claim_scrape(worker.worker_id, 20, 300)
    await asyncio.gather(*(worker._handle(t) for t in tasks))


async def test_claim_fetch_extract_complete_writes_real_scraped_record(
    db, frontier, fixture_server
):
    spec_id = await frontier.register_spec(
        name="live-listing-spec",
        fields=[{"name": "products", "selector": "div.product", "many": True,
                 "fields": [{"name": "name", "selector": "h2.name"},
                            {"name": "price", "selector": "span.price"}]}],
        render_mode="never",
    )
    await frontier.submit_scrape_targets(spec_id, [f"{fixture_server}/listing.html"])
    worker = ScrapeWorker(frontier, _store(), renderer=None, worker_id="live-s0")

    await _run_one_batch(worker)

    async with db.acquire() as conn:
        target = await conn.fetchrow(
            "SELECT status, last_status_code FROM scrape_targets WHERE url = $1",
            f"{fixture_server}/listing.html",
        )
        record = await conn.fetchrow(
            "SELECT r.data FROM scraped_records r JOIN scrape_targets t ON t.id = r.target_id "
            "WHERE t.url = $1", f"{fixture_server}/listing.html",
        )
    assert target["status"] == "done"
    assert target["last_status_code"] == 200

    import json
    data = json.loads(record["data"]) if isinstance(record["data"], str) else record["data"]
    assert data["products"] == [
        {"name": "Widget A", "price": "$9.99"},
        {"name": "Widget B", "price": "$19.99"},
    ]


async def test_inactive_spec_target_is_never_claimed_live(db, frontier, fixture_server):
    spec_id = await frontier.register_spec(
        name="live-inactive-spec", fields=[{"name": "h", "selector": "h1"}],
    )
    await frontier.set_spec_active("live-inactive-spec", False)
    await frontier.submit_scrape_targets(spec_id, [f"{fixture_server}/listing.html"])
    worker = ScrapeWorker(frontier, _store(), renderer=None, worker_id="live-s0")

    await _run_one_batch(worker)  # claim_scrape() must return nothing

    async with db.acquire() as conn:
        row = await conn.fetchrow(
            "SELECT status FROM scrape_targets WHERE url = $1", f"{fixture_server}/listing.html",
        )
    assert row["status"] == "pending"  # never claimed, still sitting there


async def test_fetch_failure_marks_target_failed(db, frontier, fixture_server):
    spec_id = await frontier.register_spec(
        name="live-fail-spec", fields=[{"name": "h", "selector": "h1"}],
    )
    await frontier.submit_scrape_targets(spec_id, [f"{fixture_server}/does-not-exist.html"])
    worker = ScrapeWorker(frontier, _store(), renderer=None, worker_id="live-s0")

    await _run_one_batch(worker)

    async with db.acquire() as conn:
        row = await conn.fetchrow(
            "SELECT status, last_status_code, consecutive_failures FROM scrape_targets WHERE url = $1",
            f"{fixture_server}/does-not-exist.html",
        )
    assert row["status"] == "pending"
    assert row["last_status_code"] == 404
    assert row["consecutive_failures"] == 1
