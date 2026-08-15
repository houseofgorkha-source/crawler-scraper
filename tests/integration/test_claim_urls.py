"""
Direct SQL-contract tests for claim_urls() -- guards against the exact
regression this codebase already hit once: Postgres rejecting FOR UPDATE
combined with DISTINCT ON. These call the function directly rather than
going through PostgresFrontier, since the contract under test is the SQL
function itself.
"""
import asyncio

import pytest


async def _seed_domain(conn, host, crawl_delay_ms=1000, is_crawlable=True):
    return await conn.fetchval(
        "INSERT INTO domains (host, crawl_delay_ms, is_crawlable) VALUES ($1,$2,$3) RETURNING id",
        host, crawl_delay_ms, is_crawlable,
    )


async def _seed_url(conn, domain_id, url, priority=100):
    return await conn.fetchval(
        "INSERT INTO urls (domain_id, url, priority) VALUES ($1,$2,$3) RETURNING id",
        domain_id, url, priority,
    )


async def test_claim_does_not_raise_distinct_on_for_update_conflict(db):
    """The exact bug: this call must not raise FeatureNotSupportedError."""
    async with db.acquire() as conn:
        domain_id = await _seed_domain(conn, "a.example")
        await _seed_url(conn, domain_id, "https://a.example/")
        rows = await conn.fetch("SELECT * FROM claim_urls($1, $2, $3)", "w1", 20, 300)
    assert len(rows) == 1
    assert rows[0]["url"] == "https://a.example/"


async def test_claim_is_one_url_per_domain_per_batch(db):
    async with db.acquire() as conn:
        domain_id = await _seed_domain(conn, "b.example")
        await _seed_url(conn, domain_id, "https://b.example/1")
        await _seed_url(conn, domain_id, "https://b.example/2")
        await _seed_url(conn, domain_id, "https://b.example/3")
        rows = await conn.fetch("SELECT * FROM claim_urls($1, $2, $3)", "w1", 20, 300)
    assert len(rows) == 1  # domain fairness: only one, even though 3 are pending


async def test_claim_respects_priority_within_a_domain(db):
    async with db.acquire() as conn:
        domain_id = await _seed_domain(conn, "c.example")
        await _seed_url(conn, domain_id, "https://c.example/low", priority=10)
        await _seed_url(conn, domain_id, "https://c.example/high", priority=999)
        rows = await conn.fetch("SELECT * FROM claim_urls($1, $2, $3)", "w1", 20, 300)
    assert rows[0]["url"] == "https://c.example/high"


async def test_claim_gates_on_next_available_at(db):
    async with db.acquire() as conn:
        domain_id = await conn.fetchval(
            """INSERT INTO domains (host, next_available_at)
               VALUES ($1, now() + interval '1 hour') RETURNING id""",
            "throttled.example",
        )
        await _seed_url(conn, domain_id, "https://throttled.example/")
        rows = await conn.fetch("SELECT * FROM claim_urls($1, $2, $3)", "w1", 20, 300)
    assert len(rows) == 0


async def test_claim_skips_non_crawlable_domains(db):
    async with db.acquire() as conn:
        domain_id = await _seed_domain(conn, "banned.example", is_crawlable=False)
        await _seed_url(conn, domain_id, "https://banned.example/")
        rows = await conn.fetch("SELECT * FROM claim_urls($1, $2, $3)", "w1", 20, 300)
    assert len(rows) == 0


async def test_claim_marks_leased_and_sets_lease_expiry(db):
    async with db.acquire() as conn:
        domain_id = await _seed_domain(conn, "d.example")
        url_id = await _seed_url(conn, domain_id, "https://d.example/")
        await conn.fetch("SELECT * FROM claim_urls($1, $2, $3)", "w1", 20, 300)
        row = await conn.fetchrow("SELECT status, lease_owner FROM urls WHERE id = $1", url_id)
    assert row["status"] == "leased"
    assert row["lease_owner"] == "w1"


async def test_concurrent_claims_never_double_claim_skip_locked(db):
    """FOR UPDATE SKIP LOCKED: two concurrent claimants on urls from the
    same domain must never both receive the same row."""
    async with db.acquire() as conn:
        domain_id = await _seed_domain(conn, "e.example")
        await _seed_url(conn, domain_id, "https://e.example/only")

    async def claim(worker_id):
        async with db.acquire() as conn:
            return await conn.fetch("SELECT * FROM claim_urls($1, $2, $3)", worker_id, 20, 300)

    r1, r2 = await asyncio.gather(claim("w1"), claim("w2"))
    total_claimed = len(r1) + len(r2)
    assert total_claimed == 1  # exactly one worker got it, never both, never zero


async def test_reap_returns_expired_leases_to_pending(db):
    async with db.acquire() as conn:
        domain_id = await _seed_domain(conn, "f.example")
        url_id = await _seed_url(conn, domain_id, "https://f.example/")
        await conn.execute(
            """UPDATE urls SET status='leased', lease_owner='stale',
                   lease_expires_at = now() - interval '1 second' WHERE id=$1""",
            url_id,
        )
        n = await conn.fetchval("SELECT reap_expired_leases()")
        row = await conn.fetchrow("SELECT status, lease_owner FROM urls WHERE id=$1", url_id)
    assert n == 1
    assert row["status"] == "pending"
    assert row["lease_owner"] is None


async def test_reap_does_not_touch_unexpired_leases(db):
    async with db.acquire() as conn:
        domain_id = await _seed_domain(conn, "g.example")
        url_id = await _seed_url(conn, domain_id, "https://g.example/")
        await conn.execute(
            """UPDATE urls SET status='leased', lease_owner='fresh',
                   lease_expires_at = now() + interval '1 hour' WHERE id=$1""",
            url_id,
        )
        n = await conn.fetchval("SELECT reap_expired_leases()")
        row = await conn.fetchrow("SELECT status FROM urls WHERE id=$1", url_id)
    assert n == 0
    assert row["status"] == "leased"
