"""
Direct SQL-contract tests for claim_scrape_targets() -- guards the
is_active regression: a deactivated spec's targets must stop being
claimed, not just stop accepting new enrollments.
"""
import asyncio

import pytest


async def _seed_domain(conn, host, is_crawlable=True):
    return await conn.fetchval(
        "INSERT INTO domains (host, is_crawlable) VALUES ($1,$2) RETURNING id",
        host, is_crawlable,
    )


async def _seed_spec(conn, name="spec", is_active=True):
    return await conn.fetchval(
        """INSERT INTO scrape_specs (name, fields, is_active)
           VALUES ($1, '[]'::jsonb, $2) RETURNING id""",
        name, is_active,
    )


async def _seed_target(conn, spec_id, domain_id, url):
    return await conn.fetchval(
        "INSERT INTO scrape_targets (spec_id, domain_id, url) VALUES ($1,$2,$3) RETURNING id",
        spec_id, domain_id, url,
    )


async def test_claim_does_not_raise_distinct_on_for_update_conflict(db):
    async with db.acquire() as conn:
        domain_id = await _seed_domain(conn, "a.example")
        spec_id = await _seed_spec(conn)
        await _seed_target(conn, spec_id, domain_id, "https://a.example/")
        rows = await conn.fetch("SELECT * FROM claim_scrape_targets($1, $2, $3)", "s1", 20, 300)
    assert len(rows) == 1


async def test_inactive_spec_targets_are_never_claimed(db):
    """The actual bug: is_active must gate claiming, not just enrollment."""
    async with db.acquire() as conn:
        domain_id = await _seed_domain(conn, "inactive.example")
        spec_id = await _seed_spec(conn, is_active=False)
        await _seed_target(conn, spec_id, domain_id, "https://inactive.example/")
        rows = await conn.fetch("SELECT * FROM claim_scrape_targets($1, $2, $3)", "s1", 20, 300)
    assert len(rows) == 0


async def test_reactivated_spec_targets_become_claimable_again(db):
    async with db.acquire() as conn:
        domain_id = await _seed_domain(conn, "toggle.example")
        spec_id = await _seed_spec(conn, is_active=False)
        await _seed_target(conn, spec_id, domain_id, "https://toggle.example/")

        rows = await conn.fetch("SELECT * FROM claim_scrape_targets($1, $2, $3)", "s1", 20, 300)
        assert len(rows) == 0

        await conn.execute("UPDATE scrape_specs SET is_active = true WHERE id = $1", spec_id)
        rows = await conn.fetch("SELECT * FROM claim_scrape_targets($1, $2, $3)", "s1", 20, 300)
    assert len(rows) == 1


async def test_active_and_inactive_specs_on_same_domain_only_active_claimed(db):
    """Deactivating one spec must not starve other active specs sharing
    the same domain's claim slot."""
    async with db.acquire() as conn:
        domain_id = await _seed_domain(conn, "mixed.example")
        inactive_spec = await _seed_spec(conn, name="off", is_active=False)
        active_spec = await _seed_spec(conn, name="on", is_active=True)
        await _seed_target(conn, inactive_spec, domain_id, "https://mixed.example/off")
        await _seed_target(conn, active_spec, domain_id, "https://mixed.example/on")

        rows = await conn.fetch("SELECT * FROM claim_scrape_targets($1, $2, $3)", "s1", 20, 300)
    assert len(rows) == 1
    assert rows[0]["url"] == "https://mixed.example/on"


async def test_claim_is_one_target_per_domain_per_batch(db):
    async with db.acquire() as conn:
        domain_id = await _seed_domain(conn, "b.example")
        spec_id = await _seed_spec(conn)
        await _seed_target(conn, spec_id, domain_id, "https://b.example/1")
        await _seed_target(conn, spec_id, domain_id, "https://b.example/2")
        rows = await conn.fetch("SELECT * FROM claim_scrape_targets($1, $2, $3)", "s1", 20, 300)
    assert len(rows) == 1


async def test_claim_gates_on_shared_domain_politeness_clock(db):
    """claim_scrape_targets() must respect the SAME domains.next_available_at
    the Crawler's claim_urls() uses -- not a separate clock."""
    async with db.acquire() as conn:
        domain_id = await conn.fetchval(
            """INSERT INTO domains (host, next_available_at)
               VALUES ($1, now() + interval '1 hour') RETURNING id""",
            "throttled.example",
        )
        spec_id = await _seed_spec(conn)
        await _seed_target(conn, spec_id, domain_id, "https://throttled.example/")
        rows = await conn.fetch("SELECT * FROM claim_scrape_targets($1, $2, $3)", "s1", 20, 300)
    assert len(rows) == 0


async def test_concurrent_claims_never_double_claim_skip_locked(db):
    async with db.acquire() as conn:
        domain_id = await _seed_domain(conn, "e.example")
        spec_id = await _seed_spec(conn)
        await _seed_target(conn, spec_id, domain_id, "https://e.example/only")

    async def claim(worker_id):
        async with db.acquire() as conn:
            return await conn.fetch(
                "SELECT * FROM claim_scrape_targets($1, $2, $3)", worker_id, 20, 300
            )

    r1, r2 = await asyncio.gather(claim("s1"), claim("s2"))
    assert len(r1) + len(r2) == 1


async def test_reap_scrape_leases_returns_expired_to_pending(db):
    async with db.acquire() as conn:
        domain_id = await _seed_domain(conn, "f.example")
        spec_id = await _seed_spec(conn)
        target_id = await _seed_target(conn, spec_id, domain_id, "https://f.example/")
        await conn.execute(
            """UPDATE scrape_targets SET status='leased', lease_owner='stale',
                   lease_expires_at = now() - interval '1 second' WHERE id=$1""",
            target_id,
        )
        n = await conn.fetchval("SELECT reap_expired_scrape_leases()")
        row = await conn.fetchrow("SELECT status FROM scrape_targets WHERE id=$1", target_id)
    assert n == 1
    assert row["status"] == "pending"
