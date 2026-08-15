"""
IndexerDB's remaining queries (find_near_duplicate is covered separately
in test_near_duplicate.py): fetch_pending_documents, mark_indexed,
mark_index_failed, mark_duplicates.
"""
from crawler.db import IndexerDB


async def _seed_url_and_document(conn, url: str, index_state: str = "pending") -> int:
    domain_id = await conn.fetchval(
        "INSERT INTO domains (host) VALUES ($1) ON CONFLICT (host) DO UPDATE SET host=EXCLUDED.host RETURNING id",
        "idx.example",
    )
    url_id = await conn.fetchval(
        "INSERT INTO urls (domain_id, url) VALUES ($1,$2) RETURNING id", domain_id, url,
    )
    await conn.execute(
        """INSERT INTO documents (url_id, title, raw_key, text_key, index_state)
           VALUES ($1, 'T', 'raw/k', 'text/k', $2)""",
        url_id, index_state,
    )
    return url_id


async def test_fetch_pending_documents_only_returns_pending(db):
    async with db.acquire() as conn:
        pending_id = await _seed_url_and_document(conn, "https://idx.example/pending", "pending")
        await _seed_url_and_document(conn, "https://idx.example/done", "indexed")

    idb = IndexerDB(db)
    rows = await idb.fetch_pending_documents(batch=100)
    ids = {r["url_id"] for r in rows}
    assert pending_id in ids
    assert len(rows) == 1


async def test_fetch_pending_documents_respects_batch_limit(db):
    async with db.acquire() as conn:
        for i in range(5):
            await _seed_url_and_document(conn, f"https://idx.example/{i}", "pending")

    idb = IndexerDB(db)
    rows = await idb.fetch_pending_documents(batch=2)
    assert len(rows) == 2


async def test_mark_indexed_sets_state_and_timestamp(db):
    async with db.acquire() as conn:
        url_id = await _seed_url_and_document(conn, "https://idx.example/a", "pending")

    idb = IndexerDB(db)
    await idb.mark_indexed([url_id])

    async with db.acquire() as conn:
        row = await conn.fetchrow(
            "SELECT index_state, indexed_at FROM documents WHERE url_id=$1", url_id
        )
    assert row["index_state"] == "indexed"
    assert row["indexed_at"] is not None


async def test_mark_indexed_with_empty_list_is_a_noop(db):
    idb = IndexerDB(db)
    await idb.mark_indexed([])  # must not raise, must not touch anything


async def test_mark_index_failed_sets_state(db):
    async with db.acquire() as conn:
        url_id = await _seed_url_and_document(conn, "https://idx.example/b", "pending")

    idb = IndexerDB(db)
    await idb.mark_index_failed([url_id])

    async with db.acquire() as conn:
        row = await conn.fetchrow("SELECT index_state FROM documents WHERE url_id=$1", url_id)
    assert row["index_state"] == "failed"


async def test_mark_duplicates_sets_suppressed_and_duplicate_of(db):
    async with db.acquire() as conn:
        original_id = await _seed_url_and_document(conn, "https://idx.example/orig", "indexed")
        dup_id = await _seed_url_and_document(conn, "https://idx.example/dup", "pending")

    idb = IndexerDB(db)
    await idb.mark_duplicates([(dup_id, original_id)])

    async with db.acquire() as conn:
        row = await conn.fetchrow(
            "SELECT index_state, duplicate_of FROM documents WHERE url_id=$1", dup_id
        )
    assert row["index_state"] == "suppressed"
    assert row["duplicate_of"] == original_id


async def test_mark_duplicates_with_empty_list_is_a_noop(db):
    idb = IndexerDB(db)
    await idb.mark_duplicates([])  # must not raise
