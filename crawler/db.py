"""
Thin query layer for the indexer. Kept separate from PostgresFrontier
because the indexer is a different process with a different access pattern
(scan + batch update) than the crawl-time frontier (claim + point writes).
"""
from __future__ import annotations

import asyncpg


class IndexerDB:
    def __init__(self, pool: asyncpg.Pool):
        self.pool = pool

    async def fetch_pending_documents(self, batch: int) -> list[dict]:
        async with self.pool.acquire() as conn:
            rows = await conn.fetch(
                """SELECT d.url_id, u.url, d.title, d.description, d.lang,
                          d.text_key, u.simhash
                   FROM documents d JOIN urls u ON u.id = d.url_id
                   WHERE d.index_state = 'pending'
                   ORDER BY d.updated_at
                   LIMIT $1
                   FOR UPDATE OF d SKIP LOCKED""",
                batch,
            )
        return [dict(r) for r in rows]

    async def find_near_duplicate(self, simhash: int, url_id: int,
                                  max_distance: int, hamming_fn) -> int | None:
        # V1 approach: check against recently-indexed docs on the SAME
        # content_sha prefix bucket is overkill for medium scale; a direct
        # scan of candidates sharing a coarse simhash prefix is sufficient
        # and keeps this dependency-free. Revisit with an LSH index if the
        # candidate set grows past what this query can do in real time.
        async with self.pool.acquire() as conn:
            candidates = await conn.fetch(
                """SELECT id, simhash FROM urls
                   WHERE simhash IS NOT NULL AND id != $1
                     AND (simhash >> 56) = ($2::bigint >> 56)
                   LIMIT 500""",
                url_id, simhash,
            )
        for c in candidates:
            if hamming_fn(c["simhash"], simhash) <= max_distance:
                return c["id"]
        return None

    async def mark_indexed(self, url_ids: list[int]) -> None:
        if not url_ids:
            return
        async with self.pool.acquire() as conn:
            await conn.execute(
                """UPDATE documents SET index_state = 'indexed', indexed_at = now()
                   WHERE url_id = ANY($1::bigint[])""", url_ids,
            )

    async def mark_index_failed(self, url_ids: list[int]) -> None:
        if not url_ids:
            return
        async with self.pool.acquire() as conn:
            await conn.execute(
                "UPDATE documents SET index_state = 'failed' WHERE url_id = ANY($1::bigint[])",
                url_ids,
            )

    async def mark_duplicates(self, pairs: list[tuple[int, int]]) -> None:
        if not pairs:
            return
        async with self.pool.acquire() as conn:
            await conn.executemany(
                """UPDATE documents SET index_state = 'suppressed', duplicate_of = $2,
                       indexed_at = now() WHERE url_id = $1""",
                pairs,
            )
