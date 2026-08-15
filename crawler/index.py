"""
Asynchronous indexer. Runs as its OWN process, drains documents where
index_state='pending'.

Decoupled on purpose: Meilisearch being down, slow, or mid-reindex must never
apply backpressure to the crawl. The worst case here is a growing pending
queue, which is a metric, not an outage.

Near-duplicates are suppressed at this boundary rather than at write time --
the document and its graph edges are kept, only the index entry is skipped.
"""
from __future__ import annotations

import asyncio
import logging

from .extract import hamming

log = logging.getLogger(__name__)

BATCH = 200
POLL_SECONDS = 5.0
DUPLICATE_DISTANCE = 3      # hamming distance on 64-bit simhash


class Indexer:
    def __init__(self, db, search_client, store, index_name: str = "pages"):
        self.db = db
        self.search = search_client
        self.store = store
        self.index_name = index_name
        self._running = False

    async def run(self) -> None:
        self._running = True
        while self._running:
            rows = await self.db.fetch_pending_documents(BATCH)
            if not rows:
                await asyncio.sleep(POLL_SECONDS)
                continue
            try:
                await self._process(rows)
            except Exception:
                log.exception("index batch failed; will retry")
                await self.db.mark_index_failed([r["url_id"] for r in rows])
                await asyncio.sleep(POLL_SECONDS)

    def stop(self) -> None:
        self._running = False

    async def _process(self, rows) -> None:
        payload, suppressed = [], []

        for row in rows:
            near = await self.db.find_near_duplicate(
                row["simhash"], row["url_id"], DUPLICATE_DISTANCE, hamming
            )
            if near is not None:
                suppressed.append((row["url_id"], near))
                continue

            text = (await self.store.get(row["text_key"])).decode("utf-8", "replace")
            payload.append({
                "id": row["url_id"],
                "url": row["url"],
                "title": row["title"],
                "description": row["description"],
                "lang": row["lang"],
                "content": text[:50_000],
            })

        if payload:
            await self.search.index(self.index_name).add_documents(payload)
        if suppressed:
            await self.db.mark_duplicates(suppressed)
        await self.db.mark_indexed([d["id"] for d in payload])


SETTINGS = {
    "searchableAttributes": ["title", "description", "content"],
    "filterableAttributes": ["lang"],
    "displayedAttributes": ["id", "url", "title", "description", "lang"],
    "rankingRules": ["words", "typo", "proximity", "attribute", "exactness"],
}
