"""
Indexer._process() routing logic -- db/search/store all mocked. Verifies
that a near-duplicate is suppressed (never reaches Meilisearch, but is
still recorded via mark_duplicates) while a genuinely new document is
indexed normally, and that a mixed batch routes each row correctly.
"""
from unittest.mock import AsyncMock, MagicMock

from crawler.index import Indexer


def _row(url_id, simhash=100):
    return {
        "url_id": url_id, "url": f"https://example.com/{url_id}", "title": "T",
        "description": None, "lang": "en", "text_key": f"text/{url_id}", "simhash": simhash,
    }


def _indexer(near_duplicate_of=None):
    db = AsyncMock()
    db.find_near_duplicate.return_value = near_duplicate_of
    search = MagicMock()
    search.index.return_value = AsyncMock()
    store = AsyncMock()
    store.get.return_value = b"extracted page text"
    return Indexer(db, search, store), db, search, store


async def test_new_document_is_indexed_not_suppressed():
    indexer, db, search, store = _indexer(near_duplicate_of=None)

    await indexer._process([_row(1)])

    store.get.assert_awaited_once_with("text/1")
    search.index.return_value.add_documents.assert_awaited_once()
    payload = search.index.return_value.add_documents.call_args[0][0]
    assert payload == [{
        "id": 1, "url": "https://example.com/1", "title": "T",
        "description": None, "lang": "en", "content": "extracted page text",
    }]
    db.mark_indexed.assert_awaited_once_with([1])
    db.mark_duplicates.assert_not_awaited()


async def test_near_duplicate_is_suppressed_not_indexed():
    indexer, db, search, store = _indexer(near_duplicate_of=42)

    await indexer._process([_row(1)])

    store.get.assert_not_awaited()  # never fetched -- no point paying for the text
    search.index.return_value.add_documents.assert_not_awaited()
    db.mark_duplicates.assert_awaited_once_with([(1, 42)])
    db.mark_indexed.assert_awaited_once_with([])  # empty payload, still called


async def test_suppressed_document_keeps_its_graph_edges():
    """The document/links rows themselves are never touched here -- only
    documents.index_state changes, via mark_duplicates. This test exists
    to pin that _process() never calls anything that would delete a row."""
    indexer, db, search, store = _indexer(near_duplicate_of=42)

    await indexer._process([_row(1)])

    db.delete.assert_not_called()
    db.mark_duplicates.assert_awaited_once()


async def test_mixed_batch_routes_each_row_independently():
    db = AsyncMock()

    async def fake_find(simhash, url_id, max_distance, hamming_fn):
        return 999 if url_id == 2 else None  # only row 2 is a duplicate

    db.find_near_duplicate.side_effect = fake_find
    search = MagicMock()
    search.index.return_value = AsyncMock()
    store = AsyncMock()
    store.get.return_value = b"text"
    indexer = Indexer(db, search, store)

    await indexer._process([_row(1), _row(2), _row(3)])

    payload = search.index.return_value.add_documents.call_args[0][0]
    assert {d["id"] for d in payload} == {1, 3}
    db.mark_duplicates.assert_awaited_once_with([(2, 999)])
    db.mark_indexed.assert_awaited_once_with([1, 3])


async def test_content_truncated_to_50000_chars():
    indexer, db, search, store = _indexer(near_duplicate_of=None)
    store.get.return_value = ("x" * 60_000).encode()

    await indexer._process([_row(1)])

    payload = search.index.return_value.add_documents.call_args[0][0]
    assert len(payload[0]["content"]) == 50_000
