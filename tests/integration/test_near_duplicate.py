"""
IndexerDB.find_near_duplicate() against real data -- this path has never
been exercised with an actual near-duplicate anywhere in this project
before (documented as a known gap in CLAUDE.md). Uses the real hamming()
function from extract.py, not a stub, so this also re-validates that
function's contract in the context it's actually used.
"""
from crawler.db import IndexerDB
from crawler.extract import hamming

BASE = 0x1234_5678_90AB_CDEF


async def _seed_url_with_simhash(conn, url: str, simhash: int) -> int:
    domain_id = await conn.fetchval(
        "INSERT INTO domains (host) VALUES ($1) ON CONFLICT (host) DO UPDATE SET host=EXCLUDED.host RETURNING id",
        "dup.example",
    )
    return await conn.fetchval(
        "INSERT INTO urls (domain_id, url, simhash) VALUES ($1,$2,$3) RETURNING id",
        domain_id, url, simhash,
    )


async def test_finds_near_duplicate_within_distance(db):
    near = BASE ^ 0b111  # flips 3 low bits -> hamming distance 3, same top byte
    assert hamming(BASE, near) == 3

    async with db.acquire() as conn:
        original_id = await _seed_url_with_simhash(conn, "https://dup.example/a", BASE)
        near_id = await _seed_url_with_simhash(conn, "https://dup.example/b", near)

    idb = IndexerDB(db)
    match = await idb.find_near_duplicate(near, near_id, max_distance=3, hamming_fn=hamming)
    assert match == original_id


async def test_dissimilar_content_is_not_flagged_as_duplicate(db):
    far = BASE ^ 0b1111  # 4 bit flips -> distance 4, exceeds max_distance=3
    assert hamming(BASE, far) == 4

    async with db.acquire() as conn:
        await _seed_url_with_simhash(conn, "https://dup.example/a", BASE)
        far_id = await _seed_url_with_simhash(conn, "https://dup.example/c", far)

    idb = IndexerDB(db)
    match = await idb.find_near_duplicate(far, far_id, max_distance=3, hamming_fn=hamming)
    assert match is None


async def test_never_matches_itself(db):
    async with db.acquire() as conn:
        url_id = await _seed_url_with_simhash(conn, "https://dup.example/a", BASE)

    idb = IndexerDB(db)
    match = await idb.find_near_duplicate(BASE, url_id, max_distance=3, hamming_fn=hamming)
    assert match is None  # excluded via `id != $1`, even though distance would be 0


async def test_ignores_urls_with_no_simhash_yet(db):
    async with db.acquire() as conn:
        domain_id = await conn.fetchval(
            "INSERT INTO domains (host) VALUES ($1) RETURNING id", "nodup.example",
        )
        # never-crawled url: simhash IS NULL
        await conn.execute(
            "INSERT INTO urls (domain_id, url) VALUES ($1, $2)",
            domain_id, "https://nodup.example/pending",
        )
        target_id = await _seed_url_with_simhash(conn, "https://nodup.example/done", BASE)

    idb = IndexerDB(db)
    match = await idb.find_near_duplicate(BASE, target_id, max_distance=3, hamming_fn=hamming)
    assert match is None  # the NULL-simhash row must never be treated as a candidate
