# CLAUDE.md — Project Instructions for Claude Code

This file is read automatically. Read it fully before making changes.

## What this is

A broad-scope web crawler, V1. Medium scale now (tens of millions of URLs),
designed to evolve into distributed without a rewrite. Full rationale is in
`README.md` — read that too before touching architecture.

**The architecture is approved. Do not change it without explicit sign-off
from the user.** Specifically, do not introduce Go, Kafka, Kubernetes,
OpenSearch, ML classification, or graph ranking — these were deliberately
deferred, not forgotten. If you think one is needed, say so and ask first.

## Current state — read this before doing anything else

The full pipeline has been run end-to-end against live infrastructure
(Postgres, Redis, MinIO, Meilisearch via `docker compose up -d`): seed →
`claim_urls()` → robots resolution/recheck → static fetch → extract → blob
store → Postgres commit → link discovery → recursive re-crawl → async
indexing → Meilisearch search all verified working, including cross-domain
link discovery (a seeded `example.com` URL was followed out to `iana.org`,
`icann.org`, and beyond). The Playwright render-escalation tier has also
been verified separately: `needs_render()` correctly detected an empty
app-shell root on a local SPA fixture, `PlaywrightRenderer` executed the
page's JS, and the extracted text/links matched the post-render DOM, not
the empty static shell.

Four bugs were found and fixed during this verification (see `frontier.py`,
`index.py`, `db/schema.sql`, `requirements.txt` for the fixes and their
inline comments):

1. **`claim_urls()` — `FOR UPDATE` + `DISTINCT ON`.** Postgres rejects the
   two combined in one query (`FOR UPDATE is not allowed with DISTINCT
   clause`) — this was the exact interaction flagged as a likely failure
   point, and it was. Fixed by splitting into a plain `DISTINCT` to pick
   eligible domains, then a `LATERAL` join per domain doing the priority
   ordered `FOR UPDATE SKIP LOCKED LIMIT 1` pick. Same one-url-per-domain,
   skip-locked semantics as before, just in two steps instead of one.
2. **`simhash` int64 overflow.** `extract.simhash()` returns an *unsigned*
   64-bit value; Postgres `bigint` is signed, so values ≥2^63 overflowed on
   bind in `frontier.complete()`. Fixed by converting to the equivalent
   two's-complement signed value at the write boundary — hamming distance
   and XOR are unaffected since Python's bitwise ops treat negative ints as
   infinite two's-complement.
3. **Silent brotli corruption.** `fetch.py` advertises `Accept-Encoding: br`
   but `brotli`/`brotlicffi` wasn't in `requirements.txt`, so httpx silently
   returned undecoded compressed bytes as if the fetch had succeeded (no
   exception, no error outcome). Added `brotli==1.1.0` to requirements.
4. **Indexer settings type mismatch.** `index.py`'s `SETTINGS` was a plain
   camelCase dict; `meilisearch-python-sdk==3.1.0`'s `update_settings()`
   expects a `MeilisearchSettings` model instance with snake_case fields.
   Fixed by constructing the model instead of a dict.

A fifth, platform-only issue was also fixed: on Windows, the default
asyncio event loop doesn't implement `add_signal_handler`
(`NotImplementedError`), so `crawl` and `index` crashed immediately on
startup. `cli.py` now falls back to `signal.signal` there. Clean shutdown
(`Ctrl+C` → `shutting_down` → workers stop → Postgres/Redis connections
closed) has been confirmed manually. A related resource-cleanup bug was
also fixed: the Redis client created in `cmd_seed`/`cmd_crawl` was never
explicitly closed, so its connections were garbage-collected after
`asyncio.run()` had already torn down the event loop, producing a
`RuntimeError: Event loop is closed` inside `AbstractConnection.__del__`
on exit. Fixed by calling `await redis.aclose()` before `pool.close()`.

Not yet exercised: the indexer's actual near-duplicate *suppression* path
in `index.py` (`find_near_duplicate` / `mark_duplicates`) — the documents
indexed so far were all distinct, so the near-dup branch ran but never
matched anything real. `normalize.py` and the `simhash`/`hamming` functions
in `extract.py` remain unit-tested in isolation only, no automated suite
yet, though both have now also been exercised indirectly by the live
pipeline runs above.

## Non-negotiable design decisions (do not "simplify" these away)

- **Postgres is the source of truth. Redis is a cache only.** The frontier
  must be fully rebuildable from Postgres if Redis is flushed. Do not make
  anything durable depend on Redis.
- **No Bloom filter.** Dedup is `UNIQUE(sha256(normalized_url))` in Postgres.
  This was an explicit choice over a Bloom filter — don't add one to "help
  performance" without measuring an actual bottleneck first.
- **Robots.txt is enforced twice**: once at schedule time (gates the claim
  query via `domains.next_available_at`), once at fetch time
  (`policy.check_allowed()` immediately before the request). Do not remove
  either check — cached robots go stale and a stale *allow* gets the whole
  crawler blocked.
- **Leases, not permanent assignment.** Workers claim URLs with an expiry;
  a reaper (`reap_expired_leases()`, run every 60s) returns unrenewed leases
  to the pool. Never remove the reaper or the lease expiry.
- **`crawl_attempts` is append-only history, NOT hot-path state.** Current
  status lives denormalized on the `urls` row. `crawl_attempts` is monthly-
  partitioned and pruned after 30 days (`ops/partitions.sql`, meant to run
  from cron). Don't query `crawl_attempts` for anything on the fetch path.
- **The `links` table is written from day one**, even though ranking is
  deferred. Edges are only observable at parse time — if this table stops
  being written, recovering the graph later means re-crawling everything.
  Do not remove link-writing to "simplify" extraction.
- **Rendering (Playwright) is escalation, not default.** Static HTTP fetch
  always happens first unless `domains.js_required` is already true. The
  render pool is a hard-capped semaphore (`MAX_CONCURRENT_PAGES = 4` by
  default) — this is an intentional bottleneck, not a bug to "fix" by
  raising it without measuring headroom first.
- **Indexing is asynchronous**, a separate process (`crawler/index.py`)
  draining `documents.index_state = 'pending'`. The crawl must never block
  on Meilisearch being slow or down.
- **Identify honestly.** Real `User-Agent` with a contact URL
  (`crawler/policy.py: USER_AGENT`), respect `Crawl-Delay`, 500ms floor
  regardless of what robots permits.

If you think any of these should change, explain the tradeoff and ask —
don't just refactor around them.

## Layout

```
db/schema.sql          tables, claim_urls(), reap_expired_leases()
ops/partitions.sql     monthly partition create + 30-day retention drop
crawler/contracts.py   stage interfaces (Frontier, Fetcher, Renderer,
                        Extractor, BlobStore, SearchIndex protocols) —
                        these are the swap points; keep implementations
                        conforming to them
crawler/normalize.py   URL canonicalization — the correctness foundation
                        of dedup. Tested and working.
crawler/policy.py      robots.txt parsing + crawl delay
crawler/fetch.py       tier 1 HTTP fetch + JS-needed heuristic
crawler/render.py      tier 2 Playwright pool, bounded semaphore
crawler/extract.py     content extraction, link discovery, sha256 +
                        simhash fingerprinting. Tested and working.
crawler/worker.py      the per-task loop: claim → robots recheck → fetch
                        → escalate? → extract → store → commit → enqueue
crawler/frontier.py    Postgres/Redis Frontier implementation — verified
                        end-to-end against live infra, see above
crawler/db.py          query layer for the indexer process — verified
                        against live infra (near-dup suppression path not
                        yet exercised with a real duplicate, see above)
crawler/index.py       async Meilisearch drain, near-dup suppression
crawler/store.py       object storage (zstd-compressed, MinIO/S3)
crawler/cli.py         entrypoint: seed / crawl / index / reap
docker-compose.yml     postgres, redis, minio, meilisearch — single machine
requirements.txt       pip deps
```

## Environment variables (see `crawler/cli.py` for defaults)

- `CRAWLER_PG_DSN` — default `postgresql://postgres:crawler@localhost/crawler`
- `CRAWLER_REDIS_URL` — default `redis://localhost:6379/0`
- `CRAWLER_MEILI_URL` / `CRAWLER_MEILI_KEY`
- `CRAWLER_S3_ENDPOINT` / `CRAWLER_S3_KEY` / `CRAWLER_S3_SECRET` / `CRAWLER_S3_BUCKET`

## Commands

```bash
docker compose up -d
python -m crawler.cli seed https://example.com
python -m crawler.cli crawl --workers 8 [--no-render] [--render-pages N]
python -m crawler.cli index
python -m crawler.cli reap        # normally cron, not manual
```

## Scale-out path (do not jump ahead of the current bottleneck)

1. Now: one box, N async workers.
2. Extraction CPU-bound → move extraction to its own process pool.
3. Fetch concurrency-bound → replace `HttpFetcher` with Go workers reading
   the same `claim_urls()` — the SQL contract doesn't care what language
   calls it.
4. Postgres write-bound → introduce a real queue between fetch and extract.
   Only then is Kafka justified.

Don't pre-optimize for a stage you haven't hit yet.
