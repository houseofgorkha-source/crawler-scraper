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

Everything is written and syntax-checked (`python -m py_compile crawler/*.py`
passes). Two things are **not yet verified against live infrastructure**:

1. **`crawler/frontier.py` and `crawler/db.py`** — the asyncpg queries have
   never run against a real Postgres instance. Likely failure points:
   parameter binding, the `claim_urls()` SQL function's `DISTINCT ON` +
   `FOR UPDATE SKIP LOCKED` interaction, and the `ON CONFLICT (url_key)`
   dedup path in `frontier.add()` / `frontier.seed()`.
2. **The full pipeline end-to-end** — claim → fetch → extract → store →
   Postgres commit → index — has never run as one flow.

**Your first task should be:** bring up `docker compose up -d`, create the
MinIO bucket, seed one URL, run one worker, and trace it through the
pipeline by hand (check the `urls`, `domains`, `documents` tables after each
stage) until it works end-to-end. Fix bugs as you find them, but flag any
fix that changes behavior described in `README.md` rather than just
correcting a typo or query bug.

Only unit-tested so far, in isolation: `normalize.py` (URL canonicalization)
and the `simhash`/`hamming` functions in `extract.py`. Both behaved
correctly in manual testing but have no automated test suite yet.

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
crawler/frontier.py    Postgres/Redis Frontier implementation — UNTESTED
                        against live infra, see above
crawler/db.py          query layer for the indexer process — UNTESTED
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
