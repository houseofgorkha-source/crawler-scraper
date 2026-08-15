# Web Crawler — V1

Broad-scope crawler. Medium scale (tens of millions of URLs) on one machine,
with an evolution path to distributed that does not require rewriting the core.

## Architecture

```
                        ┌──────────────┐
                        │  Seed URLs   │
                        └──────┬───────┘
                               ↓
        ┌──────────────────────────────────────────────┐
        │             URL FRONTIER                     │
        │                                              │
        │  Postgres  = source of truth (urls, domains) │
        │  Redis     = hot working set (cache only)    │
        │                                              │
        │  • dedup: UNIQUE(sha256(normalized_url))     │
        │  • policy: robots + crawl-delay per domain   │
        │  • leases: claim with expiry, auto-reaped    │
        └──────────────────────┬───────────────────────┘
                               ↓ claim_urls()
        ┌──────────────────────────────────────────────┐
        │            CRAWL WORKERS (asyncio)           │
        │                                              │
        │   robots recheck → static GET (httpx)        │
        │                        ↓                     │
        │              needs_render()?  ──no──→ done   │
        │                        ↓ yes                 │
        │              ┌──────────────────┐            │
        │              │ RENDER POOL      │  bounded   │
        │              │ Playwright ≤4    │  semaphore │
        │              └──────────────────┘            │
        └──────────────────────┬───────────────────────┘
                               ↓
        ┌──────────────────────────────────────────────┐
        │                 EXTRACTOR                    │
        │  main content · metadata · links             │
        │  sha256 (exact) + simhash (near) fingerprint │
        └──────────┬────────────────────┬──────────────┘
                   ↓                    ↓
        ┌────────────────────┐   ┌──────────────────┐
        │  OBJECT STORAGE    │   │    POSTGRES      │
        │  raw html (zstd)   │   │  urls · domains  │
        │  extracted text    │   │  documents       │
        │                    │   │  links (edges)   │
        │                    │   │  crawl_attempts  │
        └────────────────────┘   └────────┬─────────┘
                                          ↓ index_state='pending'
                              ┌───────────────────────┐
                              │   INDEXER (separate)  │
                              │   near-dup suppress   │
                              │   → Meilisearch       │
                              └───────────────────────┘
```

## The decisions that matter

**Postgres is the source of truth; Redis is a cache.** The frontier must be
fully rebuildable from Postgres after a total Redis loss. No Bloom filter in
V1 — a unique index on `sha256(normalized_url)` gives exact, durable dedup and
will not be the bottleneck at this scale. A Bloom filter is a memory
optimization to add *when measurement demands it*, and it introduces a
silent-data-loss path if treated as authoritative.

**Robots is enforced twice.** At schedule time via `domains.next_available_at`
gating the claim query, and again at fetch time. This is not redundant: cached
robots go stale, and a stale *allow* is the failure that gets you blocked at
the CDN layer. Policy fails closed — no resolved robots means no fetch.

**Leases, not assignments.** A worker claims a URL with an expiry
(`FOR UPDATE SKIP LOCKED` + `lease_expires_at`). A reaper returns unrenewed
leases every 60s. Without this, every crashed worker permanently strands its
in-flight URLs and the frontier slowly leaks.

**State is denormalized; history is partitioned.** Current status lives on the
`urls` row so the hot path never joins. `crawl_attempts` is append-only,
monthly-partitioned, dropped after 30 days. A row-per-attempt table with
unbounded retention becomes the largest table in the system within weeks.

**Link edges are recorded from day one; ranking is not.** Edges are observable
only at parse time — backfilling them later costs a full re-crawl of the
corpus. The `links` table is nearly free to write and is the single
highest-regret omission in a design like this. Ranking on top of it stays out
of V1.

**Rendering is escalation, not default.** Static fetch first, always. Escalate
only on evidence (empty app-shell root, implausibly low text). Record that
evidence on the domain so known SPAs stop paying the wasted round-trip. The
render pool is a hard-capped semaphore — it is the intended bottleneck.

**Indexing is asynchronous.** A flag on the row, drained by a separate
process. Meilisearch being down produces a growing queue, not a stalled crawl.
Near-duplicates are suppressed at the index boundary — the document and its
graph edges are kept, only the search entry is skipped.

**Identify honestly.** Real User-Agent with a contact URL, `Crawl-Delay`
honored, 500ms floor regardless of what robots permits. Anonymous broad
crawlers get blocked by WAFs — this is a technical requirement, not etiquette.

## Explicitly deferred

Go workers · Kafka · Kubernetes · OpenSearch · ML classification · graph
ranking · multi-region. Each is addable without touching the core, because
stages communicate only through the contracts in `crawler/contracts.py`.

## Layout

```
db/schema.sql          tables, claim_urls(), reap_expired_leases()
ops/partitions.sql     monthly partition create + retention drop
crawler/contracts.py   stage interfaces — the swap points
crawler/normalize.py   URL canonicalization (dedup correctness)
crawler/policy.py      robots.txt + crawl delay
crawler/fetch.py       tier 1 HTTP + escalation heuristic
crawler/render.py      tier 2 Playwright pool
crawler/extract.py     content, links, sha256 + simhash
crawler/worker.py      the loop
crawler/index.py       async Meilisearch drain
crawler/store.py       object storage (zstd)
docker-compose.yml     single-machine infra
```

## Running

```bash
docker compose up -d
pip install -r requirements.txt && playwright install chromium
python -m crawler.cli seed https://example.com
python -m crawler.cli crawl --workers 8
python -m crawler.cli index
```

## Scale-out path

1. **Now** — one box, 8 async workers, ~50–100 pages/sec on static content.
2. **Bottleneck: extraction CPU** — move extraction to its own process pool.
   Contract already separates it.
3. **Bottleneck: fetch concurrency** — replace `HttpFetcher` with Go workers
   reading the same `claim_urls()`. Nothing downstream changes.
4. **Bottleneck: Postgres write throughput** — introduce a real queue between
   fetch and extract. Only then is Kafka justified.

## Verified

The full pipeline — seed → `claim_urls()` → robots resolution/recheck →
static fetch → extract → blob store → Postgres commit → link discovery →
recursive re-crawl → async indexing → Meilisearch search — has been run
end-to-end against live Postgres, Redis, MinIO, and Meilisearch, including
cross-domain link discovery. The Playwright render-escalation tier
(`needs_render()` → `PlaywrightRenderer` → extraction of the post-render
DOM) has been verified separately against a local SPA fixture. Four bugs
turned up in the process — a `claim_urls()` `FOR UPDATE`/`DISTINCT ON`
conflict, a `simhash` int64 overflow, a missing `brotli` dependency causing
silent response corruption, and an indexer settings type mismatch — all
fixed; see `CLAUDE.md` for details and `crawler/frontier.py`,
`crawler/index.py`, `db/schema.sql`, and `requirements.txt` for the fixes
themselves. Not yet exercised: the indexer's actual near-duplicate
suppression path (no real duplicate has been indexed yet to trigger it).
