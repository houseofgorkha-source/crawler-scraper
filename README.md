# Web Crawler — V1

Broad-scope crawler. Medium scale (tens of millions of URLs) on one machine,
with an evolution path to distributed that does not require rewriting the core.

A Scraper -- structured, spec-driven field extraction -- is an equally
first-class peer capability, added on top of this architecture rather than
as a mode of the Crawler. See "Scraper" below; the diagram and decisions
in this section describe the Crawler exactly as originally verified.

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
        │              │ RENDER POOL      │  bounded,  │
        │              │ Playwright ≤4    │ cross-proc │
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
render pool is hard-capped — it is the intended bottleneck. Enforced via
Postgres session-scoped advisory locks rather than an in-process semaphore,
since Crawler and Scraper are independently runnable processes that must
share one real cap, not one each.

**Indexing is asynchronous.** A flag on the row, drained by a separate
process. Meilisearch being down produces a growing queue, not a stalled crawl.
Near-duplicates are suppressed at the index boundary — the document and its
graph edges are kept, only the search entry is skipped.

**Identify honestly.** Real User-Agent with a contact URL, `Crawl-Delay`
honored, 500ms floor regardless of what robots permits. Anonymous broad
crawlers get blocked by WAFs — this is a technical requirement, not etiquette.

## Explicitly deferred

Go workers · Kafka · Kubernetes · OpenSearch · ML classification · graph
ranking · multi-region · general browser automation (form submission,
sessions, interactive/multi-step flows). Each is addable without touching
the core, because stages communicate only through the contracts in
`crawler/contracts.py`.

## Scraper

A peer of the Crawler, not a mode of it: structured, spec-driven field
extraction, added on top of the architecture above without changing it.
The Crawler answers "what exists out there"; the Scraper answers "give me
these specific fields from these specific pages."

**Extraction**: CSS/XPath selectors against a JSON-shaped field schema
(`crawler/scrape_extract.py`), with nested/repeated extraction for
listing-style pages. Runs against the static or rendered DOM per spec
(`render_mode: auto | always | never`). No form submission, sessions, or
interactive flows — declarative, idempotent selectors only.

**Storage**: `scrape_specs` / `scrape_targets` / `scraped_records`, a
`claim_scrape_targets()` SQL function structurally identical to
`claim_urls()` (same `LATERAL` + `FOR UPDATE SKIP LOCKED` pattern), gated
on the *same* `domains.next_available_at`/robots fields as the Crawler —
one shared politeness clock, never a second one. `scraped_records` is
Postgres-only (no Meilisearch — structured field data isn't a full-text
search problem) and durable (no pruning, unlike `crawl_attempts`).

**Operating modes**: `crawl` and `scrape` are independently runnable
processes. Two optional feed rules connect them — `crawl --feed-scraper`
(Crawler → Scraper) and a spec's `feed_to_crawler` flag (Scraper →
Crawler) — both off by default, so every combination (either alone, both
together, either feeding the other) is just which processes are running
and which flags are set, not different code.

**Render pool**: shared across both processes via Postgres advisory
locks — see "Rendering is escalation, not default" above.

Verified against live infra: nested/repeated structured extraction from a
listing page, the Scraper → Crawler link-feed mechanism handing a
discovered link to the Crawler's own frontier, and a real external-site
run (`submit-scrape` against `https://www.amazon.in/` under a generic
title/meta-description spec) — real robots.txt fetch and redirect
handling, a real 200 response, and a correct `scraped_records` row,
fully isolated from the Crawler's `urls` frontier via the Scraper's own
`scrape_targets` queue. See `CLAUDE.md` for exactly what was and wasn't
proven by that run.

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
crawler/scrape_extract.py  Scraper: CSS/XPath + JSON-schema extraction
crawler/scrape_worker.py   Scraper: the loop (peer of worker.py)
docker-compose.yml     single-machine infra
```

## Running

```bash
docker compose up -d
pip install -r requirements.txt && playwright install chromium
python -m crawler.cli seed https://example.com
python -m crawler.cli crawl --workers 8
python -m crawler.cli index

# Scraper -- independently runnable, same infra
python -m crawler.cli spec add spec.json
python -m crawler.cli submit-scrape <spec-name> https://example.com/p/1
python -m crawler.cli scrape --workers 8
```

## Scale-out path

1. **Now** — one box, 8 async workers, ~50–100 pages/sec on static content
   *under a diverse domain mix*. Against a domain-concentrated backlog this
   does not hold — see "Real-load findings" below.
2. **Bottleneck: extraction CPU** — move extraction to its own process pool.
   Contract already separates it.
3. **Bottleneck: fetch concurrency** — replace `HttpFetcher` with Go workers
   reading the same `claim_urls()`. Nothing downstream changes.
4. **Bottleneck: Postgres write throughput** — introduce a real queue between
   fetch and extract. Only then is Kafka justified.

Don't jump to 2–4 without re-measuring — the one real-load run performed so
far points at neither CPU, fetch concurrency, nor Postgres writes.

## Real-load findings

An 8-worker run against a real, domain-concentrated backlog (the largest
single domains held thousands of pending URLs each — `linkedin.com`,
`iana.org`, `datatracker.ietf.org`, `icann.org` among them) surfaced two
things a synthetic/diverse-domain test wouldn't have:

- **A concurrency bug**, now fixed — see "eliminate deadlocks in concurrent
  add()/complete()" in `CLAUDE.md`. Lock-order inconsistency between `add()`
  and `complete()` caused `DeadlockDetectedError` on ~5% of completions (51
  observed); consistent lock ordering cut that to 6, and a bounded 3-attempt
  retry closed the residual Postgres first-insert race, reaching 0
  deadlocks. Regression-tested in `tests/integration/test_frontier.py`.
- **The actual throughput constraint is domain concentration, not CPU,
  fetch concurrency, or Postgres writes.** A longer 8-worker run sustained
  only ~0.54 pages/sec. A follow-up 150-second run against the same kind of
  backlog reproduced this directly: near-zero completions for the first
  ~60-80 seconds (robots.txt resolution, redirect chains, and occasional
  403 blocks on the handful of large concentrated domains dominating early
  claims), then acceleration to several pages/sec once workers reached
  smaller, faster domains — netting ~100 pages in 150s (~0.67 pages/sec
  average, back-loaded). The shared politeness clock
  (`domains.next_available_at`) is doing what it's designed to do; the
  problem is that a backlog dominated by a few huge domains gives workers
  little else to do while waiting on it. Widening the seeded domain mix,
  not any of the four scale-out stages above, is the next thing to try.

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
