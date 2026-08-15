# CLAUDE.md — Project Instructions for Claude Code

This file is read automatically. Read it fully before making changes.

## What this is

A broad-scope web crawler, V1, with a Scraper as an equally first-class
peer capability (added after the V1 architecture below was already
verified — see "Scraper" further down). Medium scale now (tens of millions
of URLs), designed to evolve into distributed without a rewrite. Full
rationale is in `README.md` — read that too before touching architecture.

**The architecture is approved. Do not change it without explicit sign-off
from the user.** Specifically, do not introduce Go, Kafka, Kubernetes,
OpenSearch, ML classification, graph ranking, or general browser automation
(form submission, sessions, interactive/multi-step flows) — these were
deliberately deferred, not forgotten. If you think one is needed, say so
and ask first.

## Current state — read this before doing anything else

Verified against live infrastructure (Postgres, Redis, MinIO, Meilisearch
via `docker compose up -d`):

- **Full pipeline, end-to-end.** seed → `claim_urls()` → robots
  resolution/recheck → static fetch → extract → blob store → Postgres
  commit → link discovery → async indexing → Meilisearch search — all
  confirmed working as one flow.
- **Recursive cross-domain link discovery.** A seeded `example.com` URL was
  followed out to `iana.org`, `icann.org`, and beyond across multiple
  hosts, with each hop re-entering the pipeline above.
- **Playwright render escalation.** `needs_render()` correctly detected an
  empty app-shell root on a local SPA fixture, `PlaywrightRenderer`
  executed the page's JS, and the extracted text/links matched the
  post-render DOM, not the empty static shell.
- **Windows startup and clean shutdown.** `crawl` and `index` start
  without crashing, and `Ctrl+C` → `shutting_down` → workers stop →
  Postgres/Redis connections closed cleanly — confirmed manually.

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

Two more, platform-only, fixes:

- **Windows signal handling.** The default asyncio event loop on Windows
  doesn't implement `add_signal_handler` (`NotImplementedError`), so
  `crawl` and `index` crashed immediately on startup. `cli.py` now falls
  back to `signal.signal` there.
- **Redis cleanup on shutdown.** The Redis client created in
  `cmd_seed`/`cmd_crawl` was never explicitly closed, so its connections
  were garbage-collected after `asyncio.run()` had already torn down the
  event loop, producing a `RuntimeError: Event loop is closed` inside
  `AbstractConnection.__del__` on exit. Fixed by calling
  `await redis.aclose()` before `pool.close()`.

Not yet exercised:

- **Near-duplicate suppression.** `index.py`'s `find_near_duplicate` /
  `mark_duplicates` path — the documents indexed so far were all distinct,
  so the near-dup branch ran but never matched anything real.
- **Automated test suite.** `normalize.py` and the `simhash`/`hamming`
  functions in `extract.py` remain unit-tested in isolation only, no
  automated suite yet, though both have now also been exercised indirectly
  by the live pipeline runs above.

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
  render pool is hard-capped at `MAX_CONCURRENT_PAGES = 4` by default —
  this is an intentional bottleneck, not a bug to "fix" by raising it
  without measuring headroom first. Enforced via Postgres session-scoped
  advisory locks (see "Scraper" below for why), not an in-process
  semaphore — do not revert to `asyncio.Semaphore`, it stopped being a
  correct cap the moment a second process could hold a renderer too.
- **The domains politeness clock is shared, never duplicated.** Crawler and
  Scraper both gate their claim functions on the exact same
  `domains.next_available_at` / robots fields. If a future change gives
  Scraper (or any other subsystem) its own copy of this clock, the combined
  request rate against a domain can exceed `crawl_delay_ms` even though
  each subsystem individually respects it — this is the one thing that
  must never be split per-subsystem.
- **Indexing is asynchronous**, a separate process (`crawler/index.py`)
  draining `documents.index_state = 'pending'`. The crawl must never block
  on Meilisearch being slow or down.
- **Identify honestly.** Real `User-Agent` with a contact URL
  (`crawler/policy.py: USER_AGENT`), respect `Crawl-Delay`, 500ms floor
  regardless of what robots permits.

If you think any of these should change, explain the tradeoff and ask —
don't just refactor around them.

## Scraper

A peer of the Crawler, not a mode of it, added on top of the verified V1
architecture above. The Crawler answers "what exists out there" (broad
discovery, generic text, search). The Scraper answers "give me these
specific structured fields from these specific pages" (spec-driven,
CSS/XPath, durable structured records). Everything in the sections above —
`urls`/`domains`/`links`/`documents`/`crawl_attempts`, `claim_urls()`,
the Crawler's extraction/indexing path — is unchanged; the Scraper is
additive: `scrape_specs` / `scrape_targets` / `scraped_records`,
`claim_scrape_targets()`, `crawler/scrape_extract.py`,
`crawler/scrape_worker.py`.

**Extraction model.** A spec is CSS/XPath selectors against a JSON-shaped
field schema (`crawler/scrape_extract.py: FieldSpec`), supporting nested
and repeated extraction (`many: true` + nested `fields`, for listing/
grid pages — a flat single-record schema isn't enough for the majority of
real scrape targets). Extraction runs against either the static or the
rendered DOM per `render_mode` (`"auto"` reuses `fetch.needs_render()`;
`"always"`/`"never"` let an operator who knows the target skip the
heuristic). Extraction itself goes through `RecordExtractor`
(`crawler/contracts.py`), implemented by `HtmlRecordExtractor` and
injected into `ScrapeWorker` the same way `HtmlExtractor` is injected
into `CrawlWorker` (`extractor=None` → the default, swappable via the
constructor) — a real seam, not just a conceptual parallel to the
Crawler's. No form submission, sessions, or interactive/multi-step
flows — those are a different reliability class (stateful, replay-
sensitive) and are explicitly out of scope for this architecture, not
just deferred for later.

**Two correctness guarantees, enforced, not just intended:**
- `render_mode: "always"` never silently substitutes a different mode.
  If a spec demands rendering and no renderer is configured (e.g. `scrape
  --no-render`), the target is skipped explicitly
  (`reason="render_required_unavailable"`) — it does not silently fall
  back to a static fetch that would look like a normal success.
- `scrape_specs.is_active` is checked in two places, not one:
  `claim_scrape_targets()` excludes inactive specs' targets at the SQL
  level (so a deactivated spec's queue simply stops draining), and
  `ScrapeWorker` rechecks it again after claim, for the same reason
  robots is rechecked at fetch time even though the claim query already
  gated on it — the claim and the recheck can straddle a spec being
  deactivated in between.

**Verified against live infra**, in increasing order of scope:
`spec add` → `submit-scrape` → `scrape` produced correct nested/repeated
structured output from a local fixture (a product-listing page → an
array of `{title, price, url}` records); the `feed_to_crawler` link-field
mechanism correctly handed a discovered link to the Crawler's own
frontier (`urls`/`links`), including creating the scrape target's own
`urls` row on demand since a Scraper-only run may never have created one;
the reverse direction (`crawl --feed-scraper` enrolling a crawled URL
into a matching spec) was verified the same way; and `crawl`/`scrape` run
as two genuinely separate OS processes simultaneously, with the render
pool's cross-process cap confirmed by directly polling `pg_locks` for the
advisory-lock keys — held count never exceeded the configured
`--render-pages` value across either process.

**The five operating modes are configuration, not code paths**: which of
`crawl` / `scrape` processes are running, plus two independent opt-in
feed rules — `crawl --feed-scraper` (Crawler → Scraper: crawled URLs
matching an active spec's `host_pattern`/`path_regex` get enrolled) and a
spec's own `feed_to_crawler` flag (Scraper → Crawler: a spec's
`link_field` extractions get handed to `frontier.add()`). Both default
off, so plain `crawl` and a `scrape` fed only via `submit-scrape` behave
exactly as if the other subsystem didn't exist.

**Render pool is a genuinely cross-process resource, not a new
datastore.** `crawl` and `scrape` are independently runnable processes,
each capable of holding its own `PlaywrightRenderer`, so
`MAX_CONCURRENT_PAGES` can't be an in-process `asyncio.Semaphore` anymore
— that only ever bounded one process's pages. `render.py` now uses a
fixed set of Postgres session-scoped advisory locks (`pg_try_advisory_lock`,
one key per page slot) instead: no new table, no TTL/heartbeat/reaper
needed (a crashed holder's slot releases automatically when its connection
drops), and it works identically whether Crawler and Scraper share one
process or run as two — same code path either way. Caveat: this only
enforces a true global cap when every process is launched with the same
`--render-pages` value; a mismatched value effectively raises the ceiling
for the extra keys the larger config generates.

**Search and retention are deliberately asymmetric with the Crawler's.**
`scraped_records` does not go through Meilisearch — it's structured field
data (filter/query by field), not full-text search, and building a second
indexing system for it isn't justified until something actually needs it.
It also does not follow `crawl_attempts`' 30-day pruning — like
`documents`, it's the actual deliverable, kept indefinitely, latest-wins
on re-scrape (no pruning logic exists for it in V1).

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
crawler/scrape_extract.py  CSS/XPath + JSON-schema structured extraction
                        (Scraper) — verified against live infra
crawler/scrape_worker.py   the Scraper's per-task loop — verified
crawler/cli.py         entrypoint: seed / crawl / scrape / submit-scrape /
                        spec add / index / reap
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
python -m crawler.cli crawl --workers 8 [--no-render] [--render-pages N] [--feed-scraper]
python -m crawler.cli index
python -m crawler.cli reap        # normally cron, not manual

# Scraper -- independently runnable, same infra
python -m crawler.cli spec add spec.json
python -m crawler.cli submit-scrape <spec-name> https://example.com/p/1 ...
python -m crawler.cli scrape --workers 8 [--no-render] [--render-pages N]
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
