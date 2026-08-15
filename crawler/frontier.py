"""
Frontier implementation: Postgres is authoritative, Redis caches only what
is cheap to lose (resolved robots policy, per-host coordination hints).

Every method here maps directly to a claim in the README. If you change the
storage engine underneath, this is the only file that should need to change —
that's the point of the Frontier protocol in contracts.py.
"""
from __future__ import annotations

import json
import logging
from datetime import datetime, timezone

import asyncpg
import redis.asyncio as aioredis

from .contracts import (
    CrawlTask, DiscoveredLink, ExtractedDoc, FetchOutcome, FetchResult,
    ScrapedRecord, ScrapeTask,
)
from .normalize import normalize, registrable_host
from .policy import DomainPolicy, parse_robots, DEFAULT_CRAWL_DELAY_MS
from .scrape_extract import ScrapeSpec, spec_from_row

log = logging.getLogger(__name__)

ROBOTS_CACHE_TTL = 3600          # Redis cache of a Postgres-durable fact; short is fine
MAX_FAILURES_DEFAULT = 5


class PostgresFrontier:
    def __init__(self, pool: asyncpg.Pool, redis: aioredis.Redis,
                 robots_fetcher=None):
        self.pool = pool
        self.redis = redis
        # Injected to avoid a hard dependency on HttpFetcher for a text file;
        # tests can pass a fake.
        self._robots_fetcher = robots_fetcher

    # ------------------------------------------------------------------ #
    # Claiming
    # ------------------------------------------------------------------ #
    async def claim(self, worker_id: str, batch: int, lease_s: int) -> list[CrawlTask]:
        async with self.pool.acquire() as conn:
            rows = await conn.fetch(
                "SELECT * FROM claim_urls($1, $2, $3)", worker_id, batch, lease_s
            )
        return [
            CrawlTask(
                url_id=r["url_id"], url=r["url"], host=r["host"], depth=r["depth"],
                etag=r["etag"], last_modified=r["last_modified"],
                js_required=r["js_required"],
            )
            for r in rows
        ]

    async def renew(self, url_id: int, worker_id: str, lease_s: int) -> bool:
        async with self.pool.acquire() as conn:
            result = await conn.execute(
                """UPDATE urls SET lease_expires_at = now() + make_interval(secs => $1)
                   WHERE id = $2 AND lease_owner = $3 AND status = 'leased'""",
                lease_s, url_id, worker_id,
            )
        return result.endswith("1")

    # ------------------------------------------------------------------ #
    # Policy — cached in Redis, durable in Postgres, re-resolved on staleness
    # ------------------------------------------------------------------ #
    async def policy_for(self, host: str) -> DomainPolicy:
        # The Redis entry is only a cheap staleness/short-circuit signal
        # (is_crawlable, delay) for logging and metrics. check_allowed()
        # needs the actual robots rule set, which only Postgres carries, so
        # every call still resolves a real DomainPolicy with a parser.
        # This keeps the cache honest: it can never be mistaken for the
        # authoritative answer used to gate a fetch.
        return await self._load_from_db_or_refresh(host)

    async def _load_from_db_or_refresh(self, host: str) -> DomainPolicy:
        async with self.pool.acquire() as conn:
            row = await conn.fetchrow(
                """SELECT robots_body, robots_status, robots_fetched_at, crawl_delay_ms,
                          is_crawlable
                   FROM domains WHERE host = $1""", host,
            )
        if row and row["robots_fetched_at"]:
            age_ok = (datetime.now(timezone.utc) - row["robots_fetched_at"]).total_seconds() < ROBOTS_CACHE_TTL * 24
            if age_ok:
                policy = parse_robots(host, row["robots_body"], row["robots_status"] or 200)
                await self._cache_policy(policy)
                return policy
        return await self.refresh_robots(host)

    async def refresh_robots(self, host: str) -> DomainPolicy:
        if self._robots_fetcher is None:
            raise RuntimeError("no robots_fetcher configured on frontier")

        body, status = await self._robots_fetcher(host)
        policy = parse_robots(host, body, status)

        async with self.pool.acquire() as conn:
            await conn.execute(
                """INSERT INTO domains (host, robots_body, robots_status,
                       robots_fetched_at, robots_expires_at, crawl_delay_ms, is_crawlable)
                   VALUES ($1, $2, $3, now(), now() + interval '24 hours', $4, $5)
                   ON CONFLICT (host) DO UPDATE SET
                       robots_body = EXCLUDED.robots_body,
                       robots_status = EXCLUDED.robots_status,
                       robots_fetched_at = now(),
                       robots_expires_at = now() + interval '24 hours',
                       crawl_delay_ms = EXCLUDED.crawl_delay_ms,
                       is_crawlable = EXCLUDED.is_crawlable""",
                host, body, status, policy.crawl_delay_ms, policy.is_crawlable,
            )
        await self._cache_policy(policy)
        return policy

    async def _cache_policy(self, policy: DomainPolicy) -> None:
        await self.redis.set(
            f"robots:{policy.host}",
            json.dumps({
                "is_crawlable": policy.is_crawlable,
                "crawl_delay_ms": policy.crawl_delay_ms,
                "fetched_at": policy.fetched_at.isoformat() if policy.fetched_at else None,
            }),
            ex=ROBOTS_CACHE_TTL,
        )

    async def mark_js_required(self, host: str) -> None:
        async with self.pool.acquire() as conn:
            await conn.execute(
                """UPDATE domains SET js_evidence_count = js_evidence_count + 1,
                       js_required = (js_evidence_count + 1 >= 3)
                   WHERE host = $1""", host,
            )

    # ------------------------------------------------------------------ #
    # Outcomes
    # ------------------------------------------------------------------ #
    async def record_attempt(self, result: FetchResult, worker_id: str) -> None:
        async with self.pool.acquire() as conn:
            await conn.execute(
                """INSERT INTO crawl_attempts
                       (url_id, duration_ms, status_code, render_mode, bytes,
                        error_class, error_detail, worker_id)
                   VALUES ($1,$2,$3,$4,$5,$6,$7,$8)""",
                result.task.url_id, result.duration_ms, result.status_code,
                result.render_mode.value if result.render_mode else None,
                len(result.body) if result.body else None,
                result.error_class, result.error_detail, worker_id,
            )

    async def complete(self, result: FetchResult, doc: ExtractedDoc,
                       raw_key: str, text_key: str) -> None:
        next_crawl = "now() + interval '7 days'"
        # simhash() returns an unsigned 64-bit value; Postgres bigint is
        # signed int64, so values >= 2**63 overflow on bind. Store the
        # equivalent two's-complement signed value -- bitwise ops (XOR,
        # hamming distance) on the signed form give identical results,
        # since Python's bitwise ops treat negative ints as infinite
        # two's-complement.
        simhash_signed = doc.simhash - (1 << 64) if doc.simhash >= (1 << 63) else doc.simhash
        async with self.pool.acquire() as conn:
            async with conn.transaction():
                await conn.execute(
                    f"""UPDATE urls SET
                            status = 'done', lease_owner = NULL, lease_expires_at = NULL,
                            last_crawled_at = now(), next_crawl_at = {next_crawl},
                            last_status_code = $2, consecutive_failures = 0,
                            etag = $3, last_modified = $4,
                            content_sha256 = $5, simhash = $6, render_mode = $7
                        WHERE id = $1""",
                    result.task.url_id, result.status_code, result.etag,
                    result.last_modified, doc.content_sha256, simhash_signed,
                    doc.render_mode.value,
                )
                await conn.execute(
                    """INSERT INTO documents
                           (url_id, title, description, lang, word_count,
                            raw_key, text_key, index_state, updated_at)
                       VALUES ($1,$2,$3,$4,$5,$6,$7,'pending', now())
                       ON CONFLICT (url_id) DO UPDATE SET
                           title = EXCLUDED.title, description = EXCLUDED.description,
                           lang = EXCLUDED.lang, word_count = EXCLUDED.word_count,
                           raw_key = EXCLUDED.raw_key, text_key = EXCLUDED.text_key,
                           index_state = 'pending', updated_at = now()""",
                    result.task.url_id, doc.title, doc.description, doc.lang,
                    doc.word_count, raw_key, text_key,
                )
                await conn.execute(
                    "UPDATE domains SET pages_crawled = pages_crawled + 1 WHERE host = $1",
                    result.task.host,
                )

    async def reschedule(self, task: CrawlTask, unchanged: bool) -> None:
        async with self.pool.acquire() as conn:
            await conn.execute(
                """UPDATE urls SET status = 'done', lease_owner = NULL,
                       lease_expires_at = NULL, last_crawled_at = now(),
                       next_crawl_at = now() + interval '7 days', consecutive_failures = 0
                   WHERE id = $1""", task.url_id,
            )

    async def skip(self, task: CrawlTask, reason: str) -> None:
        async with self.pool.acquire() as conn:
            await conn.execute(
                """UPDATE urls SET status = 'skipped', lease_owner = NULL,
                       lease_expires_at = NULL WHERE id = $1""", task.url_id,
            )
        log.info("skipped url_id=%s reason=%s", task.url_id, reason)

    async def fail(self, result: FetchResult, max_failures: int = MAX_FAILURES_DEFAULT) -> None:
        async with self.pool.acquire() as conn:
            row = await conn.fetchrow(
                """UPDATE urls SET
                       consecutive_failures = consecutive_failures + 1,
                       last_status_code = $2,
                       status = CASE WHEN consecutive_failures + 1 >= $3
                                     THEN 'failed'::url_status ELSE 'pending'::url_status END,
                       lease_owner = NULL, lease_expires_at = NULL,
                       next_crawl_at = now() + (make_interval(mins => 1) *
                                                 power(2, least(consecutive_failures, 6)))
                   WHERE id = $1
                   RETURNING consecutive_failures, status""",
                result.task.url_id, result.status_code, max_failures,
            )
            await conn.execute(
                "UPDATE domains SET error_count = error_count + 1 WHERE host = $1",
                result.task.host,
            )
        if row and row["status"] == "failed":
            log.warning("url_id=%s permanently failed after %s attempts",
                       result.task.url_id, row["consecutive_failures"])

    # ------------------------------------------------------------------ #
    # Discovery
    # ------------------------------------------------------------------ #
    async def add(self, links: list[DiscoveredLink], from_url_id: int, depth: int) -> int:
        inserted = 0
        async with self.pool.acquire() as conn:
            async with conn.transaction():
                for link in links:
                    host = registrable_host(link.url)
                    if not host:
                        continue

                    domain_id = await conn.fetchval(
                        """INSERT INTO domains (host) VALUES ($1)
                           ON CONFLICT (host) DO UPDATE SET host = EXCLUDED.host
                           RETURNING id""", host,
                    )

                    url_id = await conn.fetchval(
                        """INSERT INTO urls (domain_id, url, depth)
                           VALUES ($1, $2, $3)
                           ON CONFLICT (url_key) DO NOTHING
                           RETURNING id""", domain_id, link.url, depth,
                    )
                    if url_id is not None:
                        inserted += 1
                    else:
                        url_id = await conn.fetchval(
                            "SELECT id FROM urls WHERE url_key = digest($1, 'sha256')",
                            link.url,
                        )

                    if url_id is not None:
                        await conn.execute(
                            """INSERT INTO links (from_url_id, to_url_id, anchor_text, rel)
                               VALUES ($1, $2, $3, $4)
                               ON CONFLICT (from_url_id, to_url_id) DO NOTHING""",
                            from_url_id, url_id, link.anchor_text, link.rel,
                        )

                    await conn.execute(
                        "UPDATE domains SET pages_discovered = pages_discovered + 1 WHERE id = $1",
                        domain_id,
                    )
        return inserted

    async def seed(self, urls: list[str]) -> int:
        links = []
        for u in urls:
            n = normalize(u)
            if n:
                links.append(DiscoveredLink(url=n))
        # Seeds have no parent page; use a sentinel from_url_id of 0 is invalid
        # (FK), so seeds go through a dedicated insert instead of add().
        inserted = 0
        async with self.pool.acquire() as conn:
            async with conn.transaction():
                for link in links:
                    host = registrable_host(link.url)
                    if not host:
                        continue
                    domain_id = await conn.fetchval(
                        """INSERT INTO domains (host) VALUES ($1)
                           ON CONFLICT (host) DO UPDATE SET host = EXCLUDED.host
                           RETURNING id""", host,
                    )
                    result = await conn.fetchval(
                        """INSERT INTO urls (domain_id, url, depth, priority)
                           VALUES ($1, $2, 0, 1000)
                           ON CONFLICT (url_key) DO NOTHING RETURNING id""",
                        domain_id, link.url,
                    )
                    if result is not None:
                        inserted += 1
        return inserted

    # ------------------------------------------------------------------ #
    # Scraper -- a peer queue, not a mode of the Crawler above. Shares the
    # same pool/redis/robots machinery; everything below reads/writes
    # scrape_specs / scrape_targets / scraped_records instead of
    # urls / documents, and claim_scrape_targets() gates on the exact same
    # domains.next_available_at clock claim_urls() does.
    # ------------------------------------------------------------------ #
    async def claim_scrape(self, worker_id: str, batch: int, lease_s: int) -> list[ScrapeTask]:
        async with self.pool.acquire() as conn:
            rows = await conn.fetch(
                "SELECT * FROM claim_scrape_targets($1, $2, $3)", worker_id, batch, lease_s
            )
            if not rows:
                return []
            spec_rows = await conn.fetch(
                "SELECT id, render_mode FROM scrape_specs WHERE id = ANY($1::bigint[])",
                list({r["spec_id"] for r in rows}),
            )
        render_mode_by_spec = {r["id"]: r["render_mode"] for r in spec_rows}
        return [
            ScrapeTask(
                target_id=r["target_id"], url=r["url"], host=r["host"], spec_id=r["spec_id"],
                render_mode=render_mode_by_spec.get(r["spec_id"], "auto"),
                etag=r["etag"], last_modified=r["last_modified"],
            )
            for r in rows
        ]

    async def get_scrape_spec(self, spec_id: int) -> ScrapeSpec | None:
        async with self.pool.acquire() as conn:
            row = await conn.fetchrow("SELECT * FROM scrape_specs WHERE id = $1", spec_id)
        if row is None:
            return None
        row = dict(row)
        row["fields"] = json.loads(row["fields"]) if isinstance(row["fields"], str) else row["fields"]
        return spec_from_row(row)

    async def skip_scrape(self, task: ScrapeTask, reason: str) -> None:
        async with self.pool.acquire() as conn:
            await conn.execute(
                """UPDATE scrape_targets SET status = 'skipped', lease_owner = NULL,
                       lease_expires_at = NULL WHERE id = $1""", task.target_id,
            )
        log.info("scrape skipped target_id=%s reason=%s", task.target_id, reason)

    async def reschedule_scrape(self, task: ScrapeTask) -> None:
        async with self.pool.acquire() as conn:
            await conn.execute(
                """UPDATE scrape_targets SET status = 'done', lease_owner = NULL,
                       lease_expires_at = NULL, next_attempt_at = now() + interval '7 days',
                       consecutive_failures = 0
                   WHERE id = $1""", task.target_id,
            )

    async def fail_scrape(self, task: ScrapeTask, result: FetchResult,
                          max_failures: int = MAX_FAILURES_DEFAULT) -> None:
        async with self.pool.acquire() as conn:
            row = await conn.fetchrow(
                """UPDATE scrape_targets SET
                       consecutive_failures = consecutive_failures + 1,
                       last_status_code = $2,
                       status = CASE WHEN consecutive_failures + 1 >= $3
                                     THEN 'failed'::scrape_status ELSE 'pending'::scrape_status END,
                       lease_owner = NULL, lease_expires_at = NULL,
                       next_attempt_at = now() + (make_interval(mins => 1) *
                                                   power(2, least(consecutive_failures, 6)))
                   WHERE id = $1
                   RETURNING consecutive_failures, status""",
                task.target_id, result.status_code, max_failures,
            )
        if row and row["status"] == "failed":
            log.warning("target_id=%s permanently failed after %s attempts",
                       task.target_id, row["consecutive_failures"])

    async def complete_scrape(self, task: ScrapeTask, result: FetchResult,
                              record: ScrapedRecord, raw_key: str | None) -> None:
        async with self.pool.acquire() as conn:
            async with conn.transaction():
                await conn.execute(
                    """UPDATE scrape_targets SET
                            status = 'done', lease_owner = NULL, lease_expires_at = NULL,
                            last_attempted_at = now(), next_attempt_at = now() + interval '7 days',
                            last_status_code = $2, consecutive_failures = 0,
                            etag = $3, last_modified = $4
                        WHERE id = $1""",
                    task.target_id, result.status_code, result.etag, result.last_modified,
                )
                await conn.execute(
                    """INSERT INTO scraped_records (target_id, spec_id, data, raw_key, updated_at)
                       VALUES ($1, $2, $3, $4, now())
                       ON CONFLICT (target_id) DO UPDATE SET
                           data = EXCLUDED.data, raw_key = EXCLUDED.raw_key,
                           extracted_at = now(), updated_at = now()""",
                    task.target_id, task.spec_id, json.dumps(record.data), raw_key,
                )

    async def register_spec(self, name: str, fields: list[dict], version: int = 1,
                            render_mode: str = "auto", link_field: str | None = None,
                            feed_to_crawler: bool = False, feed_from_crawler: bool = False,
                            host_pattern: str | None = None, path_regex: str | None = None) -> int:
        async with self.pool.acquire() as conn:
            spec_id = await conn.fetchval(
                """INSERT INTO scrape_specs
                       (name, version, fields, render_mode, link_field,
                        feed_to_crawler, feed_from_crawler, host_pattern, path_regex)
                   VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9)
                   ON CONFLICT (name, version) DO UPDATE SET
                       fields = EXCLUDED.fields, render_mode = EXCLUDED.render_mode,
                       link_field = EXCLUDED.link_field,
                       feed_to_crawler = EXCLUDED.feed_to_crawler,
                       feed_from_crawler = EXCLUDED.feed_from_crawler,
                       host_pattern = EXCLUDED.host_pattern, path_regex = EXCLUDED.path_regex
                   RETURNING id""",
                name, version, json.dumps(fields), render_mode, link_field,
                feed_to_crawler, feed_from_crawler, host_pattern, path_regex,
            )
        return spec_id

    async def submit_scrape_targets(self, spec_id: int, urls: list[str]) -> int:
        inserted = 0
        async with self.pool.acquire() as conn:
            async with conn.transaction():
                for raw_url in urls:
                    n = normalize(raw_url)
                    if not n:
                        continue
                    host = registrable_host(n)
                    if not host:
                        continue
                    domain_id = await conn.fetchval(
                        """INSERT INTO domains (host) VALUES ($1)
                           ON CONFLICT (host) DO UPDATE SET host = EXCLUDED.host
                           RETURNING id""", host,
                    )
                    result = await conn.fetchval(
                        """INSERT INTO scrape_targets (spec_id, domain_id, url)
                           VALUES ($1, $2, $3)
                           ON CONFLICT (spec_id, url_key) DO NOTHING RETURNING id""",
                        spec_id, domain_id, n,
                    )
                    if result is not None:
                        inserted += 1
        return inserted

    async def enroll_scrape_targets(self, url: str, host: str) -> int:
        """Crawler -> Scraper feed: enroll `url` into every active spec whose
        scope matches it. Called by CrawlWorker only when explicitly opted
        in (--feed-scraper); a no-op set of specs means this is a no-op."""
        async with self.pool.acquire() as conn:
            async with conn.transaction():
                domain_id = await conn.fetchval("SELECT id FROM domains WHERE host = $1", host)
                if domain_id is None:
                    return 0
                specs = await conn.fetch(
                    """SELECT id FROM scrape_specs
                       WHERE is_active AND feed_from_crawler
                         AND (host_pattern IS NULL OR host_pattern = $1)
                         AND (path_regex IS NULL OR $2 ~ path_regex)""",
                    host, url,
                )
                n = 0
                for row in specs:
                    inserted = await conn.fetchval(
                        """INSERT INTO scrape_targets (spec_id, domain_id, url)
                           VALUES ($1, $2, $3)
                           ON CONFLICT (spec_id, url_key) DO NOTHING RETURNING id""",
                        row["id"], domain_id, url,
                    )
                    if inserted is not None:
                        n += 1
                return n

    async def feed_links_to_crawler(self, origin_url: str, links: list[DiscoveredLink]) -> int:
        """Scraper -> Crawler feed: hand discovered links to the same
        frontier.add() the Crawler itself uses, after making sure the
        scrape target's own URL has a `urls` row to be the FK parent for
        the new link edges (a Scraper run independently of the Crawler may
        never have created one)."""
        async with self.pool.acquire() as conn:
            async with conn.transaction():
                host = registrable_host(origin_url)
                if not host:
                    return 0
                domain_id = await conn.fetchval(
                    """INSERT INTO domains (host) VALUES ($1)
                       ON CONFLICT (host) DO UPDATE SET host = EXCLUDED.host
                       RETURNING id""", host,
                )
                origin_id = await conn.fetchval(
                    """INSERT INTO urls (domain_id, url, depth, priority)
                       VALUES ($1, $2, 0, 100)
                       ON CONFLICT (url_key) DO NOTHING RETURNING id""",
                    domain_id, origin_url,
                )
                if origin_id is None:
                    origin_id = await conn.fetchval(
                        "SELECT id FROM urls WHERE url_key = digest($1, 'sha256')", origin_url,
                    )
        return await self.add(links, from_url_id=origin_id, depth=0)
