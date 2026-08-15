-- ============================================================================
-- V1 Crawler schema. Postgres is the SOURCE OF TRUTH.
-- Redis holds only a rebuildable working set. Losing Redis must cost time,
-- never data.
-- ============================================================================

CREATE EXTENSION IF NOT EXISTS pgcrypto;   -- digest()

-- ---------------------------------------------------------------------------
-- domains: per-host policy + observed behaviour. Read on every schedule
-- decision, so it stays narrow and hot.
-- ---------------------------------------------------------------------------
CREATE TABLE domains (
    id                  bigserial PRIMARY KEY,
    host                text        NOT NULL UNIQUE,   -- registrable host, lowercased, no port
    scheme_hint         text        NOT NULL DEFAULT 'https',

    -- robots.txt: cached, with explicit staleness. A NULL fetched_at means
    -- "never fetched" -> the fetcher MUST resolve it before any request.
    robots_body         text,
    robots_fetched_at   timestamptz,
    robots_expires_at   timestamptz,
    robots_status       smallint,                      -- HTTP status of last robots fetch
    crawl_delay_ms      integer     NOT NULL DEFAULT 1000,

    -- kill switch: set false on repeated 4xx/5xx, robots Disallow: /, or manual ban
    is_crawlable        boolean     NOT NULL DEFAULT true,
    block_reason        text,

    -- scheduling gate: no worker may fetch this host before next_available_at
    next_available_at   timestamptz NOT NULL DEFAULT now(),

    -- escalation hint. Set true after N pages on this host needed rendering.
    -- Lets us skip the wasted static fetch on known SPA hosts.
    js_required         boolean     NOT NULL DEFAULT false,
    js_evidence_count   integer     NOT NULL DEFAULT 0,

    -- observed health, updated by workers (cheap running aggregates)
    avg_latency_ms      integer     NOT NULL DEFAULT 0,
    pages_discovered    bigint      NOT NULL DEFAULT 0,
    pages_crawled       bigint      NOT NULL DEFAULT 0,
    error_count         bigint      NOT NULL DEFAULT 0,

    created_at          timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX domains_available_idx
    ON domains (next_available_at)
    WHERE is_crawlable;

-- ---------------------------------------------------------------------------
-- urls: one row per normalized URL. THIS is the dedup mechanism in V1.
-- No Bloom filter. A unique index over the sha256 of the normalized URL
-- sidesteps btree key-length limits and gives exact, durable dedup.
-- ---------------------------------------------------------------------------
CREATE TYPE url_status AS ENUM (
    'pending',    -- eligible for claiming
    'leased',     -- claimed by a worker, lease_expires_at is authoritative
    'done',       -- fetched successfully at least once
    'failed',     -- exhausted retries
    'skipped'     -- robots-disallowed, non-HTML, or filtered
);

CREATE TABLE urls (
    id                  bigserial PRIMARY KEY,
    domain_id           bigint      NOT NULL REFERENCES domains(id),

    url                 text        NOT NULL,          -- normalized form (see normalize.py)
    url_key             bytea       NOT NULL GENERATED ALWAYS AS
                                    (digest(url, 'sha256')) STORED,

    depth               integer     NOT NULL DEFAULT 0,
    priority            integer     NOT NULL DEFAULT 100,   -- higher = sooner

    status              url_status  NOT NULL DEFAULT 'pending',

    -- lease-based claiming. A crashed worker's URLs return automatically
    -- once lease_expires_at passes; nothing is ever permanently stranded.
    lease_owner         text,
    lease_expires_at    timestamptz,

    discovered_at       timestamptz NOT NULL DEFAULT now(),
    last_crawled_at     timestamptz,
    next_crawl_at       timestamptz NOT NULL DEFAULT now(),

    -- current state denormalized onto the row. The hot path never touches
    -- crawl_attempts.
    last_status_code    smallint,
    attempt_count       integer     NOT NULL DEFAULT 0,
    consecutive_failures integer    NOT NULL DEFAULT 0,

    -- revalidation
    etag                text,
    last_modified       text,

    -- content identity
    content_sha256      bytea,                          -- exact-duplicate detection
    simhash             bigint,                         -- near-duplicate detection
    render_mode         text,                           -- 'static' | 'rendered'

    CONSTRAINT urls_url_key_uniq UNIQUE (url_key)
);

-- The claim query's covering index: eligible work, best-first.
CREATE INDEX urls_claimable_idx
    ON urls (priority DESC, next_crawl_at)
    WHERE status = 'pending';

-- Reaper index: find expired leases cheaply.
CREATE INDEX urls_expired_lease_idx
    ON urls (lease_expires_at)
    WHERE status = 'leased';

CREATE INDEX urls_domain_idx ON urls (domain_id);
CREATE INDEX urls_content_sha_idx ON urls (content_sha256) WHERE content_sha256 IS NOT NULL;

-- ---------------------------------------------------------------------------
-- links: the web graph edges. We do NOT rank in V1 -- but we DO record.
-- Edges are observable only at parse time; backfilling them later costs a
-- full re-crawl of the corpus. Writing them is nearly free. This is the
-- single highest-regret table to omit.
-- ---------------------------------------------------------------------------
CREATE TABLE links (
    from_url_id     bigint      NOT NULL REFERENCES urls(id) ON DELETE CASCADE,
    to_url_id       bigint      NOT NULL REFERENCES urls(id) ON DELETE CASCADE,
    anchor_text     text,
    rel             text,                               -- nofollow, ugc, sponsored...
    first_seen_at   timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (from_url_id, to_url_id)
);

CREATE INDEX links_to_idx ON links (to_url_id);   -- inbound lookups for later ranking

-- ---------------------------------------------------------------------------
-- documents: extracted, indexable content. Body text lives in object storage;
-- Postgres keeps the pointer and the index lifecycle state.
-- ---------------------------------------------------------------------------
CREATE TYPE index_state AS ENUM ('pending', 'indexed', 'failed', 'suppressed');

CREATE TABLE documents (
    url_id          bigint      PRIMARY KEY REFERENCES urls(id) ON DELETE CASCADE,
    title           text,
    description     text,
    lang            text,
    word_count      integer,

    raw_key         text        NOT NULL,   -- s3://raw/<host>/<url_key>.html.zst
    text_key        text,                   -- s3://text/<host>/<url_key>.txt.zst

    -- Indexing is asynchronous: the fetch path never blocks on Meilisearch.
    index_state     index_state NOT NULL DEFAULT 'pending',
    indexed_at      timestamptz,
    -- set when simhash matches an already-indexed doc; kept, not indexed
    duplicate_of    bigint      REFERENCES urls(id),

    updated_at      timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX documents_index_queue_idx
    ON documents (updated_at)
    WHERE index_state = 'pending';

-- ---------------------------------------------------------------------------
-- crawl_attempts: append-only debugging history, NOT hot-path state.
-- Partitioned monthly and dropped after ~30 days. Without partitioning this
-- becomes the largest table in the system within weeks and degrades
-- everything around it.
-- ---------------------------------------------------------------------------
CREATE TABLE crawl_attempts (
    id              bigserial,
    url_id          bigint      NOT NULL,
    started_at      timestamptz NOT NULL DEFAULT now(),
    duration_ms     integer,
    status_code     smallint,
    render_mode     text,
    bytes           integer,
    error_class     text,
    error_detail    text,
    worker_id       text,
    PRIMARY KEY (id, started_at)
) PARTITION BY RANGE (started_at);

CREATE INDEX crawl_attempts_url_idx ON crawl_attempts (url_id, started_at DESC);

-- Partitions are created by ops/partitions.sql (run monthly from cron).
CREATE TABLE crawl_attempts_default PARTITION OF crawl_attempts DEFAULT;

-- ============================================================================
-- claim_urls: the worker contract, enforced in SQL.
--
-- Guarantees:
--   * FOR UPDATE SKIP LOCKED  -> no two workers get the same URL
--   * one URL per host per batch -> politeness holds even under concurrency
--   * domains.next_available_at -> crawl-delay enforced at SCHEDULE time
--     (the fetcher re-checks robots at FETCH time; cached allows go stale,
--      and a stale allow is what gets you banned)
--   * lease_expires_at -> crashed workers release their work automatically
-- ============================================================================
CREATE OR REPLACE FUNCTION claim_urls(
    p_worker_id     text,
    p_batch_size    integer,
    p_lease_seconds integer DEFAULT 300
)
RETURNS TABLE (url_id bigint, url text, host text, depth integer, etag text,
               last_modified text, js_required boolean)
LANGUAGE plpgsql AS $$
BEGIN
    -- Postgres rejects FOR UPDATE combined with DISTINCT ON in the same
    -- query ("FOR UPDATE is not allowed with DISTINCT clause"), so the
    -- one-row-per-domain pick and the row lock have to happen in separate
    -- steps: a plain DISTINCT to choose eligible domains, then a LATERAL
    -- join per domain that does the ORDER BY + FOR UPDATE SKIP LOCKED + LIMIT 1.
    RETURN QUERY
    WITH eligible AS (
        SELECT DISTINCT u.domain_id, d.host, d.js_required
        FROM urls u
        JOIN domains d ON d.id = u.domain_id
        WHERE u.status = 'pending'
          AND u.next_crawl_at <= now()
          AND d.is_crawlable
          AND d.next_available_at <= now()
        ORDER BY u.domain_id
        LIMIT p_batch_size
    ),
    candidate AS (
        SELECT e.domain_id, e.host, e.js_required,
               picked.id AS url_id, picked.url, picked.depth,
               picked.etag, picked.last_modified
        FROM eligible e
        CROSS JOIN LATERAL (
            SELECT u.id, u.url, u.depth, u.etag, u.last_modified
            FROM urls u
            WHERE u.domain_id = e.domain_id
              AND u.status = 'pending'
              AND u.next_crawl_at <= now()
            ORDER BY u.priority DESC, u.next_crawl_at
            FOR UPDATE OF u SKIP LOCKED
            LIMIT 1
        ) picked
    ),
    claimed AS (
        UPDATE urls u
        SET status           = 'leased',
            lease_owner      = p_worker_id,
            lease_expires_at = now() + make_interval(secs => p_lease_seconds),
            attempt_count    = u.attempt_count + 1
        FROM candidate c
        WHERE u.id = c.url_id
        RETURNING u.id, u.domain_id
    ),
    gated AS (
        UPDATE domains d
        SET next_available_at = now() + make_interval(secs => d.crawl_delay_ms / 1000.0)
        FROM claimed cl
        WHERE d.id = cl.domain_id
        RETURNING d.id
    )
    SELECT c.url_id, c.url, c.host, c.depth, c.etag, c.last_modified, c.js_required
    FROM candidate c
    JOIN claimed cl ON cl.id = c.url_id
    JOIN gated g ON g.id = c.domain_id;
END;
$$;

-- Reaper: return abandoned leases to the pool. Run every 60s.
CREATE OR REPLACE FUNCTION reap_expired_leases() RETURNS integer
LANGUAGE sql AS $$
    WITH r AS (
        UPDATE urls
        SET status = 'pending', lease_owner = NULL, lease_expires_at = NULL
        WHERE status = 'leased' AND lease_expires_at < now()
        RETURNING 1
    ) SELECT count(*)::integer FROM r;
$$;

-- ============================================================================
-- SCRAPER: a peer of the Crawler, not a mode of it.
--
-- The Crawler answers "what exists out there" (broad discovery, generic
-- text, search). The Scraper answers "give me these specific structured
-- fields from these specific pages" (spec-driven, CSS/XPath, durable
-- structured records). They are additive here, not a rework of anything
-- above: urls/domains/links/documents/crawl_attempts and claim_urls() are
-- untouched.
--
-- The one thing that MUST be shared, never duplicated: the domains table's
-- politeness clock (next_available_at, robots fields). If Crawler and
-- Scraper each kept their own clock per domain, the combined request rate
-- could exceed crawl_delay_ms even though each subsystem individually
-- respects it. claim_scrape_targets() below reads and updates the exact
-- same domains columns claim_urls() does.
-- ============================================================================

-- A scrape spec: what to extract (fields, CSS/XPath + optional nesting,
-- serialized as jsonb -- see crawler/scrape_extract.py for the shape) and
-- how this spec relates to the Crawler:
--   feed_from_crawler -> Crawler-discovered URLs matching host_pattern/
--                        path_regex are auto-enrolled as scrape targets
--                        (Crawler -> Scraper)
--   feed_to_crawler   -> links extracted into `link_field` are handed to
--                        the crawl frontier (Scraper -> Crawler)
-- Both default false: a spec is scrape-only unless explicitly wired up,
-- so "Scraper independently" and "Crawler independently" require zero
-- extra configuration to stay decoupled.
CREATE TABLE scrape_specs (
    id                  bigserial PRIMARY KEY,
    name                text        NOT NULL,
    version             integer     NOT NULL DEFAULT 1,
    fields              jsonb       NOT NULL,

    -- 'auto' reuses fetch.needs_render() same as the Crawler; 'always' /
    -- 'never' let an operator who already knows the target site skip the
    -- heuristic entirely.
    render_mode         text        NOT NULL DEFAULT 'auto'
                                     CHECK (render_mode IN ('auto', 'always', 'never')),

    link_field          text,
    feed_to_crawler     boolean     NOT NULL DEFAULT false,
    feed_from_crawler   boolean     NOT NULL DEFAULT false,
    host_pattern        text,       -- exact host match; NULL = any host
    path_regex          text,       -- Postgres regex against the full url; NULL = any

    is_active           boolean     NOT NULL DEFAULT true,
    created_at          timestamptz NOT NULL DEFAULT now(),

    CONSTRAINT scrape_specs_name_version_uniq UNIQUE (name, version)
);

-- One row per (spec, url) work item. Deliberately NOT the same row as
-- `urls` -- a URL can be simultaneously 'done' for the Crawler and
-- 'pending'/re-attempted for the Scraper, and the same URL can be scraped
-- under more than one spec. Same lease/status shape as `urls` on purpose:
-- it's the same claim-and-lease discipline, just a second, independent
-- queue.
CREATE TYPE scrape_status AS ENUM ('pending', 'leased', 'done', 'failed', 'skipped');

CREATE TABLE scrape_targets (
    id                  bigserial PRIMARY KEY,
    spec_id             bigint      NOT NULL REFERENCES scrape_specs(id),
    domain_id           bigint      NOT NULL REFERENCES domains(id),

    url                 text        NOT NULL,
    url_key             bytea       NOT NULL GENERATED ALWAYS AS
                                    (digest(url, 'sha256')) STORED,

    status              scrape_status NOT NULL DEFAULT 'pending',
    lease_owner         text,
    lease_expires_at    timestamptz,

    discovered_at       timestamptz NOT NULL DEFAULT now(),
    last_attempted_at   timestamptz,
    next_attempt_at     timestamptz NOT NULL DEFAULT now(),

    last_status_code    smallint,
    attempt_count       integer     NOT NULL DEFAULT 0,
    consecutive_failures integer    NOT NULL DEFAULT 0,

    etag                text,
    last_modified       text,

    -- A URL may be scraped by more than one spec, but not enrolled twice
    -- under the same spec.
    CONSTRAINT scrape_targets_spec_url_uniq UNIQUE (spec_id, url_key)
);

CREATE INDEX scrape_targets_claimable_idx
    ON scrape_targets (next_attempt_at)
    WHERE status = 'pending';

CREATE INDEX scrape_targets_expired_lease_idx
    ON scrape_targets (lease_expires_at)
    WHERE status = 'leased';

CREATE INDEX scrape_targets_domain_idx ON scrape_targets (domain_id);

-- Structured output. Decoupled from `documents` on purpose: scrape output
-- shape is spec-defined (arbitrary fields), not the Crawler's fixed
-- title/description/text columns. This is the Scraper's actual
-- deliverable -- durable, no retention/pruning, latest-wins on re-scrape
-- (same upsert convention as `documents`, not an append-only history like
-- `crawl_attempts`).
CREATE TABLE scraped_records (
    target_id       bigint      PRIMARY KEY REFERENCES scrape_targets(id) ON DELETE CASCADE,
    spec_id         bigint      NOT NULL REFERENCES scrape_specs(id),
    data            jsonb       NOT NULL,
    raw_key         text,                   -- optional raw HTML snapshot, s3://raw/...
    extracted_at    timestamptz NOT NULL DEFAULT now(),
    updated_at      timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX scraped_records_data_gin_idx ON scraped_records USING gin (data);

-- ============================================================================
-- claim_scrape_targets: same contract as claim_urls() -- one target per
-- domain per batch, FOR UPDATE SKIP LOCKED, gated by the SHARED
-- domains.next_available_at/is_crawlable. Split into eligible-domains
-- DISTINCT + per-domain LATERAL lock for the same reason claim_urls() is:
-- Postgres rejects FOR UPDATE combined with DISTINCT ON.
--
-- Also gated on scrape_specs.is_active, joined at both the domain-
-- eligibility step and the per-domain row pick -- a target whose spec has
-- been deactivated must never be claimed, whether or not other active
-- specs still have pending work on the same domain.
-- ============================================================================
CREATE OR REPLACE FUNCTION claim_scrape_targets(
    p_worker_id     text,
    p_batch_size    integer,
    p_lease_seconds integer DEFAULT 300
)
RETURNS TABLE (target_id bigint, url text, host text, spec_id bigint,
               etag text, last_modified text)
LANGUAGE plpgsql AS $$
BEGIN
    RETURN QUERY
    WITH eligible AS (
        SELECT DISTINCT t.domain_id, d.host
        FROM scrape_targets t
        JOIN domains d ON d.id = t.domain_id
        JOIN scrape_specs s ON s.id = t.spec_id
        WHERE t.status = 'pending'
          AND t.next_attempt_at <= now()
          AND d.is_crawlable
          AND d.next_available_at <= now()
          AND s.is_active
        ORDER BY t.domain_id
        LIMIT p_batch_size
    ),
    candidate AS (
        SELECT e.domain_id, e.host,
               picked.id AS target_id, picked.url, picked.spec_id,
               picked.etag, picked.last_modified
        FROM eligible e
        CROSS JOIN LATERAL (
            SELECT t.id, t.url, t.spec_id, t.etag, t.last_modified
            FROM scrape_targets t
            JOIN scrape_specs s ON s.id = t.spec_id
            WHERE t.domain_id = e.domain_id
              AND t.status = 'pending'
              AND t.next_attempt_at <= now()
              AND s.is_active
            ORDER BY t.next_attempt_at
            FOR UPDATE OF t SKIP LOCKED
            LIMIT 1
        ) picked
    ),
    claimed AS (
        UPDATE scrape_targets t
        SET status           = 'leased',
            lease_owner      = p_worker_id,
            lease_expires_at = now() + make_interval(secs => p_lease_seconds),
            attempt_count    = t.attempt_count + 1,
            last_attempted_at = now()
        FROM candidate c
        WHERE t.id = c.target_id
        RETURNING t.id, t.domain_id
    ),
    gated AS (
        -- Same column, same update, as claim_urls()'s gating step -- this
        -- is the shared politeness clock, deliberately not a separate one.
        UPDATE domains d
        SET next_available_at = now() + make_interval(secs => d.crawl_delay_ms / 1000.0)
        FROM claimed cl
        WHERE d.id = cl.domain_id
        RETURNING d.id
    )
    SELECT c.target_id, c.url, c.host, c.spec_id, c.etag, c.last_modified
    FROM candidate c
    JOIN claimed cl ON cl.id = c.target_id
    JOIN gated g ON g.id = c.domain_id;
END;
$$;

-- Reaper for the scrape queue's leases. Kept as a separate function rather
-- than generalizing reap_expired_leases() -- that function is verified and
-- untouched; this one is its exact structural twin for scrape_targets.
CREATE OR REPLACE FUNCTION reap_expired_scrape_leases() RETURNS integer
LANGUAGE sql AS $$
    WITH r AS (
        UPDATE scrape_targets
        SET status = 'pending', lease_owner = NULL, lease_expires_at = NULL
        WHERE status = 'leased' AND lease_expires_at < now()
        RETURNING 1
    ) SELECT count(*)::integer FROM r;
$$;
