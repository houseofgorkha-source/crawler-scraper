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
    RETURN QUERY
    WITH candidate AS (
        SELECT DISTINCT ON (u.domain_id) u.id
        FROM urls u
        JOIN domains d ON d.id = u.domain_id
        WHERE u.status = 'pending'
          AND u.next_crawl_at <= now()
          AND d.is_crawlable
          AND d.next_available_at <= now()
        ORDER BY u.domain_id, u.priority DESC, u.next_crawl_at
        FOR UPDATE OF u SKIP LOCKED
        LIMIT p_batch_size
    ),
    claimed AS (
        UPDATE urls u
        SET status           = 'leased',
            lease_owner      = p_worker_id,
            lease_expires_at = now() + make_interval(secs => p_lease_seconds),
            attempt_count    = u.attempt_count + 1
        FROM candidate c
        WHERE u.id = c.id
        RETURNING u.id, u.url, u.domain_id, u.depth, u.etag, u.last_modified
    ),
    gated AS (
        UPDATE domains d
        SET next_available_at = now() + make_interval(secs => d.crawl_delay_ms / 1000.0)
        FROM claimed cl
        WHERE d.id = cl.domain_id
        RETURNING d.id, d.host, d.js_required
    )
    SELECT cl.id, cl.url, g.host, cl.depth, cl.etag, cl.last_modified, g.js_required
    FROM claimed cl JOIN gated g ON g.id = cl.domain_id;
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
