"""
CLI entrypoint.

    python -m crawler.cli seed https://example.com https://other.org
    python -m crawler.cli crawl --workers 8 [--feed-scraper]
    python -m crawler.cli index
    python -m crawler.cli reap        # normally run from cron, see below

    python -m crawler.cli spec add spec.json
    python -m crawler.cli spec list
    python -m crawler.cli spec show <spec-name> [--version N]
    python -m crawler.cli spec activate|deactivate <spec-name> [--version N]
    python -m crawler.cli submit-scrape <spec-name> https://example.com/p/1 ...
    python -m crawler.cli scrape --workers 8
    python -m crawler.cli records list <spec-name> [--limit N] [--output file.json]

crawl and scrape are independently runnable processes -- run either alone,
both together, or wire one to feed the other (--feed-scraper on crawl,
feed_to_crawler in the spec). See CLAUDE.md for the five operating modes.
"""
from __future__ import annotations

import argparse
import asyncio
import json
import logging
import os
import signal

import asyncpg
import redis.asyncio as aioredis
import structlog
from prometheus_client import start_http_server

from .db import IndexerDB
from .fetch import HttpFetcher
from .frontier import PostgresFrontier
from .index import Indexer, SETTINGS
from .render import PlaywrightRenderer
from .scrape_worker import ScrapeWorker
from .store import BlobStore
from .worker import CrawlWorker

log = structlog.get_logger()

PG_DSN = os.environ.get("CRAWLER_PG_DSN", "postgresql://postgres:crawler@localhost/crawler")
REDIS_URL = os.environ.get("CRAWLER_REDIS_URL", "redis://localhost:6379/0")
MEILI_URL = os.environ.get("CRAWLER_MEILI_URL", "http://localhost:7700")
MEILI_KEY = os.environ.get("CRAWLER_MEILI_KEY", "devkey")
S3_BUCKET = os.environ.get("CRAWLER_S3_BUCKET", "crawler")


async def _fetch_robots(host: str, origin: str | None = None) -> tuple[str | None, int]:
    fetcher = HttpFetcher()
    try:
        from .contracts import CrawlTask, FetchOutcome

        async def _get(scheme_authority: str):
            # robots.txt is legitimately served as text/plain (the de-facto
            # standard, confirmed live against real sites), not text/html --
            # expect_html=False keeps the body regardless of declared
            # content-type, instead of silently discarding it.
            return await fetcher.fetch(CrawlTask(
                url_id=-1, url=f"{scheme_authority}/robots.txt", host=host, depth=0),
                expect_html=False)

        if origin is not None:
            # The caller (frontier.refresh_robots) resolved this from the
            # actual URL being crawled, so it already carries the right
            # scheme and port -- no guessing needed.
            result = await _get(origin)
        else:
            # No source URL available (e.g. a bare host with no task
            # context). https is the correct default for real hosts, but a
            # plain-HTTP-only target has no TLS listener at all, so that
            # attempt fails at the connection level, not with an HTTP
            # error -- only then retry over http.
            result = await _get(f"https://{host}")
            if result.outcome is FetchOutcome.NETWORK_ERROR:
                result = await _get(f"http://{host}")

        if result.has_body:
            return result.body.decode("utf-8", "replace"), result.status_code
        return None, result.status_code or 599
    finally:
        await fetcher.aclose()


async def _pool() -> asyncpg.Pool:
    return await asyncpg.create_pool(PG_DSN, min_size=4, max_size=32)


async def _blob_store() -> tuple[BlobStore, object]:
    import aioboto3
    session = aioboto3.Session()
    # aioboto3 clients are async context managers; for a long-lived daemon we
    # enter once and hold it for the process lifetime. The context manager
    # itself (not just the client it yields) has to be kept and __aexit__'d
    # on shutdown -- it owns the underlying aiohttp ClientSession/connector,
    # and without a matching exit that session is never closed, leaking a
    # connection until the process exits ("Unclosed client session" /
    # "Unclosed connector" warnings from aiohttp at GC time).
    cm = session.client(
        "s3", endpoint_url=os.environ.get("CRAWLER_S3_ENDPOINT", "http://localhost:9000"),
        aws_access_key_id=os.environ.get("CRAWLER_S3_KEY", "minioadmin"),
        aws_secret_access_key=os.environ.get("CRAWLER_S3_SECRET", "minioadmin"),
    )
    client = await cm.__aenter__()
    return BlobStore(client, S3_BUCKET), cm


def _maybe_start_metrics(port: int | None) -> None:
    # Opt-in, no default port: crawl/scrape/index commonly run as separate
    # simultaneous processes, and a shared default port would collide the
    # moment more than one of them is started with metrics on.
    if port is not None:
        start_http_server(port)
        log.info("metrics_started", port=port)


def _install_stop_handler(stop: asyncio.Event) -> None:
    # ProactorEventLoop (the asyncio default on Windows) does not implement
    # add_signal_handler; fall back to signal.signal there. Ctrl+C still
    # raises KeyboardInterrupt in that case, which is caught by cmd_* callers.
    loop = asyncio.get_running_loop()
    try:
        for sig in (signal.SIGINT, signal.SIGTERM):
            loop.add_signal_handler(sig, stop.set)
    except NotImplementedError:
        def _handler(signum, frame):
            loop.call_soon_threadsafe(stop.set)
        for sig in (signal.SIGINT, signal.SIGTERM):
            signal.signal(sig, _handler)


async def cmd_seed(args: argparse.Namespace) -> None:
    pool = await _pool()
    redis = aioredis.from_url(REDIS_URL)
    frontier = PostgresFrontier(pool, redis, robots_fetcher=_fetch_robots)
    n = await frontier.seed(args.urls)
    log.info("seeded", requested=len(args.urls), inserted=n)
    await redis.aclose()
    await pool.close()


async def cmd_crawl(args: argparse.Namespace) -> None:
    _maybe_start_metrics(args.metrics_port)
    pool = await _pool()
    redis = aioredis.from_url(REDIS_URL)
    frontier = PostgresFrontier(pool, redis, robots_fetcher=_fetch_robots)
    store, store_cm = await _blob_store()

    renderer = None
    if not args.no_render:
        renderer = PlaywrightRenderer(PG_DSN, max_pages=args.render_pages)
        await renderer.start()

    workers = [
        CrawlWorker(frontier, store, renderer=renderer, worker_id=f"w{i}",
                    feed_scraper=args.feed_scraper)
        for i in range(args.workers)
    ]

    stop = asyncio.Event()
    _install_stop_handler(stop)

    async def reaper_loop():
        while not stop.is_set():
            async with pool.acquire() as conn:
                n = await conn.fetchval("SELECT reap_expired_leases()")
            if n:
                log.info("reaped_leases", count=n)
            await asyncio.sleep(60)

    tasks = [asyncio.create_task(w.run()) for w in workers]
    tasks.append(asyncio.create_task(reaper_loop()))

    log.info("crawl_started", workers=args.workers, render=not args.no_render)
    await stop.wait()

    log.info("shutting_down")
    for w in workers:
        w.stop()
    for t in tasks:
        t.cancel()
    await asyncio.gather(*tasks, return_exceptions=True)
    if renderer:
        await renderer.aclose()
    await store_cm.__aexit__(None, None, None)
    await redis.aclose()
    await pool.close()


async def cmd_scrape(args: argparse.Namespace) -> None:
    _maybe_start_metrics(args.metrics_port)
    pool = await _pool()
    redis = aioredis.from_url(REDIS_URL)
    frontier = PostgresFrontier(pool, redis, robots_fetcher=_fetch_robots)
    store, store_cm = await _blob_store()

    renderer = None
    if not args.no_render:
        renderer = PlaywrightRenderer(PG_DSN, max_pages=args.render_pages)
        await renderer.start()

    workers = [
        ScrapeWorker(frontier, store, renderer=renderer, worker_id=f"s{i}")
        for i in range(args.workers)
    ]

    stop = asyncio.Event()
    _install_stop_handler(stop)

    async def reaper_loop():
        while not stop.is_set():
            async with pool.acquire() as conn:
                n = await conn.fetchval("SELECT reap_expired_scrape_leases()")
            if n:
                log.info("reaped_scrape_leases", count=n)
            await asyncio.sleep(60)

    tasks = [asyncio.create_task(w.run()) for w in workers]
    tasks.append(asyncio.create_task(reaper_loop()))

    log.info("scrape_started", workers=args.workers, render=not args.no_render)
    await stop.wait()

    log.info("shutting_down")
    for w in workers:
        w.stop()
    for t in tasks:
        t.cancel()
    await asyncio.gather(*tasks, return_exceptions=True)
    if renderer:
        await renderer.aclose()
    await store_cm.__aexit__(None, None, None)
    await redis.aclose()
    await pool.close()


async def cmd_submit_scrape(args: argparse.Namespace) -> None:
    pool = await _pool()
    async with pool.acquire() as conn:
        spec_id = await conn.fetchval(
            "SELECT id FROM scrape_specs WHERE name = $1 ORDER BY version DESC LIMIT 1",
            args.spec,
        )
    if spec_id is None:
        log.error("unknown_spec", spec=args.spec)
        await pool.close()
        return
    redis = aioredis.from_url(REDIS_URL)
    frontier = PostgresFrontier(pool, redis, robots_fetcher=_fetch_robots)
    n = await frontier.submit_scrape_targets(spec_id, args.urls)
    log.info("submitted_scrape_targets", spec=args.spec, requested=len(args.urls), inserted=n)
    await redis.aclose()
    await pool.close()


async def cmd_spec_add(args: argparse.Namespace) -> None:
    with open(args.file, "r", encoding="utf-8") as f:
        spec = json.load(f)

    pool = await _pool()
    redis = aioredis.from_url(REDIS_URL)
    frontier = PostgresFrontier(pool, redis, robots_fetcher=_fetch_robots)
    spec_id = await frontier.register_spec(
        name=spec["name"],
        fields=spec["fields"],
        version=spec.get("version", 1),
        render_mode=spec.get("render_mode", "auto"),
        link_field=spec.get("link_field"),
        feed_to_crawler=spec.get("feed_to_crawler", False),
        feed_from_crawler=spec.get("feed_from_crawler", False),
        host_pattern=spec.get("host_pattern"),
        path_regex=spec.get("path_regex"),
    )
    log.info("spec_registered", name=spec["name"], version=spec.get("version", 1), id=spec_id)
    await redis.aclose()
    await pool.close()


async def cmd_spec_list(args: argparse.Namespace) -> None:
    pool = await _pool()
    redis = aioredis.from_url(REDIS_URL)
    frontier = PostgresFrontier(pool, redis, robots_fetcher=_fetch_robots)
    specs = await frontier.list_specs()
    for s in specs:
        log.info("spec", name=s["name"], version=s["version"], is_active=s["is_active"],
                 render_mode=s["render_mode"], feed_to_crawler=s["feed_to_crawler"],
                 feed_from_crawler=s["feed_from_crawler"])
    if not specs:
        log.info("no_specs_registered")
    await redis.aclose()
    await pool.close()


async def cmd_spec_show(args: argparse.Namespace) -> None:
    pool = await _pool()
    redis = aioredis.from_url(REDIS_URL)
    frontier = PostgresFrontier(pool, redis, robots_fetcher=_fetch_robots)
    row = await frontier.get_spec_row(args.name, args.version)
    if row is None:
        log.error("unknown_spec", spec=args.name)
    else:
        fields = row["fields"]
        fields = json.loads(fields) if isinstance(fields, str) else fields
        print(json.dumps({**{k: v for k, v in row.items() if k != "fields"},
                          "fields": fields}, default=str, indent=2))
    await redis.aclose()
    await pool.close()


async def _cmd_spec_set_active(args: argparse.Namespace, active: bool) -> None:
    pool = await _pool()
    redis = aioredis.from_url(REDIS_URL)
    frontier = PostgresFrontier(pool, redis, robots_fetcher=_fetch_robots)
    spec_id = await frontier.set_spec_active(args.name, active, args.version)
    if spec_id is None:
        log.error("unknown_spec", spec=args.name)
    else:
        log.info("spec_active_set", spec=args.name, id=spec_id, is_active=active)
    await redis.aclose()
    await pool.close()


async def cmd_spec_activate(args: argparse.Namespace) -> None:
    await _cmd_spec_set_active(args, True)


async def cmd_spec_deactivate(args: argparse.Namespace) -> None:
    await _cmd_spec_set_active(args, False)


async def cmd_records_list(args: argparse.Namespace) -> None:
    pool = await _pool()
    redis = aioredis.from_url(REDIS_URL)
    frontier = PostgresFrontier(pool, redis, robots_fetcher=_fetch_robots)
    records = await frontier.list_scraped_records(args.spec, args.version, args.limit)

    if args.output:
        with open(args.output, "w", encoding="utf-8") as f:
            json.dump(records, f, indent=2)
        log.info("records_exported", spec=args.spec, count=len(records), file=args.output)
    else:
        for r in records:
            print(json.dumps(r, default=str))
        log.info("records_listed", spec=args.spec, count=len(records))
    await redis.aclose()
    await pool.close()


async def _create_index_with_retry(search, stop: asyncio.Event, delay: float = 5.0):
    # "Indexing is asynchronous ... the crawl must never block on
    # Meilisearch being slow or down" (CLAUDE.md) applies to the indexer
    # process's own startup too, not just to the crawl itself: Meilisearch
    # simply not being up yet (a normal docker-compose startup race, or a
    # transient outage) previously crashed cmd_index immediately via an
    # unguarded create_index()/update_settings() call, before Indexer.run()
    # -- whose own per-batch loop already tolerates Meilisearch failures --
    # ever got a chance to start. Retry with a fixed backoff until
    # Meilisearch answers or shutdown is requested, instead of requiring an
    # external supervisor to keep restarting a process that crash-loops.
    from meilisearch_python_sdk.errors import MeilisearchCommunicationError

    while not stop.is_set():
        try:
            index = await search.create_index("pages", primary_key="id")
            await index.update_settings(SETTINGS)
            return index
        except MeilisearchCommunicationError:
            log.warning("meilisearch_unavailable_at_startup", retry_in_s=delay)
            try:
                await asyncio.wait_for(stop.wait(), timeout=delay)
            except asyncio.TimeoutError:
                pass
    return None


async def cmd_index(args: argparse.Namespace) -> None:
    from meilisearch_python_sdk import AsyncClient

    _maybe_start_metrics(args.metrics_port)
    pool = await _pool()
    db = IndexerDB(pool)
    store, store_cm = await _blob_store()

    stop = asyncio.Event()
    _install_stop_handler(stop)

    async with AsyncClient(MEILI_URL, MEILI_KEY) as search:
        index = await _create_index_with_retry(search, stop)
        if index is not None:
            indexer = Indexer(db, search, store)
            task = asyncio.create_task(indexer.run())
            log.info("indexer_started")
            await stop.wait()
            indexer.stop()
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)

    await store_cm.__aexit__(None, None, None)
    await pool.close()


async def cmd_reap(args: argparse.Namespace) -> None:
    pool = await _pool()
    async with pool.acquire() as conn:
        n = await conn.fetchval("SELECT reap_expired_leases()")
        n_scrape = await conn.fetchval("SELECT reap_expired_scrape_leases()")
    log.info("reaped_leases", count=n, scrape_count=n_scrape)
    await pool.close()


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(message)s")
    structlog.configure(
        processors=[structlog.processors.JSONRenderer()],
        logger_factory=structlog.stdlib.LoggerFactory(),
    )

    parser = argparse.ArgumentParser(prog="crawler")
    sub = parser.add_subparsers(dest="command", required=True)

    p_seed = sub.add_parser("seed", help="add seed URLs to the frontier")
    p_seed.add_argument("urls", nargs="+")
    p_seed.set_defaults(func=cmd_seed)

    p_crawl = sub.add_parser("crawl", help="run crawl workers")
    p_crawl.add_argument("--workers", type=int, default=8)
    p_crawl.add_argument("--render-pages", type=int, default=4,
                         help="max concurrent Playwright pages")
    p_crawl.add_argument("--no-render", action="store_true",
                         help="disable the render tier entirely (static-only)")
    p_crawl.add_argument("--feed-scraper", action="store_true",
                         help="enroll crawled URLs into any matching active scrape spec")
    p_crawl.add_argument("--metrics-port", type=int, default=None,
                         help="expose Prometheus metrics on this port (off by default)")
    p_crawl.set_defaults(func=cmd_crawl)

    p_scrape = sub.add_parser("scrape", help="run scrape workers")
    p_scrape.add_argument("--workers", type=int, default=8)
    p_scrape.add_argument("--render-pages", type=int, default=4,
                          help="max concurrent Playwright pages (shared cross-process "
                               "with any running crawl process via Postgres advisory locks)")
    p_scrape.add_argument("--no-render", action="store_true",
                          help="disable the render tier entirely (static-only)")
    p_scrape.add_argument("--metrics-port", type=int, default=None,
                          help="expose Prometheus metrics on this port (off by default)")
    p_scrape.set_defaults(func=cmd_scrape)

    p_submit = sub.add_parser("submit-scrape", help="add explicit scrape targets under a spec")
    p_submit.add_argument("spec", help="registered spec name")
    p_submit.add_argument("urls", nargs="+")
    p_submit.set_defaults(func=cmd_submit_scrape)

    p_spec = sub.add_parser("spec", help="manage scrape specs")
    spec_sub = p_spec.add_subparsers(dest="spec_command", required=True)
    p_spec_add = spec_sub.add_parser("add", help="register a scrape spec from a JSON file")
    p_spec_add.add_argument("file", help="path to a spec JSON file")
    p_spec_add.set_defaults(func=cmd_spec_add)

    p_spec_list = spec_sub.add_parser("list", help="list all registered specs")
    p_spec_list.set_defaults(func=cmd_spec_list)

    p_spec_show = spec_sub.add_parser("show", help="show a spec's full definition")
    p_spec_show.add_argument("name")
    p_spec_show.add_argument("--version", type=int, default=None,
                             help="defaults to the latest version")
    p_spec_show.set_defaults(func=cmd_spec_show)

    p_spec_activate = spec_sub.add_parser("activate", help="set a spec active (resume its queue)")
    p_spec_activate.add_argument("name")
    p_spec_activate.add_argument("--version", type=int, default=None)
    p_spec_activate.set_defaults(func=cmd_spec_activate)

    p_spec_deactivate = spec_sub.add_parser(
        "deactivate", help="set a spec inactive (its queue stops draining, nothing is deleted)"
    )
    p_spec_deactivate.add_argument("name")
    p_spec_deactivate.add_argument("--version", type=int, default=None)
    p_spec_deactivate.set_defaults(func=cmd_spec_deactivate)

    p_records = sub.add_parser("records", help="inspect scraped_records")
    records_sub = p_records.add_subparsers(dest="records_command", required=True)
    p_records_list = records_sub.add_parser(
        "list", help="list/export a spec's scraped records"
    )
    p_records_list.add_argument("spec", help="registered spec name")
    p_records_list.add_argument("--version", type=int, default=None)
    p_records_list.add_argument("--limit", type=int, default=100)
    p_records_list.add_argument("--output", default=None,
                                help="write JSON to this file instead of printing one per line")
    p_records_list.set_defaults(func=cmd_records_list)

    p_index = sub.add_parser("index", help="run the async indexer")
    p_index.add_argument("--metrics-port", type=int, default=None,
                         help="expose Prometheus metrics on this port (off by default)")
    p_index.set_defaults(func=cmd_index)

    p_reap = sub.add_parser("reap", help="run one lease-reap pass and exit (for cron)")
    p_reap.set_defaults(func=cmd_reap)

    args = parser.parse_args()
    asyncio.run(args.func(args))


if __name__ == "__main__":
    main()
