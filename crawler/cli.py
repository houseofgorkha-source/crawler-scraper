"""
CLI entrypoint.

    python -m crawler.cli seed https://example.com https://other.org
    python -m crawler.cli crawl --workers 8
    python -m crawler.cli index
    python -m crawler.cli reap        # normally run from cron, see below
"""
from __future__ import annotations

import argparse
import asyncio
import logging
import os
import signal

import asyncpg
import redis.asyncio as aioredis
import structlog

from .db import IndexerDB
from .fetch import HttpFetcher
from .frontier import PostgresFrontier
from .index import Indexer, SETTINGS
from .render import PlaywrightRenderer
from .store import BlobStore
from .worker import CrawlWorker

log = structlog.get_logger()

PG_DSN = os.environ.get("CRAWLER_PG_DSN", "postgresql://postgres:crawler@localhost/crawler")
REDIS_URL = os.environ.get("CRAWLER_REDIS_URL", "redis://localhost:6379/0")
MEILI_URL = os.environ.get("CRAWLER_MEILI_URL", "http://localhost:7700")
MEILI_KEY = os.environ.get("CRAWLER_MEILI_KEY", "devkey")
S3_BUCKET = os.environ.get("CRAWLER_S3_BUCKET", "crawler")


async def _fetch_robots(host: str) -> tuple[str | None, int]:
    fetcher = HttpFetcher()
    try:
        from .contracts import CrawlTask
        result = await fetcher.fetch(CrawlTask(url_id=-1, url=f"https://{host}/robots.txt",
                                                host=host, depth=0))
        if result.has_body:
            return result.body.decode("utf-8", "replace"), result.status_code
        return None, result.status_code or 599
    finally:
        await fetcher.aclose()


async def _pool() -> asyncpg.Pool:
    return await asyncpg.create_pool(PG_DSN, min_size=4, max_size=32)


async def _blob_store() -> BlobStore:
    import aioboto3
    session = aioboto3.Session()
    # aioboto3 clients are async context managers; for a long-lived daemon we
    # enter once and hold it for the process lifetime.
    cm = session.client(
        "s3", endpoint_url=os.environ.get("CRAWLER_S3_ENDPOINT", "http://localhost:9000"),
        aws_access_key_id=os.environ.get("CRAWLER_S3_KEY", "minioadmin"),
        aws_secret_access_key=os.environ.get("CRAWLER_S3_SECRET", "minioadmin"),
    )
    client = await cm.__aenter__()
    return BlobStore(client, S3_BUCKET)


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
    pool = await _pool()
    redis = aioredis.from_url(REDIS_URL)
    frontier = PostgresFrontier(pool, redis, robots_fetcher=_fetch_robots)
    store = await _blob_store()

    renderer = None
    if not args.no_render:
        renderer = PlaywrightRenderer(max_pages=args.render_pages)
        await renderer.start()

    workers = [
        CrawlWorker(frontier, store, renderer=renderer, worker_id=f"w{i}")
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
    await redis.aclose()
    await pool.close()


async def cmd_index(args: argparse.Namespace) -> None:
    from meilisearch_python_sdk import AsyncClient

    pool = await _pool()
    db = IndexerDB(pool)
    store = await _blob_store()

    async with AsyncClient(MEILI_URL, MEILI_KEY) as search:
        index = await search.create_index("pages", primary_key="id")
        await index.update_settings(SETTINGS)
        indexer = Indexer(db, search, store)

        stop = asyncio.Event()
        _install_stop_handler(stop)

        task = asyncio.create_task(indexer.run())
        log.info("indexer_started")
        await stop.wait()
        indexer.stop()
        task.cancel()

    await pool.close()


async def cmd_reap(args: argparse.Namespace) -> None:
    pool = await _pool()
    async with pool.acquire() as conn:
        n = await conn.fetchval("SELECT reap_expired_leases()")
    log.info("reaped_leases", count=n)
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
    p_crawl.set_defaults(func=cmd_crawl)

    p_index = sub.add_parser("index", help="run the async indexer")
    p_index.set_defaults(func=cmd_index)

    p_reap = sub.add_parser("reap", help="run one lease-reap pass and exit (for cron)")
    p_reap.set_defaults(func=cmd_reap)

    args = parser.parse_args()
    asyncio.run(args.func(args))


if __name__ == "__main__":
    main()
