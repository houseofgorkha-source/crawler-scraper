"""
Worker contracts.

Every stage is a pure function over these types. Stages never call each other
directly -- they exchange these objects through the queue. That is what makes
a Python fetch worker replaceable by a Go one later without touching anything
downstream: the contract is the interface, not the implementation.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import Protocol, Sequence


class RenderMode(str, Enum):
    STATIC = "static"       # plain HTTP fetch was sufficient
    RENDERED = "rendered"   # escalated to a headless browser


class FetchOutcome(str, Enum):
    OK = "ok"
    NOT_MODIFIED = "not_modified"   # 304 / etag match -- no body, reschedule only
    ROBOTS_DENIED = "robots_denied"
    HTTP_ERROR = "http_error"
    NETWORK_ERROR = "network_error"
    NON_HTML = "non_html"
    TOO_LARGE = "too_large"


# --------------------------------------------------------------------------
# Stage 1 -> 2 : Frontier hands work to a fetch worker
# --------------------------------------------------------------------------
@dataclass(frozen=True, slots=True)
class CrawlTask:
    url_id: int
    url: str
    host: str
    depth: int
    etag: str | None = None
    last_modified: str | None = None
    js_required: bool = False       # domain-level hint: skip the static attempt
    lease_expires_at: datetime | None = None


# --------------------------------------------------------------------------
# Stage 2 -> 3 : fetch worker hands bytes to the extractor
# --------------------------------------------------------------------------
@dataclass(slots=True)
class FetchResult:
    task: CrawlTask | "ScrapeTask"
    outcome: FetchOutcome
    status_code: int | None = None
    final_url: str | None = None            # after redirects; may differ from task.url
    body: bytes | None = None
    content_type: str | None = None
    encoding: str | None = None
    etag: str | None = None
    last_modified: str | None = None
    render_mode: RenderMode = RenderMode.STATIC
    duration_ms: int = 0
    error_class: str | None = None
    error_detail: str | None = None

    @property
    def has_body(self) -> bool:
        return self.outcome is FetchOutcome.OK and bool(self.body)


# --------------------------------------------------------------------------
# Stage 3 -> 4 : extractor hands structured content to persistence + indexing
# --------------------------------------------------------------------------
@dataclass(slots=True)
class DiscoveredLink:
    url: str                # already normalized
    anchor_text: str | None = None
    rel: str | None = None


@dataclass(slots=True)
class ExtractedDoc:
    url_id: int
    canonical_url: str
    title: str | None
    description: str | None
    text: str
    lang: str | None
    word_count: int
    content_sha256: bytes
    simhash: int
    render_mode: RenderMode
    links: Sequence[DiscoveredLink] = field(default_factory=list)


# --------------------------------------------------------------------------
# Scraper: a peer of the Crawler, not a mode of it. Same fetch/render
# substrate (HttpFetcher, PlaywrightRenderer, BlobStore all accept either
# task type unchanged -- they only touch .url/.etag/.last_modified), but a
# distinct task/result shape because a scrape target isn't a `urls` row and
# a scraped record isn't an `ExtractedDoc`. See crawler/scrape_extract.py
# for the spec format and crawler/scrape_worker.py for the loop.
# --------------------------------------------------------------------------
@dataclass(frozen=True, slots=True)
class ScrapeTask:
    target_id: int
    url: str
    host: str
    spec_id: int
    render_mode: str = "auto"       # "auto" | "always" | "never" -- from the spec
    etag: str | None = None
    last_modified: str | None = None


@dataclass(slots=True)
class ScrapedRecord:
    target_id: int
    spec_id: int
    data: dict                      # spec-shaped structured fields
    links: Sequence[DiscoveredLink] = field(default_factory=list)


# --------------------------------------------------------------------------
# Protocols. Implementations live in their own modules; anything satisfying
# these can be swapped in (including a different language behind an RPC shim).
# --------------------------------------------------------------------------
class Frontier(Protocol):
    async def claim(self, worker_id: str, batch: int, lease_s: int) -> list[CrawlTask]: ...
    async def renew(self, url_id: int, worker_id: str, lease_s: int) -> bool: ...
    async def complete(self, result: FetchResult, doc: ExtractedDoc | None) -> None: ...
    async def fail(self, result: FetchResult) -> None: ...
    async def add(self, links: Sequence[DiscoveredLink], from_url_id: int, depth: int) -> int: ...


class Fetcher(Protocol):
    async def fetch(self, task: CrawlTask | ScrapeTask) -> FetchResult: ...


class Renderer(Protocol):
    async def render(self, task: CrawlTask | ScrapeTask) -> FetchResult: ...


class Extractor(Protocol):
    def extract(self, result: FetchResult) -> ExtractedDoc | None: ...


class RecordExtractor(Protocol):
    def extract(self, result: FetchResult, spec: "ScrapeSpec") -> ScrapedRecord | None: ...


class BlobStore(Protocol):
    async def put(self, key: str, data: bytes, content_type: str) -> str: ...
    async def get(self, key: str) -> bytes: ...


class SearchIndex(Protocol):
    async def upsert(self, docs: Sequence[ExtractedDoc]) -> None: ...
