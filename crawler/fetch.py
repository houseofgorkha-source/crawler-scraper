"""
Tier 1 fetch (plain HTTP) + escalation decision.

Escalation is the single biggest cost lever in the system. A browser page is
~100x the memory and ~10x the latency of an HTTP GET. So: static first,
always, unless the domain is already known to require rendering. Escalate
only on evidence, and record that evidence so the domain stops paying the
wasted static attempt.
"""
from __future__ import annotations

import re
import time

import httpx

from .contracts import CrawlTask, FetchOutcome, FetchResult, RenderMode, ScrapeTask
from .policy import USER_AGENT

MAX_BODY_BYTES = 5 * 1024 * 1024
CONNECT_TIMEOUT = 10.0
READ_TIMEOUT = 20.0

_HTML_TYPES = ("text/html", "application/xhtml+xml")
_SCRIPT_RE = re.compile(rb"<script[^>]*>.*?</script>", re.DOTALL | re.IGNORECASE)
_TAG_RE = re.compile(rb"<[^>]+>")

# Root elements that frameworks mount into. An empty one is strong evidence
# that the real content arrives via JS.
_APP_SHELL_RE = re.compile(
    rb"""<(?:div|main)[^>]+id=["'](?:root|app|__next|__nuxt)["'][^>]*>\s*</""",
    re.IGNORECASE,
)

MIN_STATIC_TEXT = 400   # chars of visible text below which we suspect a shell


def needs_render(body: bytes) -> bool:
    """Heuristic: did the static HTML actually contain the content?"""
    if _APP_SHELL_RE.search(body):
        return True
    visible = _TAG_RE.sub(b" ", _SCRIPT_RE.sub(b"", body))
    return len(visible.split()) < MIN_STATIC_TEXT // 5


class HttpFetcher:
    def __init__(self, client: httpx.AsyncClient | None = None):
        self._client = client or httpx.AsyncClient(
            headers={
                "User-Agent": USER_AGENT,
                "Accept": "text/html,application/xhtml+xml;q=0.9,*/*;q=0.5",
                "Accept-Encoding": "gzip, deflate, br",
            },
            timeout=httpx.Timeout(READ_TIMEOUT, connect=CONNECT_TIMEOUT),
            follow_redirects=True,
            max_redirects=5,
            http2=True,
        )

    async def fetch(self, task: CrawlTask | ScrapeTask) -> FetchResult:
        started = time.perf_counter()

        # Conditional request: a 304 costs almost nothing and is the cheapest
        # possible recrawl. Worth doing on every revisit.
        headers: dict[str, str] = {}
        if task.etag:
            headers["If-None-Match"] = task.etag
        if task.last_modified:
            headers["If-Modified-Since"] = task.last_modified

        def elapsed() -> int:
            return int((time.perf_counter() - started) * 1000)

        try:
            async with self._client.stream("GET", task.url, headers=headers) as resp:
                if resp.status_code == 304:
                    return FetchResult(task, FetchOutcome.NOT_MODIFIED, 304,
                                       duration_ms=elapsed())
                if resp.status_code >= 400:
                    return FetchResult(task, FetchOutcome.HTTP_ERROR, resp.status_code,
                                       duration_ms=elapsed())

                ctype = resp.headers.get("content-type", "")
                if not any(t in ctype for t in _HTML_TYPES):
                    return FetchResult(task, FetchOutcome.NON_HTML, resp.status_code,
                                       content_type=ctype, duration_ms=elapsed())

                chunks, total = [], 0
                async for chunk in resp.aiter_bytes():
                    total += len(chunk)
                    if total > MAX_BODY_BYTES:
                        return FetchResult(task, FetchOutcome.TOO_LARGE,
                                           resp.status_code, duration_ms=elapsed())
                    chunks.append(chunk)

                return FetchResult(
                    task=task,
                    outcome=FetchOutcome.OK,
                    status_code=resp.status_code,
                    final_url=str(resp.url),
                    body=b"".join(chunks),
                    content_type=ctype,
                    encoding=resp.encoding,
                    etag=resp.headers.get("etag"),
                    last_modified=resp.headers.get("last-modified"),
                    render_mode=RenderMode.STATIC,
                    duration_ms=elapsed(),
                )

        except httpx.HTTPError as exc:
            return FetchResult(task, FetchOutcome.NETWORK_ERROR,
                               duration_ms=elapsed(),
                               error_class=type(exc).__name__,
                               error_detail=str(exc)[:500])

    async def aclose(self) -> None:
        await self._client.aclose()
