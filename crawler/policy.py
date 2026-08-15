"""
Domain policy: robots.txt, crawl delay, identification.

Enforced TWICE by design:
  1. at SCHEDULE time  -- domains.next_available_at gates the claim query
  2. at FETCH time     -- check_allowed() immediately before the request

Point 2 is not redundant. Cached robots.txt goes stale, and a stale *allow*
is the failure mode that gets a crawler banned at the CDN layer. The cost of
re-checking an in-memory rule set is nanoseconds; the cost of being wrong is
losing access to the host permanently.
"""
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from urllib.robotparser import RobotFileParser

# Identify honestly. A contact URL is not etiquette -- anonymous broad
# crawlers get blocked by WAFs, which is a technical failure.
USER_AGENT = "MyCrawler/0.1 (+https://example.org/crawler; contact@example.org)"
ROBOTS_TTL = timedelta(hours=24)
DEFAULT_CRAWL_DELAY_MS = 1000
MIN_CRAWL_DELAY_MS = 500      # floor: never hammer, even if robots permits
MAX_CRAWL_DELAY_MS = 30_000   # ceiling: an absurd delay means "skip this host"


@dataclass(slots=True)
class DomainPolicy:
    host: str
    is_crawlable: bool
    crawl_delay_ms: int
    fetched_at: datetime | None
    _parser: RobotFileParser | None = None

    @property
    def is_stale(self) -> bool:
        if self.fetched_at is None:
            return True
        return datetime.now(timezone.utc) - self.fetched_at > ROBOTS_TTL

    def check_allowed(self, url: str) -> bool:
        if not self.is_crawlable:
            return False
        if self._parser is None:
            # No robots.txt resolved yet -> refuse. Fail closed, never open.
            return False
        return self._parser.can_fetch(USER_AGENT, url)


def parse_robots(host: str, body: str | None, status: int) -> DomainPolicy:
    """
    Interpretation of robots fetch outcomes, per the de-facto standard:
      2xx           -> obey the rules as written
      404/410       -> no restrictions, crawl freely
      401/403       -> treat the whole host as disallowed
      5xx / timeout -> treat as disallowed for now, retry later
    """
    now = datetime.now(timezone.utc)
    parser = RobotFileParser()

    if status in (401, 403):
        return DomainPolicy(host, False, DEFAULT_CRAWL_DELAY_MS, now)
    if status >= 500:
        return DomainPolicy(host, False, DEFAULT_CRAWL_DELAY_MS, None)
    if status in (404, 410) or not body:
        parser.parse([])
        return DomainPolicy(host, True, DEFAULT_CRAWL_DELAY_MS, now, parser)

    parser.parse(body.splitlines())

    delay_ms = DEFAULT_CRAWL_DELAY_MS
    declared = parser.crawl_delay(USER_AGENT)
    if declared:
        delay_ms = int(float(declared) * 1000)
    else:
        rr = parser.request_rate(USER_AGENT)
        if rr and rr.requests > 0:
            delay_ms = int(rr.seconds / rr.requests * 1000)

    delay_ms = max(MIN_CRAWL_DELAY_MS, min(delay_ms, MAX_CRAWL_DELAY_MS))
    crawlable = parser.can_fetch(USER_AGENT, f"https://{host}/")

    return DomainPolicy(host, crawlable, delay_ms, now, parser)
