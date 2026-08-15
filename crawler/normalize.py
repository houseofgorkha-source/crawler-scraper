"""
URL normalization.

Dedup is only as good as normalization. Since the unique index is over
sha256(normalized_url), any inconsistency here silently creates duplicate
crawls that no amount of downstream cleverness recovers.

Rules are deliberately conservative: we only strip things that are
*provably* non-semantic. Aggressive stripping loses real pages.
"""
from __future__ import annotations

import re
from urllib.parse import urlsplit, urlunsplit, parse_qsl, urlencode, unquote

# Tracking params that never change page content.
_DROP_PARAMS = re.compile(
    r"^(utm_[a-z_]+|gclid|fbclid|mc_[ce]id|msclkid|igshid|ref|ref_src|"
    r"yclid|_ga|_gl|s_cid|vero_id|spm)$",
    re.IGNORECASE,
)

_DEFAULT_PORTS = {"http": "80", "https": "443"}
_INDEX_FILES = re.compile(r"/(index|default)\.(html?|php|aspx?)$", re.IGNORECASE)


def normalize(raw: str, base: str | None = None) -> str | None:
    """Return the canonical form of `raw`, or None if it is not crawlable."""
    from urllib.parse import urljoin

    if base:
        raw = urljoin(base, raw)

    raw = raw.strip()
    if not raw:
        return None

    parts = urlsplit(raw)

    scheme = parts.scheme.lower()
    if scheme not in ("http", "https"):
        return None  # mailto:, javascript:, tel:, data: ...

    host = parts.hostname.lower() if parts.hostname else None
    if not host:
        return None

    # www is almost always an alias; treat it as one. (Rare counterexamples
    # exist; they are worth the enormous dedup win.)
    if host.startswith("www."):
        host = host[4:]

    netloc = host
    if parts.port and str(parts.port) != _DEFAULT_PORTS.get(scheme):
        netloc = f"{host}:{parts.port}"

    # Path: percent-decode unreserved chars, collapse //, drop index files.
    path = unquote(parts.path) or "/"
    path = re.sub(r"//+", "/", path)
    path = _INDEX_FILES.sub("/", path)
    if len(path) > 1 and path.endswith("/"):
        path = path.rstrip("/")   # /article/ == /article
    if not path:
        path = "/"

    # Query: drop tracking params, sort the rest for stable ordering.
    query = urlencode(
        sorted(
            (k, v) for k, v in parse_qsl(parts.query, keep_blank_values=True)
            if not _DROP_PARAMS.match(k)
        )
    )

    # Fragments are client-side only. Always dropped.
    return urlunsplit((scheme, netloc, path, query, ""))


def registrable_host(url: str) -> str | None:
    """Host used for politeness grouping. All rate limiting keys off this."""
    h = urlsplit(url).hostname
    if not h:
        return None
    h = h.lower()
    return h[4:] if h.startswith("www.") else h


def robots_origin(url: str) -> str | None:
    """scheme://authority (with a non-default port preserved) for fetching
    robots.txt against the exact server a URL is actually served from.

    Deliberately separate from registrable_host(): that one intentionally
    drops scheme/port so politeness grouping and rate limiting stay per
    hostname. This one exists because robots.txt itself is scoped per
    origin (RFC 9309) -- a host on a non-standard port (or over plain
    HTTP) has its own robots.txt that only the real origin can serve.
    """
    parts = urlsplit(url)
    scheme = parts.scheme.lower()
    if scheme not in ("http", "https") or not parts.hostname:
        return None
    netloc = parts.hostname.lower()
    if parts.port and str(parts.port) != _DEFAULT_PORTS.get(scheme):
        netloc = f"{netloc}:{parts.port}"
    return f"{scheme}://{netloc}"
