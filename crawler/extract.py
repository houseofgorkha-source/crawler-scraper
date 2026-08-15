"""
Extraction: HTML -> ExtractedDoc.

Two fingerprints, doing different jobs:
  * content_sha256 -- exact duplicates. Free, catches mirrors and the
    trailing-slash class of URL aliases that normalization missed.
  * simhash        -- near duplicates. Catches the same article with a
    different ad block, timestamp or nav. This is most of the real
    duplication on the web, and exact hashing never sees it.

Near-duplicates are STORED but not INDEXED (documents.duplicate_of). Deleting
them loses the link graph edges they contribute.
"""
from __future__ import annotations

import hashlib
import re
from collections import Counter

from selectolax.parser import HTMLParser

from .contracts import DiscoveredLink, ExtractedDoc, FetchResult, RenderMode
from .normalize import normalize

_BOILERPLATE = "script, style, nav, header, footer, aside, noscript, iframe, form"
_WORD_RE = re.compile(r"\w+", re.UNICODE)
_MASK64 = (1 << 64) - 1


def simhash(text: str, ngram: int = 4) -> int:
    """64-bit SimHash over token shingles. Hamming distance <= 3 ~= duplicate."""
    tokens = _WORD_RE.findall(text.lower())
    if len(tokens) < ngram:
        shingles = ["".join(tokens)] if tokens else []
    else:
        shingles = ["".join(tokens[i:i + ngram])
                    for i in range(len(tokens) - ngram + 1)]

    vector = [0] * 64
    for shingle, weight in Counter(shingles).items():
        h = int.from_bytes(
            hashlib.blake2b(shingle.encode(), digest_size=8).digest(), "big"
        )
        for bit in range(64):
            vector[bit] += weight if (h >> bit) & 1 else -weight

    out = 0
    for bit, v in enumerate(vector):
        if v > 0:
            out |= 1 << bit
    return out & _MASK64


def hamming(a: int, b: int) -> int:
    return ((a ^ b) & _MASK64).bit_count()


class HtmlExtractor:
    def __init__(self, max_depth: int = 6):
        self.max_depth = max_depth

    def extract(self, result: FetchResult) -> ExtractedDoc | None:
        if not result.has_body:
            return None

        tree = HTMLParser(result.body.decode(result.encoding or "utf-8", "replace"))
        base = result.final_url or result.task.url

        # Respect rel=canonical: it is the publisher telling us the true
        # identity of this page. Cheapest dedup signal available.
        canonical = base
        if (node := tree.css_first('link[rel="canonical"]')) is not None:
            if href := node.attributes.get("href"):
                canonical = normalize(href, base) or base

        if (m := tree.css_first('meta[name="robots"]')) is not None:
            if "noindex" in (m.attributes.get("content") or "").lower():
                return None

        links = self._links(tree, base)

        for node in tree.css(_BOILERPLATE):
            node.decompose()

        title = tree.css_first("title")
        title_text = title.text(strip=True) if title else None

        desc = None
        if (d := tree.css_first('meta[name="description"]')) is not None:
            desc = d.attributes.get("content")

        body_node = tree.css_first("main") or tree.css_first("article") or tree.body
        text = re.sub(r"\s+", " ", body_node.text(separator=" ") if body_node else "").strip()

        return ExtractedDoc(
            url_id=result.task.url_id,
            canonical_url=canonical,
            title=title_text,
            description=desc,
            text=text,
            lang=self._lang(tree),
            word_count=len(text.split()),
            content_sha256=hashlib.sha256(text.encode()).digest(),
            simhash=simhash(text),
            render_mode=result.render_mode,
            links=links,
        )

    def _links(self, tree: HTMLParser, base: str) -> list[DiscoveredLink]:
        seen: set[str] = set()
        out: list[DiscoveredLink] = []
        for a in tree.css("a[href]"):
            href = a.attributes.get("href")
            if not href:
                continue
            url = normalize(href, base)
            if not url or url in seen:
                continue
            seen.add(url)
            out.append(DiscoveredLink(
                url=url,
                anchor_text=(a.text(strip=True) or None),
                rel=a.attributes.get("rel"),
            ))
        return out

    @staticmethod
    def _lang(tree: HTMLParser) -> str | None:
        html = tree.css_first("html")
        if html and (lang := html.attributes.get("lang")):
            return lang.split("-")[0].lower()
        return None
