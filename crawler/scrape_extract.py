"""
Structured extraction for the Scraper: CSS/XPath selectors against a JSON-
shaped field spec, producing a ScrapedRecord. Deliberately narrow -- no
form submission, no sessions, no interactive workflows. A field spec is
declarative and idempotent: same DOM in, same record out.

Uses lxml (not selectolax) because it natively supports both CSS (via
cssselect) and XPath in one parse. extract.py's HtmlExtractor keeps
selectolax unchanged -- that engine choice was made for the Crawler's
parse-and-discard workload and this doesn't touch it.
"""
from __future__ import annotations

from dataclasses import dataclass, field

from lxml import html as lxml_html

from .contracts import DiscoveredLink, FetchResult, ScrapedRecord
from .normalize import normalize

_VALID_SELECTOR_TYPES = ("css", "xpath")
_VALID_RENDER_MODES = ("auto", "always", "never")


@dataclass(slots=True)
class FieldSpec:
    name: str
    selector: str
    selector_type: str = "css"          # "css" | "xpath"
    attr: str | None = None             # e.g. "href", "src"; None => text content
    many: bool = False                  # True => extract a list of matches
    fields: list["FieldSpec"] | None = None   # nested sub-fields (per match, if `many`)


@dataclass(slots=True)
class ScrapeSpec:
    id: int
    name: str
    version: int
    fields: list[FieldSpec]
    render_mode: str = "auto"
    link_field: str | None = None       # field whose value(s) feed the crawl frontier
    feed_to_crawler: bool = False
    feed_from_crawler: bool = False
    is_active: bool = True


def spec_from_row(row: dict) -> ScrapeSpec:
    """Build a ScrapeSpec from a scrape_specs row (fields already jsonb-decoded)."""
    return ScrapeSpec(
        id=row["id"],
        name=row["name"],
        version=row["version"],
        fields=[_field_from_dict(f) for f in row["fields"]],
        render_mode=row["render_mode"],
        link_field=row["link_field"],
        feed_to_crawler=row["feed_to_crawler"],
        feed_from_crawler=row["feed_from_crawler"],
        is_active=row["is_active"],
    )


def _field_from_dict(d: dict) -> FieldSpec:
    return FieldSpec(
        name=d["name"],
        selector=d["selector"],
        selector_type=d.get("selector_type", "css"),
        attr=d.get("attr"),
        many=d.get("many", False),
        fields=[_field_from_dict(f) for f in d["fields"]] if d.get("fields") else None,
    )


class HtmlRecordExtractor:
    """RecordExtractor implementation: CSS/XPath field extraction via lxml.

    Mirrors HtmlExtractor's role for the Crawler -- injected into
    ScrapeWorker the same way (extractor=None -> HtmlRecordExtractor()),
    so the Scraper's extraction stage is swappable through the same kind
    of seam the Crawler's already has, not just conceptually parallel to it.
    """

    def extract(self, result: FetchResult, spec: ScrapeSpec) -> ScrapedRecord | None:
        if not result.has_body:
            return None

        tree = lxml_html.fromstring(result.body)
        base = result.final_url or result.task.url
        tree.make_links_absolute(base)

        data = _extract_fields(tree, spec.fields)

        links: list[DiscoveredLink] = []
        if spec.link_field:
            raw = data.get(spec.link_field)
            candidates = raw if isinstance(raw, list) else ([raw] if raw else [])
            for href in candidates:
                if not href:
                    continue
                n = normalize(href, base)
                if n:
                    links.append(DiscoveredLink(url=n))

        return ScrapedRecord(
            target_id=result.task.target_id,
            spec_id=spec.id,
            data=data,
            links=links,
        )


def _select(node, f: FieldSpec) -> list:
    if f.selector_type == "xpath":
        return node.xpath(f.selector)
    return node.cssselect(f.selector)


def _value(match, attr: str | None) -> str | None:
    if isinstance(match, str):
        # xpath text()/@attr selections come back as plain strings already
        return match.strip() or None
    if attr:
        v = match.get(attr)
        return v.strip() if v else None
    text = match.text_content()
    return text.strip() if text else None


def _extract_fields(node, fields: list[FieldSpec]) -> dict:
    out: dict = {}
    for f in fields:
        matches = _select(node, f)
        if f.many:
            if f.fields:
                out[f.name] = [_extract_fields(m, f.fields) for m in matches]
            else:
                out[f.name] = [v for m in matches if (v := _value(m, f.attr)) is not None]
        else:
            m = matches[0] if matches else None
            if m is None:
                out[f.name] = None
            elif f.fields:
                out[f.name] = _extract_fields(m, f.fields)
            else:
                out[f.name] = _value(m, f.attr)
    return out
