from crawler.contracts import FetchOutcome, FetchResult, RenderMode, ScrapeTask
from crawler.scrape_extract import (
    FieldSpec, HtmlRecordExtractor, ScrapeSpec, spec_from_row,
)

TASK = ScrapeTask(target_id=1, url="https://example.com/list", host="example.com", spec_id=1)


def _result(body: str) -> FetchResult:
    return FetchResult(
        task=TASK, outcome=FetchOutcome.OK, status_code=200,
        final_url=TASK.url, body=body.encode("utf-8"),
        content_type="text/html", encoding="utf-8", render_mode=RenderMode.STATIC,
    )


def _spec(fields, **kwargs) -> ScrapeSpec:
    return ScrapeSpec(id=1, name="test", version=1, fields=fields, **kwargs)


def test_flat_field_extraction():
    html = "<html><body><h1 class='t'>Hello</h1></body></html>"
    spec = _spec([FieldSpec(name="title", selector="h1.t")])
    record = HtmlRecordExtractor().extract(_result(html), spec)
    assert record.data == {"title": "Hello"}


def test_missing_field_is_none():
    html = "<html><body><p>no title here</p></body></html>"
    spec = _spec([FieldSpec(name="title", selector="h1.t")])
    record = HtmlRecordExtractor().extract(_result(html), spec)
    assert record.data == {"title": None}


def test_attr_extraction():
    html = '<html><body><a class="l" href="/page">x</a></body></html>'
    spec = _spec([FieldSpec(name="href", selector="a.l", attr="href")])
    record = HtmlRecordExtractor().extract(_result(html), spec)
    assert record.data == {"href": "https://example.com/page"}  # absolutized


def test_many_flat_extraction():
    html = """<html><body>
        <span class="p">1</span><span class="p">2</span><span class="p">3</span>
    </body></html>"""
    spec = _spec([FieldSpec(name="prices", selector="span.p", many=True)])
    record = HtmlRecordExtractor().extract(_result(html), spec)
    assert record.data == {"prices": ["1", "2", "3"]}


def test_nested_repeated_extraction():
    html = """<html><body>
        <div class="item"><h2 class="n">A</h2><span class="p">10</span></div>
        <div class="item"><h2 class="n">B</h2><span class="p">20</span></div>
    </body></html>"""
    spec = _spec([
        FieldSpec(name="items", selector="div.item", many=True, fields=[
            FieldSpec(name="name", selector="h2.n"),
            FieldSpec(name="price", selector="span.p"),
        ]),
    ])
    record = HtmlRecordExtractor().extract(_result(html), spec)
    assert record.data == {
        "items": [{"name": "A", "price": "10"}, {"name": "B", "price": "20"}],
    }


def test_xpath_selector():
    html = "<html><body><h1>XPath Title</h1></body></html>"
    spec = _spec([FieldSpec(name="title", selector="//h1/text()", selector_type="xpath")])
    record = HtmlRecordExtractor().extract(_result(html), spec)
    assert record.data == {"title": "XPath Title"}


def test_link_field_produces_normalized_discovered_link():
    html = '<html><body><a class="next" href="/page/2">Next</a></body></html>'
    spec = _spec(
        [FieldSpec(name="next_page", selector="a.next", attr="href")],
        link_field="next_page",
    )
    record = HtmlRecordExtractor().extract(_result(html), spec)
    assert len(record.links) == 1
    assert record.links[0].url == "https://example.com/page/2"


def test_link_field_missing_produces_no_links():
    html = "<html><body><p>no link here</p></body></html>"
    spec = _spec(
        [FieldSpec(name="next_page", selector="a.next", attr="href")],
        link_field="next_page",
    )
    record = HtmlRecordExtractor().extract(_result(html), spec)
    assert record.links == []


def test_empty_body_returns_none():
    result = FetchResult(
        task=TASK, outcome=FetchOutcome.OK, status_code=200,
        body=b"", render_mode=RenderMode.STATIC,
    )
    spec = _spec([FieldSpec(name="title", selector="h1")])
    assert HtmlRecordExtractor().extract(result, spec) is None


def test_spec_from_row_rebuilds_nested_fields():
    row = {
        "id": 5, "name": "n", "version": 1,
        "fields": [
            {"name": "items", "selector": "div.item", "many": True,
             "fields": [{"name": "title", "selector": "h2"}]},
        ],
        "render_mode": "auto", "link_field": None,
        "feed_to_crawler": False, "feed_from_crawler": False, "is_active": True,
    }
    spec = spec_from_row(row)
    assert spec.fields[0].many is True
    assert spec.fields[0].fields[0].name == "title"
