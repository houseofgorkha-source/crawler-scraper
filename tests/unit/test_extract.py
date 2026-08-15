from crawler.contracts import CrawlTask, FetchOutcome, FetchResult, RenderMode
from crawler.extract import HtmlExtractor, hamming, simhash

TASK = CrawlTask(url_id=1, url="https://example.com/page", host="example.com", depth=0)


def _result(body: str, content_type: str = "text/html") -> FetchResult:
    return FetchResult(
        task=TASK, outcome=FetchOutcome.OK, status_code=200,
        final_url=TASK.url, body=body.encode("utf-8"),
        content_type=content_type, encoding="utf-8",
        render_mode=RenderMode.STATIC,
    )


def test_simhash_identical_text_identical_hash():
    text = "the quick brown fox jumps over the lazy dog " * 5
    assert simhash(text) == simhash(text)


def test_simhash_near_duplicate_within_distance():
    # Realistic use case per the module's own docstring: the same article
    # with a different ad block/timestamp/nav appended. A handful of
    # differing words at the end only shifts a small fraction of shingles
    # when the shared body is long enough -- short snippets don't have
    # this property, since a single differing word can dominate the vote.
    body = (
        "researchers announced a breakthrough in battery chemistry today "
        "that could extend electric vehicle range by forty percent while "
        "reducing charging time to under ten minutes according to the "
        "peer reviewed study published this week in a leading journal "
    ) * 3
    a = body + "published 2026-08-01"
    b = body + "published 2026-08-02"
    assert hamming(simhash(a), simhash(b)) <= 3


def test_simhash_unrelated_text_far_apart():
    a = "web crawlers discover pages by following hyperlinks recursively"
    b = "quarterly revenue grew twelve percent driven by cloud subscriptions"
    assert hamming(simhash(a), simhash(b)) > 3


def test_simhash_fits_in_64_bits():
    h = simhash("some arbitrary piece of text with enough words to shingle")
    assert 0 <= h < (1 << 64)


def test_hamming_self_distance_zero():
    h = simhash("any text")
    assert hamming(h, h) == 0


def test_hamming_symmetric():
    a = simhash("first document")
    b = simhash("a rather different second document")
    assert hamming(a, b) == hamming(b, a)


def test_extractor_pulls_title_and_text():
    html = """
    <html lang="en"><head><title>Test Page</title></head>
    <body><main><h1>Heading</h1><p>Some real content here.</p></main></body>
    </html>
    """
    doc = HtmlExtractor().extract(_result(html))
    assert doc is not None
    assert doc.title == "Test Page"
    assert "Some real content here." in doc.text
    assert doc.lang == "en"


def test_extractor_strips_boilerplate():
    html = """
    <html><head><title>T</title></head>
    <body>
      <nav>Nav links</nav>
      <main><p>Real article content.</p></main>
      <footer>Footer junk</footer>
      <script>var x = 1;</script>
    </body></html>
    """
    doc = HtmlExtractor().extract(_result(html))
    assert "Real article content." in doc.text
    assert "Nav links" not in doc.text
    assert "Footer junk" not in doc.text
    assert "var x" not in doc.text


def test_extractor_respects_noindex():
    html = """
    <html><head><title>T</title>
    <meta name="robots" content="noindex"></head>
    <body><main><p>Should not be indexed.</p></main></body></html>
    """
    assert HtmlExtractor().extract(_result(html)) is None


def test_extractor_discovers_and_normalizes_links():
    html = """
    <html><head><title>T</title></head>
    <body><main>
      <a href="/relative">Relative</a>
      <a href="https://example.com/relative">Duplicate</a>
      <a href="mailto:x@y.com">Not a link</a>
    </main></body></html>
    """
    doc = HtmlExtractor().extract(_result(html))
    urls = [l.url for l in doc.links]
    assert urls == ["https://example.com/relative"]  # dedup + mailto dropped


def test_extractor_returns_none_for_empty_body():
    result = FetchResult(
        task=TASK, outcome=FetchOutcome.OK, status_code=200,
        body=b"", render_mode=RenderMode.STATIC,
    )
    assert HtmlExtractor().extract(result) is None


def test_extractor_content_sha256_matches_visible_text():
    import hashlib
    html = "<html><head><title>T</title></head><body><main><p>Hello world</p></main></body></html>"
    doc = HtmlExtractor().extract(_result(html))
    assert doc.content_sha256 == hashlib.sha256(doc.text.encode()).digest()
