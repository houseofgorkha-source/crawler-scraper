from crawler.normalize import normalize, registrable_host


def test_strips_default_https_port():
    assert normalize("https://example.com:443/page") == "https://example.com/page"


def test_keeps_non_default_port():
    assert normalize("https://example.com:8443/page") == "https://example.com:8443/page"


def test_strips_www():
    assert normalize("https://www.example.com/page") == "https://example.com/page"


def test_lowercases_host():
    assert normalize("https://EXAMPLE.com/Page") == "https://example.com/Page"


def test_rejects_non_http_schemes():
    assert normalize("mailto:a@b.com") is None
    assert normalize("javascript:void(0)") is None
    assert normalize("tel:+1234567890") is None
    assert normalize("data:text/plain;base64,aGVsbG8=") is None


def test_rejects_empty_or_hostless():
    assert normalize("") is None
    assert normalize("   ") is None


def test_drops_fragment():
    assert normalize("https://example.com/page#section") == "https://example.com/page"


def test_collapses_trailing_slash_except_root():
    assert normalize("https://example.com/article/") == "https://example.com/article"
    assert normalize("https://example.com/") == "https://example.com/"
    assert normalize("https://example.com") == "https://example.com/"


def test_collapses_double_slashes_in_path():
    assert normalize("https://example.com/a//b") == "https://example.com/a/b"


def test_strips_index_files():
    # index-file substitution produces "/dir/", then the trailing-slash
    # collapse (which runs after) strips it down to "/dir" -- both stages
    # are exercised here, not just the index-file rule in isolation.
    assert normalize("https://example.com/dir/index.html") == "https://example.com/dir"
    assert normalize("https://example.com/dir/default.php") == "https://example.com/dir"
    assert normalize("https://example.com/index.html") == "https://example.com/"


def test_drops_tracking_params():
    result = normalize("https://example.com/page?utm_source=x&id=5&fbclid=y")
    assert result == "https://example.com/page?id=5"


def test_sorts_remaining_query_params():
    result = normalize("https://example.com/page?b=2&a=1")
    assert result == "https://example.com/page?a=1&b=2"


def test_relative_url_resolved_against_base():
    result = normalize("/other.html", base="https://example.com/dir/page.html")
    assert result == "https://example.com/other.html"


def test_same_page_different_scheme_case_normalizes_identically():
    a = normalize("HTTPS://WWW.Example.com:443/Path/")
    b = normalize("https://example.com/Path")
    assert a == b


def test_registrable_host_strips_www_and_port():
    assert registrable_host("https://www.example.com:8080/x") == "example.com"


def test_registrable_host_none_for_hostless():
    assert registrable_host("mailto:a@b.com") is None
