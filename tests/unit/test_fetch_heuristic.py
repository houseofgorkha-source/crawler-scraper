from crawler.fetch import needs_render


def test_empty_app_shell_triggers_render():
    body = b'<html><body><div id="root"></div></body></html>'
    assert needs_render(body) is True


def test_next_js_shell_triggers_render():
    body = b'<html><body><div id="__next"></div></body></html>'
    assert needs_render(body) is True


def test_full_static_page_does_not_trigger_render():
    body = (
        b"<html><body><main><h1>Article Title</h1>"
        + b"<p>" + b"word " * 200 + b"</p></main></body></html>"
    )
    assert needs_render(body) is False


def test_sparse_page_below_threshold_triggers_render():
    body = b"<html><body><p>too short</p></body></html>"
    assert needs_render(body) is True


def test_script_content_not_counted_as_visible_text():
    # A page whose only "text" is inside <script> should still read as
    # sparse -- script contents must not count toward the visible-text
    # threshold.
    body = b"<html><body><script>" + b"var x = 1; " * 100 + b"</script></body></html>"
    assert needs_render(body) is True
