from crawler.fetch import classify_http_error


def test_403_is_forbidden():
    assert classify_http_error(403) == "forbidden"


def test_429_is_rate_limited():
    assert classify_http_error(429) == "rate_limited"


def test_503_is_server_error():
    assert classify_http_error(503) == "server_error"


def test_404_is_client_error():
    assert classify_http_error(404) == "client_error"


def test_200_is_other():
    assert classify_http_error(200) == "other"
