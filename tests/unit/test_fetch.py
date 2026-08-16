import httpx

from crawler.contracts import CrawlTask, FetchOutcome
from crawler.fetch import HttpFetcher


def _task():
    return CrawlTask(
        url_id=1,
        url="https://example.com/test",
        host="example.com",
        depth=0,
        js_required=False,
    )


async def test_http_error_preserves_response_headers():
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(
            status_code=429,
            headers={
                "Retry-After": "30",
                "Content-Type": "text/html",
            },
            request=request,
        )

    client = httpx.AsyncClient(
        transport=httpx.MockTransport(handler),
        follow_redirects=True,
    )
    fetcher = HttpFetcher(client)

    try:
        result = await fetcher.fetch(_task())
    finally:
        await fetcher.aclose()

    assert result.outcome is FetchOutcome.HTTP_ERROR
    assert result.status_code == 429
    assert result.headers["retry-after"] == "30"
    assert result.content_type == "text/html"


async def test_http_error_preserves_response_body():
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(
            status_code=403,
            headers={"Content-Type": "text/html"},
            content=b"<html><title>Access Denied</title></html>",
            request=request,
        )

    client = httpx.AsyncClient(
        transport=httpx.MockTransport(handler),
        follow_redirects=True,
    )
    fetcher = HttpFetcher(client)

    try:
        result = await fetcher.fetch(_task())
    finally:
        await fetcher.aclose()

    assert result.outcome is FetchOutcome.HTTP_ERROR
    assert result.status_code == 403
    assert result.body == b"<html><title>Access Denied</title></html>"


async def test_http_error_records_challenge_type():
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(
            status_code=403,
            headers={"Content-Type": "text/html"},
            content=(
                b"<html><title>Just a Moment...</title>"
                b"<body>Checking your browser before accessing this site.</body></html>"
            ),
            request=request,
        )

    client = httpx.AsyncClient(
        transport=httpx.MockTransport(handler),
        follow_redirects=True,
    )
    fetcher = HttpFetcher(client)

    try:
        result = await fetcher.fetch(_task())
    finally:
        await fetcher.aclose()

    assert result.outcome is FetchOutcome.HTTP_ERROR
    assert result.status_code == 403
    assert result.challenge_type == "cloudflare_challenge"


async def test_normal_page_has_no_challenge_type():
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(
            status_code=200,
            headers={"Content-Type": "text/html"},
            content=b"<html><body>Normal lab page</body></html>",
            request=request,
        )

    client = httpx.AsyncClient(
        transport=httpx.MockTransport(handler),
        follow_redirects=True,
    )
    fetcher = HttpFetcher(client)

    try:
        result = await fetcher.fetch(_task())
    finally:
        await fetcher.aclose()

    assert result.outcome is FetchOutcome.OK
    assert result.challenge_type is None
