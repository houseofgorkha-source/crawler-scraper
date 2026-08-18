from unittest.mock import AsyncMock

from crawler.challenge import RendererChallengeResolver
from crawler.contracts import (
    Challenge,
    ChallengeResolution,
    CrawlTask,
    FetchOutcome,
    FetchResult,
    RenderMode,
)


def _task():
    return CrawlTask(
        url_id=1,
        url="https://example.com/p",
        host="example.com",
        depth=0,
        js_required=False,
    )


def _challenge():
    return Challenge(
        challenge_type="cloudflare_challenge",
        status_code=403,
        url="https://example.com/p",
        headers={"content-type": "text/html"},
        body=b"<html>Just a moment...</html>",
    )


async def test_no_renderer_returns_unresolved():
    resolver = RendererChallengeResolver(None)

    result = await resolver.resolve(_challenge(), _task())

    assert result.outcome is ChallengeResolution.UNRESOLVED
    assert result.fetch_result is None
    assert result.error_detail == "no renderer configured"


async def test_renderer_still_returns_challenge():
    renderer = AsyncMock()
    renderer.render.return_value = FetchResult(
        task=_task(),
        outcome=FetchOutcome.OK,
        status_code=403,
        final_url="https://example.com/p",
        body=b"<html>Just a moment...</html>",
        render_mode=RenderMode.RENDERED,
    )

    resolver = RendererChallengeResolver(renderer)

    result = await resolver.resolve(_challenge(), _task())

    assert result.outcome is ChallengeResolution.UNRESOLVED
    assert result.fetch_result is not None
    assert result.error_detail == "challenge remained after rendering"
    renderer.render.assert_awaited_once_with(_task())


async def test_renderer_returns_clean_page():
    renderer = AsyncMock()
    renderer.render.return_value = FetchResult(
        task=_task(),
        outcome=FetchOutcome.OK,
        status_code=200,
        final_url="https://example.com/p",
        body=b"<html><body><h1>Real page</h1></body></html>",
        render_mode=RenderMode.RENDERED,
    )

    resolver = RendererChallengeResolver(renderer)

    result = await resolver.resolve(_challenge(), _task())

    assert result.outcome is ChallengeResolution.RESOLVED
    assert result.fetch_result is not None
    assert result.fetch_result.status_code == 200
    renderer.render.assert_awaited_once_with(_task())