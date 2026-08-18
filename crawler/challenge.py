from __future__ import annotations

from .contracts import (
    Challenge,
    ChallengeResolution,
    ChallengeResolutionResult,
    ChallengeResolver,
    CrawlTask,
    FetchOutcome,
    FetchResult,
    ScrapeTask,
)
from .fetch import detect_challenge


class RendererChallengeResolver:
    """Resolve browser-based challenges using the existing renderer.

    The resolver is deliberately narrow:
    - receives the detected challenge and original task
    - renders the task through the existing renderer
    - considers the challenge resolved only when the rendered response
      no longer contains a recognized challenge
    - leaves unresolved challenges on the normal failure path
    """

    def __init__(self, renderer):
        self.renderer = renderer

    async def resolve(
        self,
        challenge: Challenge,
        task: CrawlTask | ScrapeTask,
    ) -> ChallengeResolutionResult:
        if self.renderer is None:
            return ChallengeResolutionResult(
                outcome=ChallengeResolution.UNRESOLVED,
                error_detail="no renderer configured",
            )

        try:
            result: FetchResult = await self.renderer.render(task)
        except Exception as exc:
            return ChallengeResolutionResult(
                outcome=ChallengeResolution.UNRESOLVED,
                error_detail=f"challenge resolution failed: {type(exc).__name__}: {exc}",
            )

        if result.outcome is not FetchOutcome.OK:
            return ChallengeResolutionResult(
                outcome=ChallengeResolution.UNRESOLVED,
                fetch_result=result,
                error_detail="renderer did not produce a successful response",
            )

        if detect_challenge(result.body, result.status_code) is not None:
            return ChallengeResolutionResult(
                outcome=ChallengeResolution.UNRESOLVED,
                fetch_result=result,
                error_detail="challenge remained after rendering",
            )

        return ChallengeResolutionResult(
            outcome=ChallengeResolution.RESOLVED,
            fetch_result=result,
        )
