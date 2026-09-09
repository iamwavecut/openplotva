#!/usr/bin/env python3
import asyncio
import os
import sys
from dataclasses import dataclass
from functools import partial

from litellm.litellm_core_utils.logging_worker import GLOBAL_LOGGING_WORKER
from pr_agent.config_loader import get_settings
from pr_agent.git_providers.utils import apply_repo_settings
from pr_agent.log import get_logger, setup_logger
from pr_agent.tools.pr_code_suggestions import PRCodeSuggestions
from pr_agent.tools.pr_reviewer import PRReviewer

from glm_coding_plan import MODEL as GLM_MODEL, MODELS as GLM_MODELS, GlmCodingPlanHandler, GlmCompletionError, GlmQuotaUnavailable
from review_execution import ReviewExecution

NO_MAJOR_ISSUES_MARKER = "No major issues detected"


@dataclass
class ReviewResult:
    reviewer: PRReviewer | None
    body: str
    has_findings: bool


@dataclass
class SuggestionsResult:
    suggester: PRCodeSuggestions | None
    data: dict
    has_findings: bool


def env_bool(name: str, default: bool = False) -> bool:
    value = os.environ.get(name)
    if value is None or value == "":
        return default
    return value.strip().lower() in {"1", "true", "yes", "on"}


def env_int(name: str, default: int) -> int:
    value = os.environ.get(name)
    if value is None or value == "":
        return default
    return int(value)


def configure_settings(pr_url: str) -> None:
    settings = get_settings()
    settings.set("CONFIG.CLI_MODE", True)
    settings.set("CONFIG.CONFIG_BRANCH", os.environ.get("PR_AGENT_CONFIG_BRANCH") or None)
    settings.set("CONFIG.EXTRA_CONFIG_URL", os.environ.get("PR_AGENT_EXTRA_CONFIG_URL") or "")

    apply_repo_settings(pr_url)

    settings.set("CONFIG.MODEL", os.environ.get("PR_AGENT_MODEL", GLM_MODEL))
    settings.set("CONFIG.FALLBACK_MODELS", [])
    settings.set("CONFIG.AI_TIMEOUT", env_int("PR_AGENT_AI_TIMEOUT", 600))
    settings.set("CONFIG.REASONING_EFFORT", os.environ.get("PR_AGENT_REASONING_EFFORT", "low"))
    settings.set("CONFIG.CUSTOM_MODEL_MAX_TOKENS", env_int("PR_AGENT_CUSTOM_MODEL_MAX_TOKENS", 1000000))
    settings.set("CONFIG.MAX_MODEL_TOKENS", env_int("PR_AGENT_MAX_MODEL_TOKENS", 1000000))
    settings.set("CONFIG.PUBLISH_OUTPUT", False)
    settings.set("CONFIG.PUBLISH_OUTPUT_PROGRESS", False)
    settings.set("LITELLM.DROP_PARAMS", True)

    settings.set("GITHUB.PUBLISH_AS_CHECK_RUN", False)

    settings.set("PR_REVIEWER.REQUIRE_TESTS_REVIEW", False)
    settings.set("PR_REVIEWER.PERSISTENT_COMMENT", False)
    settings.set("PR_REVIEWER.FINAL_UPDATE_MESSAGE", False)
    settings.set(
        "PR_REVIEWER.PUBLISH_OUTPUT_NO_SUGGESTIONS",
        env_bool("PR_AGENT_PUBLISH_NO_FINDINGS", False),
    )

    settings.set("PR_CODE_SUGGESTIONS.PERSISTENT_COMMENT", False)
    settings.set("PR_CODE_SUGGESTIONS.PUBLISH_OUTPUT_NO_SUGGESTIONS", False)
    settings.set("PR_CODE_SUGGESTIONS.FOCUS_ONLY_ON_PROBLEMS", True)

    review_instructions = os.environ.get("PR_AGENT_REVIEW_EXTRA_INSTRUCTIONS")
    if review_instructions:
        settings.set("PR_REVIEWER.EXTRA_INSTRUCTIONS", review_instructions)
        settings.set("PR_CODE_SUGGESTIONS.EXTRA_INSTRUCTIONS", review_instructions)


def ai_handler_options() -> dict:
    settings = get_settings()
    if settings.config.model not in GLM_MODELS:
        raise GlmCompletionError('unsupported_model')
    return {"ai_handler": partial(
        GlmCodingPlanHandler,
        api_key=settings.get("OPENAI.KEY") or os.environ.get("OPENAI_KEY"),
        timeout=settings.config.ai_timeout,
    )}


async def generate_review(pr_url: str) -> ReviewResult:
    reviewer = PRReviewer(pr_url, **ai_handler_options())
    if not reviewer.git_provider.get_files():
        get_logger().info("PR has no files, skipping review")
        return ReviewResult(reviewer=None, body="", has_findings=False)

    get_logger().info("Generating PR review")
    await reviewer._prepare_prediction(get_settings().config.model)
    reviewer.ai_handler.ensure_complete()
    if not reviewer.prediction:
        raise GlmCompletionError("empty_review")

    body = reviewer._prepare_pr_review()
    if not body.strip():
        raise GlmCompletionError("invalid_review")
    has_findings = bool(body.strip()) and NO_MAJOR_ISSUES_MARKER not in body
    return ReviewResult(reviewer=reviewer, body=body, has_findings=has_findings)


async def generate_suggestions(pr_url: str) -> SuggestionsResult:
    if env_bool("PR_AGENT_SKIP_IMPROVE", False):
        get_logger().info("Skipping code suggestions because the PR exceeds configured improve limits")
        return SuggestionsResult(suggester=None, data={"code_suggestions": []}, has_findings=False)

    suggester = PRCodeSuggestions(pr_url, **ai_handler_options())
    if not suggester.git_provider.get_files():
        get_logger().info("PR has no files, skipping code suggestions")
        return SuggestionsResult(suggester=suggester, data={"code_suggestions": []}, has_findings=False)

    get_logger().info("Generating PR code suggestions")
    data = await suggester.prepare_prediction_main(get_settings().config.model)
    suggester.ai_handler.ensure_complete()
    if not isinstance(data, dict) or not isinstance(data.get("code_suggestions"), list):
        raise GlmCompletionError("invalid_suggestions")

    suggestions = data.get("code_suggestions") or []
    return SuggestionsResult(suggester=suggester, data=data, has_findings=bool(suggestions))


async def publish_results(review: ReviewResult, suggestions: SuggestionsResult) -> None:
    settings = get_settings()
    settings.set("CONFIG.PUBLISH_OUTPUT", True)
    settings.set("CONFIG.PUBLISH_OUTPUT_PROGRESS", False)

    publish_no_findings = env_bool("PR_AGENT_PUBLISH_NO_FINDINGS", False)

    if review.reviewer and review.body and (review.has_findings or publish_no_findings):
        review.reviewer.git_provider.publish_comment(review.body)
        get_logger().info("Published PR-Agent review comment")
    else:
        get_logger().info("No actionable review findings to publish")

    if suggestions.suggester and suggestions.has_findings:
        await suggestions.suggester.push_inline_code_suggestions(suggestions.data)
        get_logger().info("Published PR-Agent inline code suggestions")
    else:
        get_logger().info("No actionable code suggestions to publish")


async def run() -> int:
    setup_logger(os.environ.get("LOG_LEVEL", "INFO"))
    pr_url = os.environ["PR_URL"]
    configure_settings(pr_url)

    review = await generate_review(pr_url)
    suggestions = await generate_suggestions(pr_url)

    await publish_results(review, suggestions)

    return 0


async def main() -> int:
    try:
        execution = ReviewExecution(os.environ)
        await execution.start()
        try:
            status = await run()
        except GlmQuotaUnavailable as error:
            await execution.finish('quota_wait', error.retry_after_seconds)
            print('Review deferred: usage quota unavailable; the controller will resume this review.', flush=True)
            return 75
        except Exception:
            await execution.finish('failed')
            raise
        await execution.finish('complete')
        return status
    finally:
        await asyncio.wait_for(GLOBAL_LOGGING_WORKER.flush(), timeout=15)
        await GLOBAL_LOGGING_WORKER.stop()


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
