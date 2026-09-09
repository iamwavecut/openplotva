#!/usr/bin/env python3
import asyncio
import json
import os
import sys
from dataclasses import dataclass
from functools import partial

from litellm.litellm_core_utils.logging_worker import GLOBAL_LOGGING_WORKER
from pr_agent.algo.pr_processing import retry_with_fallback_models
from pr_agent.algo.utils import ModelType
from pr_agent.config_loader import get_settings
from pr_agent.git_providers.utils import apply_repo_settings
from pr_agent.log import get_logger, setup_logger
from pr_agent.tools.pr_code_suggestions import PRCodeSuggestions
from pr_agent.tools.pr_reviewer import PRReviewer

from glm_coding_plan import MODEL as GLM_MODEL, GlmCodingPlanHandler, GlmCompletionError

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


def env_json_list(name: str, default: list[str]) -> list[str]:
    value = os.environ.get(name)
    if value is None or value == "":
        return default
    parsed = json.loads(value)
    if not isinstance(parsed, list):
        raise ValueError(f"{name} must be a JSON list")
    return parsed


def configure_settings(pr_url: str) -> None:
    settings = get_settings()
    settings.set("CONFIG.CLI_MODE", True)
    settings.set("CONFIG.CONFIG_BRANCH", os.environ.get("PR_AGENT_CONFIG_BRANCH") or None)
    settings.set("CONFIG.EXTRA_CONFIG_URL", os.environ.get("PR_AGENT_EXTRA_CONFIG_URL") or "")

    apply_repo_settings(pr_url)

    settings.set("CONFIG.MODEL", os.environ.get("PR_AGENT_MODEL", "openai/glm-5.3"))
    settings.set("CONFIG.FALLBACK_MODELS", env_json_list("PR_AGENT_FALLBACK_MODELS", []))
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
    if settings.config.model != GLM_MODEL:
        return {}
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
    if isinstance(reviewer.ai_handler, GlmCodingPlanHandler):
        await reviewer._prepare_prediction(GLM_MODEL)
        reviewer.ai_handler.ensure_complete()
    else:
        await retry_with_fallback_models(reviewer._prepare_prediction, model_type=ModelType.REGULAR)
    if not reviewer.prediction:
        if isinstance(reviewer.ai_handler, GlmCodingPlanHandler):
            raise GlmCompletionError("empty_review")
        return ReviewResult(reviewer=reviewer, body="", has_findings=False)

    body = reviewer._prepare_pr_review()
    if isinstance(reviewer.ai_handler, GlmCodingPlanHandler) and not body.strip():
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
    if isinstance(suggester.ai_handler, GlmCodingPlanHandler):
        data = await suggester.prepare_prediction_main(GLM_MODEL)
        suggester.ai_handler.ensure_complete()
        if not isinstance(data, dict) or not isinstance(data.get("code_suggestions"), list):
            raise GlmCompletionError("invalid_suggestions")
    else:
        data = await retry_with_fallback_models(suggester.prepare_prediction_main, model_type=ModelType.REGULAR)
    if not data or "code_suggestions" not in data:
        data = {"code_suggestions": []}

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
        return await run()
    finally:
        await asyncio.wait_for(GLOBAL_LOGGING_WORKER.flush(), timeout=15)
        await GLOBAL_LOGGING_WORKER.stop()


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
