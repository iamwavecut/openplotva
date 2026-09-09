"""Shared, versioned controller/worker contracts. No external side effects."""
from __future__ import annotations

import hashlib
import json
import re
from pathlib import PurePosixPath

REPOSITORY = "iamwavecut/openplotva"
OWNER = "iamwavecut"
OWNER_ID = 239034
OMP_VERSION = "18.1.14"
MODEL = "glm-5.3"
DAY = 86400
INITIAL_LIMIT = 30
DEEP_LIMIT = 10
INITIAL_SECONDS = 600
DEEP_SECONDS = 14400
MAX_ROUNDS = 5
LABELS = {"agent:created": "8250df", "agent:queued": "0969da",
          "needs-triage": "fbca04", "needs-human": "d93f0b"}


class Deferred(Exception):
    """A recoverable unavailable dependency or exhausted resource, with safe text."""


class QuotaUnavailable(Deferred):
    """The Coding Plan must recover before this retained job can run again."""

    def __init__(self, *, usage=None, active_seconds=0, retry_after_seconds=3600):
        super().__init__("GLM Coding Plan quota is temporarily unavailable")
        self.usage = dict(usage or {})
        self.active_seconds = active_seconds
        self.retry_after_seconds = max(60, min(86400, retry_after_seconds))


class InvalidResult(Exception):
    """Untrusted output failed validation; it cannot authorize publication."""


def fingerprint(value: object) -> str:
    return hashlib.sha256(json.dumps(value, sort_keys=True, separators=(",", ":")).encode()).hexdigest()


def identifier(value: str) -> str:
    if not isinstance(value, str) or not re.fullmatch(r"[a-zA-Z0-9_-]{1,100}", value):
        raise InvalidResult("invalid identifier")
    return value


def sha(value: str) -> str:
    if not isinstance(value, str) or not re.fullmatch(r"[a-f0-9]{40}", value):
        raise InvalidResult("invalid revision")
    return value


def text(value: object, limit: int = 12000) -> str:
    if not isinstance(value, str) or not value.strip() or len(value) > limit or "\x00" in value:
        raise InvalidResult("invalid text field")
    # Inputs originate in an allowlisted evidence projection. Block credential-shaped
    # strings again at the publication boundary, including accidental agent output.
    if re.search(r"(?i)(?:gh[pousr]_[a-z0-9]{15,}|github_pat_[a-z0-9_]+|sk-[a-z0-9_-]{16,}|"
                 r"\b\d{8,12}:[a-z0-9_-]{30,}|-----BEGIN [A-Z ]*PRIVATE KEY|"
                 r"(?:authorization|api[_-]?key|access[_-]?token)\s*[:=]\s*\S+)", value):
        raise InvalidResult("credential-shaped output rejected")
    return value


def diagnosis(value: object) -> dict:
    if not isinstance(value, dict):
        raise InvalidResult("diagnosis must be an object")
    required = {"external_cause", "code_defect", "observations", "hypotheses", "supporting",
                "contradicting", "related_changes", "missing", "next_action", "title", "summary",
                "matches", "acceptance"}
    if set(value) != required:
        raise InvalidResult("unexpected diagnosis fields")
    for key in ("external_cause", "code_defect"):
        if value[key] not in ("confirmed", "possible", "not_observed"):
            raise InvalidResult("invalid independent cause classification")
    if value["next_action"] not in ("observe", "investigate", "fix"):
        raise InvalidResult("invalid next action")
    for key in ("observations", "hypotheses", "supporting", "contradicting", "related_changes", "missing", "acceptance"):
        if not isinstance(value[key], list) or len(value[key]) > 30:
            raise InvalidResult("invalid evidence list")
        for item in value[key]:
            text(item, 4000)
    text(value["title"], 180)
    text(value["summary"])
    if not isinstance(value["matches"], list) or len(value["matches"]) > 20:
        raise InvalidResult("invalid semantic matches")
    for match in value["matches"]:
        if not isinstance(match, dict) or set(match) != {"kind", "number", "relationship", "reason"}:
            raise InvalidResult("invalid semantic match")
        if match["kind"] not in ("issue", "pr") or type(match["number"]) is not int or match["number"] <= 0:
            raise InvalidResult("invalid GitHub match reference")
        if match["relationship"] not in ("same_cause", "related", "regression", "new_evidence"):
            raise InvalidResult("invalid match relationship")
        text(match["reason"], 4000)
        if match["relationship"] == "new_evidence" and not (
            value["next_action"] == "fix" and value["code_defect"] == "confirmed"
            and value["supporting"] and value["acceptance"]
        ):
            raise InvalidResult("materially new evidence requires a supported functional defect")
    if value["next_action"] == "observe" and not (
        value["external_cause"] == "confirmed" and value["code_defect"] == "not_observed"
        and value["supporting"] and not value["missing"]
    ):
        raise InvalidResult("external-only observation requires evidence and no unresolved gaps")
    if value["next_action"] == "fix" and not (
        value["code_defect"] == "confirmed" and value["supporting"] and value["acceptance"]
    ):
        raise InvalidResult("a fix requires evidence and acceptance criteria")
    return value


def validate_output(value, stage):
    if stage == 'triage':
        if not isinstance(value, dict) or set(value) != {'action', 'reply', 'reason'}:
            raise InvalidResult('unexpected owner feedback decision fields')
        if value['action'] not in ('reply', 'continue', 'close_pr'):
            raise InvalidResult('invalid owner feedback action')
        text(value['reply'], 12000)
        text(value['reason'], 4000)
        return value
    if not isinstance(value, dict) or set(value) != {"diagnosis", "outcome", "feedback"}:
        raise InvalidResult("unexpected worker result fields")
    diagnosis(value["diagnosis"])
    if value["outcome"] not in ("patch", "no_fix", "needs_human"):
        raise InvalidResult("invalid worker outcome")
    if value["outcome"] == "patch" and (stage == "initial" or value["diagnosis"]["next_action"] != "fix"):
        raise InvalidResult("patch was not justified by a deep diagnosis")
    if not isinstance(value["feedback"], list) or len(value["feedback"]) > 100:
        raise InvalidResult("invalid feedback responses")
    for response in value["feedback"]:
        if not isinstance(response, dict) or set(response) != {"kind", "id", "action", "body"}:
            raise InvalidResult("invalid feedback response")
        if response["kind"] not in ("comment", "thread") or response["action"] not in ("fixed", "rebuttal"):
            raise InvalidResult("invalid feedback resolution")
        identifier(str(response["id"]))
        text(response["body"], 12000)
    return value


def safe_patch_path(value: str) -> bool:
    path = PurePosixPath(value)
    if path.is_absolute() or any(part in ("..", ".git") for part in path.parts) or "\\" in value:
        return False
    if not value or any(ord(c) < 32 for c in value):
        return False
    parts = [part.lower() for part in path.parts]
    # Publishing a change to the machinery controlling this agent needs the owner.
    if any(part in ("agents.md", "claude.md", "skill.md", ".omp", ".pi", ".cargo") for part in parts):
        return False
    if parts[0] in (".github", "deploy", "tools", "supply-chain"):
        return False
    if parts[-1] in ("dockerfile", "makefile", "justfile", "deny.toml", "audit.toml",
                     "rust-toolchain.toml", "rustfmt.toml", ".rustfmt.toml",
                     "clippy.toml", ".clippy.toml", ".pr_agent.toml", "dangerfile",
                     "dangerfile.js", "dangerfile.ts"):
        return False
    if parts[-1].startswith(("maintenance.", "maintenance_", "185_maintenance", "186_maintenance")):
        return False
    if parts[-1].startswith(".env") or parts[-1].endswith((".pem", ".key")):
        return False
    return value != "docs/CODEBASE_MAP.md"
