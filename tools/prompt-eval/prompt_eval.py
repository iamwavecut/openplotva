#!/usr/bin/env python3
"""Replay worker prompts against an OpenAI-compatible endpoint and score the outputs.

Stdlib only. See README.md for the fixture format and the check vocabulary.
"""

from __future__ import annotations

import argparse
import base64
import concurrent.futures
import html
import http.client
import json
import mimetypes
import os
import re
import statistics
import sys
import threading
import time
import urllib.parse
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

HERE = Path(__file__).resolve().parent
REPO = HERE.parent.parent
DEFAULT_FIXTURES = HERE / "fixtures"
LOCAL_DIR = HERE / "local"
SCHEMAS = HERE / "schemas"

ROLE_RE = re.compile(r"\{\{\s*role\s+\"(system|user|assistant)\"\s*\}\}")
IF_RE = re.compile(r"\{\{#if\s+(\w+)\s*\}\}(.*?)(?:\{\{else\}\}(.*?))?\{\{/if\}\}", re.S)
IF_EQ_RE = re.compile(
    r"\{\{#ifEquals\s+(\w+)\s+\"([^\"]*)\"\s*\}\}(.*?)(?:\{\{else\}\}(.*?))?\{\{/ifEquals\}\}", re.S
)
RAW_VAR_RE = re.compile(r"\{\{\{\s*(\w+)\s*\}\}\}|\{\{&\s*(\w+)\s*\}\}")
VAR_RE = re.compile(r"\{\{\s*(\w+)\s*\}\}")
EACH_RE = re.compile(r"\{\{#each\s+(\w+)\s*\}\}(.*?)\{\{/each\}\}", re.S)
THIS_RE = re.compile(r"\{\{\s*this\.(\w+)\s*\}\}")
FENCE_RE = re.compile(r"^\s*```(?:json)?\s*(.*?)\s*```\s*$", re.S)
CYRILLIC = re.compile(r"[А-Яа-яЁёІіЇїЄєҐґЎў]")
LATIN = re.compile(r"[A-Za-z]")
TAG_RE = re.compile(r"</?\s*([a-zA-Z][a-zA-Z0-9-]*)")
MEMORY_TASK_LINE = "Based on the window above, return the JSON object described in the system prompt."
QUOTE_EDGES = " \t\n\"'«»“”„.,!?;:"


def handlebars_escape(value: str) -> str:
    return (
        html.escape(value, quote=True).replace("&#x27;", "&#x27;").replace("`", "&#x60;").replace("=", "&#x3D;")
    )


def render_template(text: str, variables: dict[str, Any]) -> str:
    if text.startswith("---\n"):
        end = text.find("\n---\n", 4)
        if end != -1:
            text = text[end + 5 :]

    def truthy(name: str) -> bool:
        value = variables.get(name)
        return bool(value) and value not in ("0", "false")

    def each(match: re.Match) -> str:
        items = variables.get(match.group(1)) or []
        body = match.group(2)
        return "".join(
            THIS_RE.sub(lambda f: handlebars_escape(str(item.get(f.group(1), ""))), body)
            for item in items
            if isinstance(item, dict)
        )

    text = EACH_RE.sub(each, text)
    text = IF_EQ_RE.sub(
        lambda m: (m.group(3) if str(variables.get(m.group(1), "")) == m.group(2) else (m.group(4) or "")),
        text,
    )
    text = IF_RE.sub(lambda m: m.group(2) if truthy(m.group(1)) else (m.group(3) or ""), text)
    text = RAW_VAR_RE.sub(lambda m: str(variables.get(m.group(1) or m.group(2), "")), text)
    text = VAR_RE.sub(lambda m: handlebars_escape(str(variables.get(m.group(1), ""))), text)
    return text


def split_roles(text: str) -> list[dict[str, str]]:
    parts = ROLE_RE.split(text)
    if len(parts) == 1:
        return [{"role": "system", "content": text.strip()}]
    messages = []
    for index in range(1, len(parts), 2):
        content = parts[index + 1].strip()
        if content:
            messages.append({"role": parts[index], "content": content})
    return messages


def load_prompt(prompt_dir: Path, name: str, variables: dict[str, Any]) -> list[dict[str, str]]:
    path = prompt_dir / f"{name}.prompt"
    return split_roles(render_template(path.read_text(encoding="utf-8"), variables))


@dataclass
class Fixture:
    path: Path
    data: dict[str, Any]

    @property
    def id(self) -> str:
        return self.data.get("id") or f"{self.flow}/{self.path.stem}"

    @property
    def flow(self) -> str:
        return self.data["flow"]


def load_fixtures(roots: list[Path], flows: set[str] | None) -> list[Fixture]:
    fixtures = []
    for root in roots:
        if not root.exists():
            continue
        for path in sorted(root.rglob("*.json")):
            if "runs" in path.parts:
                continue
            data = json.loads(path.read_text(encoding="utf-8"))
            if "flow" not in data:
                continue
            if flows and data["flow"] not in flows:
                continue
            fixtures.append(Fixture(path, data))
    return fixtures


def load_schema(name: str | None) -> dict[str, Any] | None:
    if not name:
        return None
    return json.loads((SCHEMAS / f"{name}.json").read_text(encoding="utf-8"))


def data_url(path: Path) -> str:
    mime = mimetypes.guess_type(path.name)[0] or "application/octet-stream"
    return f"data:{mime};base64,{base64.b64encode(path.read_bytes()).decode('ascii')}"


def escape_prompt_attr(value: str) -> str:
    return value.replace("<", "&lt;").replace('"', "&quot;")


def render_memory_blocks(user: dict[str, Any]) -> str:
    """The memory extraction user message, as `ExtractInput::to_prompt_payload` renders it."""
    run = user.get("run", {})
    facts = {"chat_type": (user.get("chat_type") or "").strip()}
    for key, name in (("range_start_at", "window_start"), ("range_end_at", "window_end")):
        value = run.get(key) or ""
        if value and not value.startswith("0001-"):
            facts[name] = value
    facts = {key: value for key, value in facts.items() if value}
    lines = ["<run>", json.dumps(facts, ensure_ascii=False, separators=(",", ":")), "</run>", "<existing_cards>"]
    cards = sorted(user.get("existing_cards", []), key=lambda card: ((card.get("subject") or "").strip().lower(), card.get("id", 0)))
    lines.extend(json.dumps(card, ensure_ascii=False, separators=(",", ":")) for card in cards)
    lines += ["</existing_cards>", "<chat_window>"]
    for message in user.get("messages", []):
        attrs = []
        if message.get("message_id"):
            attrs.append(f'id="{message["message_id"]}"')
        if (message.get("entry_id") or "").strip():
            attrs.append(f'entry="{escape_prompt_attr(message["entry_id"].strip())}"')
        if message.get("user_id"):
            attrs.append(f'user="{message["user_id"]}"')
        author = (message.get("sender_name") or "").strip()
        if not author and (message.get("sender_username") or "").strip():
            author = "@" + message["sender_username"].strip().lstrip("@")
        if author:
            attrs.append(f'author="{escape_prompt_attr(author)}"')
        if message.get("occurred_at"):
            attrs.append(f'at="{message["occurred_at"]}"')
        if message.get("is_forwarded"):
            attrs.append('forwarded="true"')
        text = " ".join((message.get("text") or "").split()).replace("<", "&lt;")
        opening = "<msg " + " ".join(attrs) + ">" if attrs else "<msg>"
        lines.append(f"{opening}{text}</msg>")
    lines += ["</chat_window>", MEMORY_TASK_LINE]
    return "\n".join(lines)


def build_request(fixture: Fixture, prompt_dir: Path, args: argparse.Namespace) -> dict[str, Any] | None:
    data = fixture.data
    if "messages" in data:
        messages = [dict(message) for message in data["messages"]]
        if data.get("prompt") and messages and messages[0].get("role") == "system":
            rendered = [m for m in load_prompt(prompt_dir, data["prompt"], data.get("vars", {})) if m["role"] == "system"]
            if rendered:
                messages[0] = rendered[0]
        return finish_request(data, messages, args)
    messages = load_prompt(prompt_dir, data["prompt"], data.get("vars", {}))
    if data.get("user_prompt"):
        user_text = "\n".join(m["content"] for m in load_prompt(prompt_dir, data["user_prompt"], data.get("vars", {})))
    else:
        user_text = data.get("user")
    if user_text is not None:
        if data.get("user_layout") == "memory_blocks" and not args.legacy_user_layout:
            user_text = render_memory_blocks(user_text)
        elif not isinstance(user_text, str):
            user_text = json.dumps(user_text, ensure_ascii=False, indent=2)
        if data.get("image"):
            image = (HERE / data["image"]).resolve()
            if not image.exists():
                return None
            content = [
                {"type": "image_url", "image_url": {"url": data_url(image), "detail": "auto"}},
                {"type": "text", "text": user_text},
            ]
            messages.append({"role": "user", "content": content})
        else:
            messages.append({"role": "user", "content": user_text})
    return finish_request(data, messages, args)


def finish_request(data: dict[str, Any], messages: list[dict[str, Any]], args: argparse.Namespace) -> dict[str, Any]:
    request: dict[str, Any] = {"model": args.model, "messages": messages, "stream": False}
    request.update(data.get("request", {}))
    for item in args.set or []:
        key, _, raw = item.partition("=")
        try:
            request[key] = json.loads(raw)
        except json.JSONDecodeError:
            request[key] = raw
    schema = load_schema(data.get("schema"))
    mode = args.mode
    if schema is not None and mode == "response_format":
        request["response_format"] = {
            "type": "json_schema",
            "json_schema": {"name": data.get("tool_name") or data["flow"], "schema": schema},
        }
    elif schema is not None and mode == "tools":
        name = data.get("tool_name") or data["flow"]
        request["tools"] = [{"type": "function", "function": {"name": name, "parameters": schema}}]
        request["tool_choice"] = {"type": "function", "function": {"name": name}}
        request["parallel_tool_calls"] = False
    return request


def post(endpoint: str, request: dict[str, Any], headers: dict[str, str], timeout: float) -> tuple[int, Any, float]:
    url = urllib.parse.urlsplit(endpoint)
    if url.scheme not in ("http", "https") or not url.hostname:
        return 0, {"error": f"unsupported endpoint {endpoint!r}: expected an http(s) URL"}, 0.0
    connection_class = http.client.HTTPSConnection if url.scheme == "https" else http.client.HTTPConnection
    path = url.path or "/"
    if url.query:
        path += "?" + url.query
    body = json.dumps(request).encode("utf-8")
    started = time.monotonic()
    connection = connection_class(url.hostname, url.port, timeout=timeout)
    try:
        connection.request("POST", path, body=body, headers={"Content-Type": "application/json", **headers})
        response = connection.getresponse()
        text = response.read().decode("utf-8", "replace")
        if response.status != 200:
            return response.status, {"error": text[:2000]}, time.monotonic() - started
        return response.status, json.loads(text), time.monotonic() - started
    except (OSError, http.client.HTTPException, json.JSONDecodeError) as error:
        return 0, {"error": str(error)}, time.monotonic() - started
    finally:
        connection.close()


def response_text(payload: Any) -> str:
    try:
        message = payload["choices"][0]["message"]
    except (KeyError, IndexError, TypeError):
        return ""
    calls = message.get("tool_calls") or []
    if calls:
        return calls[0].get("function", {}).get("arguments") or ""
    return message.get("content") or ""


def parse_json_output(text: str) -> tuple[Any, str]:
    stripped = text.strip()
    fenced = FENCE_RE.match(stripped)
    note = ""
    if fenced:
        stripped = fenced.group(1)
        note = "fenced"
    try:
        return json.loads(stripped), note
    except json.JSONDecodeError:
        start, end = stripped.find("{"), stripped.rfind("}")
        if start != -1 and end > start:
            try:
                return json.loads(stripped[start : end + 1]), "salvaged"
            except json.JSONDecodeError:
                pass
    return None, "unparsable"


def walk(value: Any, path: str) -> list[Any]:
    if not path or path == ".":
        return [value]
    current = [value]
    for part in path.split("."):
        expand = part.endswith("[]")
        key = part[:-2] if expand else part
        nxt = []
        for item in current:
            if key:
                if not isinstance(item, dict) or key not in item:
                    continue
                item = item[key]
            if expand:
                if isinstance(item, list):
                    nxt.extend(item)
            else:
                nxt.append(item)
        current = nxt
    return current


def strings_in(value: Any) -> list[str]:
    if isinstance(value, str):
        return [value]
    if isinstance(value, dict):
        return [s for item in value.values() for s in strings_in(item)]
    if isinstance(value, list):
        return [s for item in value for s in strings_in(item)]
    return []


def schema_errors(value: Any, schema: dict[str, Any], path: str = "$") -> list[str]:
    errors = []
    expected = schema.get("type")
    types = expected if isinstance(expected, list) else [expected] if expected else []
    type_ok = {
        "object": lambda v: isinstance(v, dict),
        "array": lambda v: isinstance(v, list),
        "string": lambda v: isinstance(v, str),
        "integer": lambda v: isinstance(v, int) and not isinstance(v, bool),
        "number": lambda v: isinstance(v, (int, float)) and not isinstance(v, bool),
        "boolean": lambda v: isinstance(v, bool),
        "null": lambda v: v is None,
    }
    if types and not any(type_ok.get(t, lambda v: True)(value) for t in types):
        return [f"{path}: expected {'/'.join(types)}"]
    if "enum" in schema and value not in schema["enum"]:
        errors.append(f"{path}: {value!r} not in enum")
    if isinstance(value, dict):
        for key in schema.get("required", []):
            if key not in value:
                errors.append(f"{path}.{key}: missing")
        props = schema.get("properties", {})
        for key, item in value.items():
            if key in props:
                errors.extend(schema_errors(item, props[key], f"{path}.{key}"))
            elif schema.get("additionalProperties") is False:
                errors.append(f"{path}.{key}: not allowed")
    if isinstance(value, list):
        if "minItems" in schema and len(value) < schema["minItems"]:
            errors.append(f"{path}: fewer than {schema['minItems']} items")
        if "maxItems" in schema and len(value) > schema["maxItems"]:
            errors.append(f"{path}: more than {schema['maxItems']} items")
        if "items" in schema:
            for index, item in enumerate(value):
                errors.extend(schema_errors(item, schema["items"], f"{path}[{index}]"))
    return errors


def script_share(texts: list[str], lang: str) -> float:
    letters = "".join(texts)
    cyr = len(CYRILLIC.findall(letters))
    lat = len(LATIN.findall(letters))
    total = cyr + lat
    if total == 0:
        return 1.0
    return (cyr if lang in ("ru", "uk", "be") else lat) / total


def id_values(value: Any) -> list[int]:
    found = []
    if isinstance(value, dict):
        for key, item in value.items():
            if key.endswith("_id") and isinstance(item, int) and not isinstance(item, bool):
                found.append(item)
            elif key.endswith("_ids") and isinstance(item, list):
                found.extend(x for x in item if isinstance(x, int) and not isinstance(x, bool))
            else:
                found.extend(id_values(item))
    elif isinstance(value, list):
        for item in value:
            found.extend(id_values(item))
    return found


def input_ints(fixture: Fixture) -> set[int]:
    source = fixture.data.get("user")
    if source is None:
        source = [m.get("content") for m in fixture.data.get("messages", []) if m.get("role") == "user"]
    text = json.dumps(source, ensure_ascii=False)
    return {int(x) for x in re.findall(r"-?\d+", text)}


def comparable(text: str) -> str:
    folded = text.replace("ё", "е").replace("Ё", "е")
    for chars, target in (("«»“”„", '"'), ("’‘", "'"), ("—–", "-")):
        for char in chars:
            folded = folded.replace(char, target)
    return " ".join(folded.lower().split())


def quote_fragments(quote: str) -> list[str]:
    unescaped = quote.replace("&lt;", "<").replace("&quot;", '"')
    parts = [piece for part in unescaped.split("…") for piece in part.split("...")]
    return [fragment for fragment in (comparable(part.strip(QUOTE_EDGES)) for part in parts) if fragment]


def evidence_failures(fixture: Fixture, parsed: Any) -> list[str]:
    """Candidate cards whose evidence quote is not in a message they cite (the extraction gate rule)."""
    messages = (fixture.data.get("user") or {}).get("messages", [])
    failures = []
    for card in parsed.get("candidate_cards") or [] if isinstance(parsed, dict) else []:
        if not isinstance(card, dict):
            failures.append("non-object card")
            continue
        entries = {str(entry).strip() for entry in card.get("source_entry_ids") or []}
        ids = {value for value in card.get("source_message_ids") or [] if isinstance(value, int) and value}
        cited = [m for m in messages if (m.get("entry_id") or "").strip() in entries or m.get("message_id") in ids]
        fragments = quote_fragments(str(card.get("evidence_quote") or ""))
        if not cited or not fragments or not any(all(f in comparable(m.get("text") or "") for f in fragments) for m in cited):
            failures.append(str(card.get("fact_text") or "")[:40])
    return failures


def merge_plan_errors(fixture: Fixture, parsed: Any) -> list[str]:
    """Subject merge by index: the rules `validate_subject_merge_plan` enforces."""
    count = len((fixture.data.get("user") or {}).get("cards", []))
    if not isinstance(parsed, dict):
        return ["no object"]
    actions: dict[int, tuple[str, Any]] = {}
    for decision in parsed.get("decisions") or []:
        index = decision.get("index") if isinstance(decision, dict) else None
        if not isinstance(index, int) or not 0 <= index < count:
            return [f"unknown index {index}"]
        if index in actions:
            return [f"repeated index {index}"]
        action = str(decision.get("action") or "").strip().lower()
        if action not in ("keep", "cluster_with", "demote"):
            return [f"unknown action {action!r}"]
        actions[index] = (action, decision.get("survivor_index"))
    missing = [index for index in range(count) if index not in actions]
    if missing:
        return [f"missing indexes {missing[:5]}"]
    heads = set()
    for index, (action, survivor) in actions.items():
        if action != "cluster_with":
            continue
        if not isinstance(survivor, int) or survivor == index or actions.get(survivor, ("",))[0] != "keep":
            return [f"index {index} folds into {survivor}"]
        heads.add(survivor)
    texts = {
        entry.get("survivor_index")
        for entry in parsed.get("survivors") or []
        if isinstance(entry, dict) and str(entry.get("merged_fact_text") or "").strip()
    }
    lacking = sorted(heads - texts)
    return [f"survivors without text {lacking}"] if lacking else []


def resolution_choices(fixture: Fixture, parsed: Any) -> tuple[dict[int, dict[str, Any]], list[str]]:
    """Candidate resolution: the rules `apply_resolution_plan` enforces, plus the choice per candidate."""
    candidates = (fixture.data.get("user") or {}).get("candidates", [])
    if not isinstance(parsed, dict):
        return {}, ["no object"]
    choices: dict[int, dict[str, Any]] = {}
    for choice in parsed.get("decisions") or []:
        index = choice.get("candidate_index") if isinstance(choice, dict) else None
        if not isinstance(index, int) or not 0 <= index < len(candidates):
            return choices, [f"unknown candidate {index}"]
        if index in choices:
            return choices, [f"repeated candidate {index}"]
        action = str(choice.get("action") or "").strip().lower()
        if action not in ("add", "reinforce", "update", "supersede", "competing"):
            return choices, [f"unknown action {action!r}"]
        card = choice.get("card_index")
        if action != "add" and (not isinstance(card, int) or not 0 <= card < len(candidates[index].get("similar", []))):
            return choices, [f"candidate {index} names card {card}"]
        choices[index] = {"action": action, "card": card}
    missing = [index for index in range(len(candidates)) if index not in choices]
    return choices, [f"missing candidates {missing}"] if missing else []


def run_check(check: str, fixture: Fixture, raw: str, parsed: Any, parse_note: str) -> tuple[bool, str]:
    name, _, arg = check.partition(":")
    if name == "json":
        return parsed is not None, parse_note or "ok"
    if name == "strict_json":
        return parsed is not None and parse_note == "", parse_note or "ok"
    if parsed is None and name not in ("label", "lang", "no_phrases", "no_substring", "contains", "html_tags", "max_blank_run", "no_repeat_lines"):
        return False, "no json"
    if name == "schema":
        schema = load_schema(fixture.data.get("schema"))
        errors = schema_errors(parsed, schema) if schema else ["no schema"]
        return not errors, "; ".join(errors[:3]) or "ok"
    if name == "lang":
        lang, _, path = arg.partition(":")
        texts = strings_in(walk(parsed, path)) if (path and parsed is not None) else [raw]
        share = script_share(texts, lang)
        return share >= 0.9, f"{lang} share {share:.2f}"
    if name in ("no_phrases", "no_substring"):
        needle, scoped, field = arg.rpartition("@") if "@" in arg else (arg, "", "")
        if scoped and parsed is None:
            return False, "no json"
        haystack = "\n".join(strings_in(walk(parsed, field))) if scoped else raw
        if name == "no_substring":
            return needle not in haystack, f"found {needle!r}" if needle in haystack else "ok"
        hits = [p for p in needle.split(";") if p and p.lower() in haystack.lower()]
        return not hits, ", ".join(hits) or "ok"
    if name in ("max_items", "min_items"):
        path, _, limit = arg.rpartition(":")
        items = walk(parsed, path)
        count = len(items[0]) if items and isinstance(items[0], list) else 0
        ok = count <= int(limit) if name == "max_items" else count >= int(limit)
        return ok, f"{count} items"
    if name == "ids_from_input":
        allowed = input_ints(fixture) | {0}
        bad = sorted({i for i in id_values(parsed) if i not in allowed})
        return not bad, f"unknown ids {bad[:5]}" if bad else "ok"
    if name == "resolution_plan_valid":
        _, errors = resolution_choices(fixture, parsed)
        return not errors, "; ".join(errors) or "ok"
    if name == "decision":
        target, _, expected = arg.partition("=")
        actions, _, card = expected.partition("@")
        choice = resolution_choices(fixture, parsed)[0].get(int(target))
        ok = bool(choice) and choice["action"] in actions.split("|") and (not card or choice["card"] == int(card))
        return ok, f"{choice}" if choice else "no decision"
    if name == "merge_plan_valid":
        errors = merge_plan_errors(fixture, parsed)
        return not errors, "; ".join(errors) or "ok"
    if name == "evidence_quoted":
        failures = evidence_failures(fixture, parsed)
        return not failures, f"unquoted {failures[:3]}" if failures else "ok"
    if name == "min_count":
        target, _, limit = arg.rpartition(":")
        path, _, wanted = target.partition("=")
        count = sum(1 for value in walk(parsed, path) if str(value) == wanted)
        return count >= int(limit), f"{count} of {wanted}"
    if name == "separate":
        left, right = (int(part) for part in arg.split(","))
        heads = {}
        for decision in parsed.get("decisions") or [] if isinstance(parsed, dict) else []:
            if isinstance(decision, dict) and isinstance(decision.get("index"), int):
                folded = decision.get("action") == "cluster_with"
                heads[decision["index"]] = decision.get("survivor_index") if folded else decision["index"]
        together = left in heads and right in heads and heads[left] == heads[right]
        return not together, f"{left} and {right} folded together" if together else "ok"
    if name == "not_equals":
        path, _, unwanted = arg.partition("=")
        values = walk(parsed, path)
        hits = sum(1 for value in values if str(value) == unwanted)
        return hits == 0, f"{hits} of {len(values)}"
    if name == "label":
        first = next((line.strip() for line in raw.splitlines() if line.strip()), "")
        return first.startswith(arg), first[:40]
    if name == "contains":
        return arg in raw, "ok" if arg in raw else f"missing {arg!r}"
    if name == "equals":
        path, _, expected = arg.partition("=")
        values = walk(parsed, path)
        return bool(values) and all(str(v) == expected for v in values), str(values[:2])
    if name == "word_range":
        path, lo, hi = arg.rsplit(":", 2)
        counts = [len(s.split()) for s in strings_in(walk(parsed, path))]
        ok = bool(counts) and all(int(lo) <= c <= int(hi) for c in counts)
        return ok, f"words {counts}"
    if name == "html_tags":
        allowed = {t.strip().lower() for t in arg.split(",") if t.strip()}
        text = raw if parsed is None else "\n".join(strings_in(parsed))
        bad = sorted({t.lower() for t in TAG_RE.findall(text)} - allowed)
        return not bad, f"tags {bad}" if bad else "ok"
    if name == "max_blank_run":
        longest = max((m.count("\n") - 1 for m in re.findall(r"\n(?:[ \t]*\n)+", raw)), default=0)
        return longest <= int(arg), f"blank run {longest}"
    if name == "no_repeat_lines":
        lines = [line.strip() for line in raw.splitlines() if len(line.strip()) > 8]
        worst = max((lines.count(line) for line in set(lines)), default=0)
        return worst <= int(arg), f"max repeat {worst}"
    return False, f"unknown check {name}"


@dataclass
class Result:
    fixture: str
    flow: str
    status: int
    latency: float
    usage: dict[str, Any]
    checks: dict[str, tuple[bool, str]] = field(default_factory=dict)
    raw: str = ""
    skipped: bool = False
    finish_reason: str = ""


def run_suite(fixtures: list[Fixture], prompt_dir: Path, args: argparse.Namespace, out_dir: Path, label: str) -> list[Result]:
    headers = {}
    if args.api_key_env:
        key = os.environ.get(args.api_key_env, "")
        if key:
            headers["Authorization"] = f"Bearer {key}"
    for item in args.header or []:
        key, _, value = item.partition(":")
        headers[key.strip()] = value.strip()
    out_dir.mkdir(parents=True, exist_ok=True)
    jobs = [(fixture, attempt) for fixture in fixtures for attempt in range(args.runs)]
    lock = threading.Lock()
    log = (out_dir / f"{label}.jsonl").open("a", encoding="utf-8")

    def run_one(job: tuple[Fixture, int]) -> Result:
        fixture, attempt = job
        request = build_request(fixture, prompt_dir, args)
        if request is None:
            return Result(fixture.id, fixture.flow, 0, 0.0, {}, skipped=True)
        status, payload, latency = post(args.endpoint, request, headers, args.timeout)
        raw = response_text(payload) if status == 200 else json.dumps(payload)[:500]
        parsed, note = parse_json_output(raw) if status == 200 else (None, "http error")
        usage = payload.get("usage", {}) if isinstance(payload, dict) else {}
        finish = ""
        if isinstance(payload, dict) and payload.get("choices"):
            finish = str(payload["choices"][0].get("finish_reason") or "")
        result = Result(fixture.id, fixture.flow, status, latency, usage, raw=raw, finish_reason=finish)
        for check in fixture.data.get("checks", []):
            result.checks[check] = run_check(check, fixture, raw, parsed, note) if status == 200 else (False, f"HTTP {status}")
        with lock:
            log.write(json.dumps({
                "fixture": fixture.id, "attempt": attempt, "label": label, "status": status,
                "latency_s": round(latency, 3), "usage": result.usage, "finish_reason": finish, "raw": raw,
                "checks": {k: {"ok": v[0], "note": v[1]} for k, v in result.checks.items()},
            }, ensure_ascii=False) + "\n")
            log.flush()
            print(f"  {label} {fixture.id} #{attempt} HTTP {status} {latency:.1f}s {finish} "
                  + " ".join(f"{'✓' if ok else '✗'}{name.split(':')[0]}" for name, (ok, _) in result.checks.items()),
                  flush=True)
        return result

    with concurrent.futures.ThreadPoolExecutor(max_workers=max(1, args.concurrency)) as pool:
        results = list(pool.map(run_one, jobs))
    log.close()
    return results


def summarize(results: list[Result], label: str) -> str:
    lines = [f"## {label}", "", "| flow | runs | check | pass | first failures |", "|---|---|---|---|---|"]
    by_flow: dict[str, list[Result]] = {}
    for result in results:
        by_flow.setdefault(result.flow, []).append(result)
    for flow, items in sorted(by_flow.items()):
        done = [r for r in items if not r.skipped]
        checks = sorted({c for r in done for c in r.checks})
        for check in checks:
            outcomes = [r.checks[check] for r in done if check in r.checks]
            passed = sum(1 for ok, _ in outcomes if ok)
            failures = [f"{r.fixture.split('/')[-1]}: {r.checks[check][1]}" for r in done if check in r.checks and not r.checks[check][0]][:3]
            lines.append(f"| {flow} | {len(done)} | {check} | {passed}/{len(outcomes)} | {'; '.join(failures)} |")
        latencies = [r.latency for r in done if r.status == 200]
        if latencies:
            p95 = sorted(latencies)[max(0, int(len(latencies) * 0.95) - 1)]
            tokens_in = [r.usage.get("prompt_tokens", 0) for r in done]
            tokens_out = [r.usage.get("completion_tokens", 0) for r in done]
            lines.append(f"| {flow} | {len(done)} | latency p50/p95 s | {statistics.median(latencies):.1f}/{p95:.1f} | "
                         f"tokens in/out avg {statistics.mean(tokens_in):.0f}/{statistics.mean(tokens_out):.0f} |")
            truncated = sum(1 for r in done if r.finish_reason == "length")
            lines.append(f"| {flow} | {len(done)} | finish_reason=length | {truncated}/{len(done)} | |")
        skipped = [r for r in items if r.skipped]
        if skipped:
            lines.append(f"| {flow} | 0 | skipped | {len(skipped)} | missing local media |")
    return "\n".join(lines)


SELF_TEST_CASES = [
    ("json", '{"a": 1}', True),
    ("json", '```json\n{"a": 1}\n```', True),
    ("json", "not json at all", False),
    ("strict_json", '```json\n{"a": 1}\n```', False),
    ("lang:ru", "Кот спит на батарее", True),
    ("lang:ru", "The cat sleeps", False),
    ("no_phrases:but wait;let me think", '{"x": "fine"}', True),
    ("no_phrases:but wait;let me think", '{"x": "but wait, no"}', False),
    ("no_substring:|", '{"outputs": ["a cat"]}', True),
    ("no_substring:|", '{"outputs": ["a cat | ugly"]}', False),
    ("no_substring:|@outputs[]", '{"input": "cat | ugly", "outputs": ["a cat"]}', True),
    ("no_phrases:ugly@outputs[]", '{"input": "cat | ugly", "outputs": ["a cat"]}', True),
    ("no_phrases:ugly@outputs[]", '{"input": "cat", "outputs": ["an ugly cat"]}', False),
    ("max_items:candidate_cards:1", '{"candidate_cards": []}', True),
    ("max_items:candidate_cards:1", '{"candidate_cards": [1, 2]}', False),
    ("label:PROMPT:", "PROMPT: a cat", True),
    ("label:PROMPT:", "PROMTP: a cat", False),
    ("word_range:outputs[]:2:4", '{"outputs": ["one two three"]}', True),
    ("word_range:outputs[]:2:4", '{"outputs": ["one"]}', False),
    ("equals:nsfw_result=forbidden", '{"nsfw_result": "forbidden"}', True),
    ("equals:nsfw_result=forbidden", '{"nsfw_result": "adult"}', False),
    ("equals:aspect_ratio=2:3", '{"aspect_ratio": "2:3"}', True),
    ("contains:Скидки", '{"outputs": ["Скидки до 50%"]}', True),
    ("contains:Скидки", '{"outputs": ["Sale"]}', False),
    ("html_tags:b,i,u,a", "<b>x</b> <i>y</i>", True),
    ("html_tags:b,i,u,a", "<ul><li>x</li></ul>", False),
    ("max_blank_run:2", '{\n"a": 1\n}', True),
    ("max_blank_run:2", '{\n\n\n\n\n"a": 1}', False),
    ("no_repeat_lines:2", "line number one\nline number two", True),
    ("no_repeat_lines:2", "same line here\nsame line here\nsame line here", False),
    ("not_equals:cards[].type=event", '{"cards": [{"type": "decision"}]}', True),
    ("min_count:decisions[].action=demote:2", '{"decisions": [{"action": "demote"}, {"action": "demote"}]}', True),
    ("resolution_plan_valid", '{"decisions": [{"candidate_index": 0, "action": "reinforce", "card_index": 0}]}', True),
    ("resolution_plan_valid", '{"decisions": [{"candidate_index": 0, "action": "reinforce", "card_index": 3}]}', False),
    ("decision:0=update|reinforce@0", '{"decisions": [{"candidate_index": 0, "action": "reinforce", "card_index": 0}]}', True),
    ("decision:0=add", '{"decisions": [{"candidate_index": 0, "action": "reinforce", "card_index": 0}]}', False),
    ("min_count:decisions[].action=demote:2", '{"decisions": [{"action": "demote"}, {"action": "keep"}]}', False),
    ("separate:0,1", '{"decisions": [{"index": 0, "action": "keep"}, {"index": 1, "action": "keep"}]}', True),
    ("separate:0,1", '{"decisions": [{"index": 0, "action": "keep"}, {"index": 1, "action": "cluster_with", "survivor_index": 0}]}', False),
    ("not_equals:cards[].type=event", '{"cards": [{"type": "event"}]}', False),
    ("evidence_quoted", '{"candidate_cards": [{"fact_text": "x", "evidence_quote": "«НЕ ЕМ мясо…»", "source_message_ids": [5]}]}', True),
    ("evidence_quoted", '{"candidate_cards": [{"fact_text": "x", "evidence_quote": "люблю стейки", "source_message_ids": [5]}]}', False),
    ("evidence_quoted", '{"candidate_cards": [{"fact_text": "x", "evidence_quote": "не ем мясо", "source_message_ids": [6]}]}', False),
    ("merge_plan_valid", '{"decisions": [{"index": 0, "action": "keep"}, {"index": 1, "action": "cluster_with", "survivor_index": 0}], "survivors": [{"survivor_index": 0, "merged_fact_text": "m"}]}', True),
    ("merge_plan_valid", '{"decisions": [{"index": 0, "action": "keep"}], "survivors": []}', False),
    ("merge_plan_valid", '{"decisions": [{"index": 0, "action": "cluster_with", "survivor_index": 1}, {"index": 1, "action": "cluster_with", "survivor_index": 0}], "survivors": []}', False),
    ("ids_from_input", '{"old_card_id": 7}', True),
    ("ids_from_input", '{"old_card_id": 99}', False),
]


def self_test() -> int:
    rendered = render_template(
        "{{#each slots}}- slot {{this.index}}: {{this.model}}\n{{/each}}{{#if klein}}K{{/if}}",
        {"slots": [{"index": 0, "model": "A"}, {"index": 1, "model": "B"}], "klein": True},
    )
    assert rendered == "- slot 0: A\n- slot 1: B\nK", rendered
    fixture = Fixture(
        Path("self-test.json"),
        {
            "flow": "self_test",
            "user": {
                "cards": [{"index": 0}, {"index": 1}],
                "existing_cards": [{"id": 7}],
                "candidates": [{"index": 0, "similar": [{"index": 0}]}],
                "messages": [{"message_id": 5, "entry_id": "e5", "text": "Я больше не ем   мясо, честно"}],
            },
        },
    )
    failures = 0
    for check, raw, expected in SELF_TEST_CASES:
        parsed, note = parse_json_output(raw)
        ok, detail = run_check(check, fixture, raw, parsed, note)
        if ok != expected:
            failures += 1
            print(f"FAIL {check} on {raw!r}: got {ok} ({detail})")
    template = '{{role "system"}}Hi {{name}} {{#if ctx}}ctx={{{ctx}}}{{/if}}{{#ifEquals target "klein"}}K{{else}}B{{/ifEquals}}\n{{role "user"}}Q: {{q}}'
    messages = split_roles(render_template(template, {"name": "A&B", "ctx": "x'y", "target": "klein", "q": "why"}))
    if messages != [{"role": "system", "content": "Hi A&amp;B ctx=x'yK"}, {"role": "user", "content": "Q: why"}]:
        failures += 1
        print(f"FAIL render: {messages}")
    schema = {"type": "object", "required": ["a"], "properties": {"a": {"type": "array", "maxItems": 1, "items": {"enum": ["x"]}}}}
    if schema_errors({"a": ["x"]}, schema) or not schema_errors({"a": ["y", "x"]}, schema):
        failures += 1
        print("FAIL schema subset")
    blocks = render_memory_blocks(
        {
            "chat_type": "supergroup",
            "run": {"range_start_at": "0001-01-01T00:00:00Z", "range_end_at": "2023-11-14T22:13:20Z"},
            "existing_cards": [{"id": 42, "type": "event", "subject": "Bob", "fact": "b"}, {"id": 7, "type": "preference", "subject": "Ada", "fact": "a"}],
            "messages": [{"message_id": 10, "entry_id": "e1", "user_id": 42, "sender_name": 'Ann "A"', "occurred_at": "2023-11-14T22:13:20Z", "text": "line one\nline <two>"}],
        }
    )
    expected_blocks = "\n".join(
        [
            "<run>",
            '{"chat_type":"supergroup","window_end":"2023-11-14T22:13:20Z"}',
            "</run>",
            "<existing_cards>",
            '{"id":7,"type":"preference","subject":"Ada","fact":"a"}',
            '{"id":42,"type":"event","subject":"Bob","fact":"b"}',
            "</existing_cards>",
            "<chat_window>",
            '<msg id="10" entry="e1" user="42" author="Ann &quot;A&quot;" at="2023-11-14T22:13:20Z">line one line &lt;two></msg>',
            "</chat_window>",
            MEMORY_TASK_LINE,
        ]
    )
    if blocks != expected_blocks:
        failures += 1
        print(f"FAIL memory blocks:\n{blocks}")
    if failures:
        print(f"self-test failed: {failures}")
        return 1
    print("self-test ok")
    return 0


def render_requests(fixtures: list[Fixture], args: argparse.Namespace) -> int:
    for fixture in fixtures:
        request = build_request(fixture, args.prompt_dir, args)
        if request is None:
            print(f"# {fixture.id}: skipped (missing local media)")
            continue
        for message in request["messages"]:
            if isinstance(message.get("content"), list):
                for part in message["content"]:
                    if part.get("type") == "image_url":
                        part["image_url"]["url"] = part["image_url"]["url"][:48] + "..."
        print(f"# {fixture.id}")
        print(json.dumps(request, ensure_ascii=False, indent=2))
    return 0


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--self-test", action="store_true")
    parser.add_argument("--list", action="store_true")
    parser.add_argument("--render", action="store_true", help="print the built requests without sending them")
    parser.add_argument("--flow", action="append", help="limit to these flows")
    parser.add_argument("--fixtures", action="append", type=Path, help="fixture roots (default: fixtures/ and local/)")
    parser.add_argument("--endpoint", help="OpenAI-compatible chat/completions URL")
    parser.add_argument("--model", default="")
    parser.add_argument("--api-key-env", help="environment variable holding a bearer token")
    parser.add_argument("--header", action="append", help="extra header 'Key: value'")
    parser.add_argument("--prompt-dir", type=Path, default=REPO / "prompts")
    parser.add_argument("--compare", type=Path, help="second prompt tree to run on the same fixtures")
    parser.add_argument("--mode", choices=["response_format", "tools", "prompt_only"], default="response_format")
    parser.add_argument("--set", action="append", help="override a request field, e.g. temperature=0.7")
    parser.add_argument(
        "--legacy-user-layout",
        action="store_true",
        help="send memory fixtures as pretty JSON, the layout the memory prompts used before v6",
    )
    parser.add_argument("--runs", type=int, default=1)
    parser.add_argument("--concurrency", type=int, default=1, help="parallel requests")
    parser.add_argument("--timeout", type=float, default=180.0)
    parser.add_argument("--out", type=Path, default=LOCAL_DIR / "runs" / time.strftime("%Y%m%d-%H%M%S"))
    args = parser.parse_args()

    if args.self_test:
        return self_test()
    roots = args.fixtures or [DEFAULT_FIXTURES, LOCAL_DIR]
    fixtures = load_fixtures(roots, set(args.flow) if args.flow else None)
    if args.list:
        for fixture in fixtures:
            print(f"{fixture.flow:22} {fixture.id:48} {fixture.path.relative_to(HERE) if fixture.path.is_relative_to(HERE) else fixture.path}")
        print(f"{len(fixtures)} fixtures in {len({f.flow for f in fixtures})} flows")
        return 0
    if args.render:
        return render_requests(fixtures, args)
    if not args.endpoint:
        parser.error("--endpoint is required to run fixtures")
    report = []
    base = run_suite(fixtures, args.prompt_dir, args, args.out, "baseline")
    report.append(summarize(base, f"baseline — {args.prompt_dir}"))
    if args.compare:
        other = run_suite(fixtures, args.compare, args, args.out, "candidate")
        report.append(summarize(other, f"candidate — {args.compare}"))
    text = "\n\n".join(report)
    (args.out / "summary.md").write_text(text + "\n", encoding="utf-8")
    print("\n" + text)
    return 0


if __name__ == "__main__":
    sys.exit(main())
