# Reply leak guard: prompt, context and transcript echo protection

## Problem

Production chat history (2026-09-06..14, ~91k model replies) contains ~20 replies
per day that echo the system contract, the runtime `<chat_context>` (persona
accent, custom persona, shield block, memory chunks) or a copy of the chat
transcript, plus a few per day that wrap the answer in JSON/XML envelopes with
history field names (`reply_to_id`, `to_user`). 165 of 191 attributable leaks
came from the `gemini-2.5-flash-lite` fallback, 26 from Gemma.

The existing guard (`finalize_dialog_reply`) only recognises a fixed list of
scaffolding markers **at the start** of the reply. Real leaks look different:

- `<system_contract><identity>…` / `<custom_persona>…</custom_persona><base_voice>…` —
  contract dumps under tags the guard never knew (`identity`, `base_voice`,
  `dialog_policy`, `answer_policy`, `daily_persona_accent`, …). The Telegram
  HTML sanitizer strips the tags and the bare instruction text reaches the chat.
- `<answer><text>ok</text><daily_persona_accent>…</daily_persona_accent></answer>` —
  the answer is fine but travels with a self-review that leaks the persona.
- `<context><message id=…>…</message></context>…<answer><text>reply</text></answer>` —
  transcript echo followed by the real answer.
- `<think>Thinking Process: …</think>` in the **middle** of the content.
- `[{"reply_to_id": "28815", "to_user": "Наталья", "text": "…"}]` — JSON envelope.
- Tag-less copies of the persona accent (`Theatrical, loud, witty tone. Drag
  culture quotes…`) appended after a valid reply.
- Gemini's `INSTRUCTION_LEAK_PATTERNS` still match the Go-era `=== СИСТЕМНЫЕ ПРАВИЛА`
  headers and never fire on the XML contract.

A static marker list drifts every time a prompt changes. The guard must derive
what is secret from the request that was actually sent.

## Design

### `openplotva_dialog::ReplyLeakGuard` (new module `leak_guard.rs`)

Built once per model step from the rendered request and the dialog input:

| Source | Index | Rule |
|---|---|---|
| system message(s), `<chat_context>` message minus the long-window spans | 6-word shingles (normalised words) + scaffolding tag names | any shingle hit, any protected tag, or a snake_case contract identifier (`base_voice`) named in prose → `PromptLeak` |
| `<reference_context>` chunks (memory), `<custom_persona>`, `<daily_persona_accent>` | 8-word shingles | shingle hit → `PromptLeak` (short facts and persona catchphrases stay reusable) |
| participant-authored history turns (sender, text) and the current message; the bot only as a speaker label | per-entry 8-word shingles, exact normalised text, sender names | exact echo of one entry, ≥2 distinct entries echoed, or ≥2 lines labelled `<known sender>:` → `TranscriptLeak` |

Tags, identifiers and labels are matched outside code spans (``` fences,
`<pre>`, `<code>`), so a reply explaining markup is not a leak.

Normalisation: tags removed, lowercase, every non-alphanumeric run is a word
separator. Shingles are hashed with `DefaultHasher`; the index lives only for
the step.

Protected tag names are derived from the rendered prompt: every `<name` whose
name contains `_` or is at least 8 characters long (so `system_contract`,
`identity`, `base_voice`, `daily_persona_accent`, `instruction` count while
`text`, `name`, `rule`, `b`, `pre` never do). A static set of history/context
wrapper tags (`assistant_message`, `last_message`, `message_type`, `to_user`,
`chat_context`, `reference_context`, `daily_persona_accent`, `custom_persona`,
`shield_context`, `system_contract`, and `<message` with an `id`/`thread_id`/
`timestamp` attribute) applies even when the guard is built without a prompt,
so `finalize_dialog_reply(content)` keeps working with an empty guard.

### Recovery before rejection (in `sanitize_assistant_text`)

- Closed reasoning blocks (`<think>`, `<thought>`, `<analysis>`, `<thinking>`,
  `<reasoning>`) are removed wherever they appear; an unclosed one that opens a
  line is a reasoning leak; an inline mention (`в <think> теги`) stays.
- An answer envelope (`<answer>`, `<final_answer>`, `<response>`,
  `<final_response>`, `<output>`) yields its inner text (the `<text>` bodies when
  present); sibling self-review elements are dropped.
- A reply that is one JSON object/array (optionally fenced) carrying a
  transcript key (`reply_to_id`, `reply_to_user`, `to_user`, `message_id`,
  `sender`) yields the first string under
  `answer`/`response`/`text`/`content`/`message`/`reply`; such a document
  without any text is `ProtocolOnly`. JSON without transcript keys may be what
  the user asked for and is left alone.

### `finalize_dialog_reply_with_guard(content, &guard)`

Pipeline: empty → sanitize (recoveries above) → existing residual-marker check
→ JSON envelope recovery → leading context envelope without a recovered answer
envelope → `ContextLeak` → guard rules on the recovered answer → pathological.
`finalize_dialog_reply(content)` delegates with `ReplyLeakGuard::default()`.

New `DialogReplySuppression::{PromptLeak, TranscriptLeak}`; both retryable in
both providers (aifarm `FinalAnswerPromptLeak` / `FinalAnswerContextLeak`,
gemini `ProviderProtocolError` reasons), so the turn engine regenerates.

### Provider wiring

`openplotva_llm::aifarm::reply_leak_guard(messages, input)` builds the guard
from the exact `ChatMessage`s of the step (system + `<chat_context>` roles) and
`input.history` / `input.message`. aifarm applies it to the final answer, to
the text delivered next to tool calls and to every `send_message` step text;
gemini applies it to the final answer. All of these were already the only
paths through which model text reaches the chat.

## Out of scope

- Prompt wording changes (cache-sensitive; the guard is the fix).
- The Go-era `INSTRUCTION_LEAK_PATTERNS` list in gemini stays as a harmless
  belt-and-braces filter.
- Paraphrased (non-verbatim) instruction summaries and English chain-of-thought
  prose without tags are not detectable by fingerprinting; they are rare in the
  sample and remain a model-quality problem.
