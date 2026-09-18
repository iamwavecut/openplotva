# Format contagion in the dialog reply: design

## The problem

Every rejected Gemma dialog reply since the 2026-09-17 deploy is the same shape: the
model answers, but wraps the answer in the XML envelope the prompt uses for history.

```
<message id="136767" thread_id="136412" timestamp="2026-09-17T22:10:05Z">
  <user type="user">Веселое время</user>
  <message_type>text</message_type>
  <text>Кринж, конечно, но зато честно.</text>
</message>
```

Measured on production, first 70 minutes after the deploy (`llm_request_events`):

| fact | value |
|---|---|
| rejected Gemma replies | 168, **all** starting with `<message …>` |
| of those, `<text>` genuinely new (not copied from context) | 139 of 157 (89%) |
| rejection rate, text-only turns | 18.6% (93 of 499) |
| rejection rate, turns carrying an image | 41.8% (64 of 153) |
| turns that exhausted the re-sampling budget | 16 of 30, every rejection the same reason |

So the guard is right that the envelope must not reach the chat, and wrong about the
reply inside it: a correct answer is thrown away 89% of the time. Re-sampling at
temperature 0.2 changes the words, not the envelope, which is why 16 turns burned five
generations and still fell to a model without tools.

The cause is not prompt extraction. It is format contagion: the last thing the model
sees before generating is raw XML (`<last_message><message …>`), and it continues in the
form it was handed. The image turns are worse because the XML block sits directly against
the generation point after the image parts. Reported for local models elsewhere
(continue.dev discussion #10534, Hugging Face forum "repeats its prompt as output").

## What this changes

Three layers, each measured before it ships.

### A. The engine must not be able to write the envelope

`bad_words` in the vLLM chat request (present in 0.25.1, forwarded by the farm proxy).
Gemma 4's vocabulary has no whole-tag tokens, so `<message` is banned as the sequence
`<` + `message`; Telegram HTML (`<b>`, `<a href`, `<code>`) stays legal because only the
listed continuations are masked after `<`. The native tool call is the special token
`<|tool_call>` and is untouched.

Banned openings (S1): the history envelope (`<message`, `<messages`, `<last_message`,
`<message_type`, `<text`, `<user`, `<reply_to`, `<attach`, `<history`), the runtime
context (`<chat_context`, `<current_`, `<locale`, `<reference_context`, `<memory`,
`<shield`, `<custom_persona`, `<daily_persona`, `<accent`), the system contract
(`<system_contract`, `<identity`, `<base_voice`, `<persona`, `<task`, `<rule`, `<dialog_`,
`<answer_policy`, `<output_and_memory`, `<transport`, `<tool_contract`, `<naming`,
`<final_check`, `<check`, `<step`), and the answer envelopes the guard already fights
(`<answer`, `<final_`, `<response`, `<reply`, `<think`, `<thought`, `<reasoning`,
`<channel`, `<assistant`, `<context`).

Textual tool-call forms (`<tool_call`, `<call:`, tool-name tags) are **not** banned: they
are how the model calls tools today (~90% of calls), and banning them without a proven
native-call path would cost more than it saves.

`bad_words` is defeatable by a different tokenization (`<` + `mes` + `sage`), so the
regex arm below measures whether a hard constraint is available instead.

### B. The re-sample must carry the reason

Today a re-sample is the same prompt with a new sample. The literature is consistent that
models repair reliably when the feedback is external and specific and unreliably when it
is not (TACL survey "When Can LLMs Actually Correct Their Own Mistakes?", Self-Refine).
The guard's verdict is exactly such a signal.

On a re-sample the dialog step appends a short plain-text note to the end of the last user
message — after `</last_message>`, inside the same message so the chat template keeps its
shape and the 9,088-token cached prefix stays intact. The note states the verdict in
words and asks for an ordinary reply; it never names the tags, so it cannot teach the form
it is trying to prevent. The sample is additionally moved: seed derived from (turn,
attempt), temperature +0.15 per attempt capped at 0.5.

### C. The prompt must stop provoking it

Candidates, shipped only if the pod measurement supports them:

- **P1** — one plain-text line after `</last_message>`, so the generation point follows
  prose instead of a closing tag.
- **P2** — in multimodal turns, order the content parts so the text does not end on raw
  XML against the generation point.

### D. The guard unwraps instead of rejecting

For providers where A is unavailable (genkit, ninfer) and for anything A misses: when the
reply is a fabricated envelope whose inner text is not a copy of context, take the inner
text and re-run the full finalize path on it **recursively** (bounded depth), because the
unwrapped content may itself contain a tool call that must be parsed and executed. A
genuine transcript copy is still suppressed — the guard fingerprints the actual history.

## Experiment (RunPod 3090, one hour)

Same stack as the farm: vLLM 0.25.1, transformers 5.8.0, compressed-tensors 0.17.0, the
pinned checkpoint, prefix caching on.

Replay set: ~300 production requests — 150 rejected-as-envelope turns, 100 ordinary turns,
50 turns where a tool was expected. Stored images are redacted in `raw_request`, so
multimodal turns are replayed with a synthetic neutral image: the structure is faithful,
the content is not, and the report says so.

Arms: A0 baseline · A1 = S1 bad_words · A2 = regex spike (structured outputs constrain the
first token to prose or an allowed HTML tag; the spike answers whether xgrammar accepts it
and whether native tool calls survive) · A3 = P1 · A4 = S1+P1. Plus, on A0's rejections, a
hinted re-sample against a blind re-sample with the same seed shift.

Every arm is scored by the production code itself — `finalize_dialog_reply_with_guard`
with a guard rebuilt from each request's own messages, and `parse_assistant_content` —
through a throwaway probe binary, so the numbers are what the bot would have done.

Acceptance: A1 ships if envelope rejections at least halve with no loss in executable tool
calls and no drift in reply length or language. P1/P2 ship if they add ≥5 points over A1.
The hint ships if it recovers ≥15 points more than a blind re-sample. A2 is a spike: its
output is an answer, not necessarily code.

## What the measurement said

A rented RTX 3090 ran the farm's stack (vLLM 0.25.1, the pinned checkpoint, prefix caching
on) and replayed 300 production requests from 2026-09-17 per arm: 150 turns production
rejected as a wrapped answer, 100 ordinary turns, 50 turns where a tool was expected. Every
reply was scored by the production code — `finalize_dialog_reply_with_guard` with a guard
rebuilt from each request's own messages, plus `parse_assistant_content`. Multimodal turns
were replayed with a neutral placeholder image, because stored images are redacted in
`raw_request`: the structure is faithful, the content is not.

| arm | wrapped answers | delivered | tool calls executed | false tools | suppressed | median latency |
|---|---|---|---|---|---|---|
| A0 baseline | 92/150 | 278/300 | 37/50 | 6/100 | 22 | 2.0s |
| A1 `bad_words` | 0/150 | 279/300 | 43/50 | 6/100 | 21 | 1.4s |
| A2 first-token regex | 0/150 | 293/300 | 7/50 | 0/100 | 7 | 1.6s |
| A3 prompt line | 0/150 | 280/300 | 32/50 | 2/100 | 20 | 1.5s |
| A4 `bad_words` + prompt line | 0/150 | 285/300 | 30/50 | 3/100 | 15 | 1.7s |

Shipped: **A1** and **D**. A1 clears the envelope completely and buys six tool calls per
fifty; the ordinary set does not move in length (63 → 61 chars) or language (Cyrillic share
1.00). It is enabled per model through the routing config, so only the farm's vLLM rows
carry it.

Not shipped, and why:

- **A2 (guided regex).** The spike's answer: xgrammar accepts the pattern, no engine errors,
  latency unaffected, and it delivers the most replies — but it costs 30 of 50 tool calls,
  because the textual call form is how this model calls tools and native calls do not take
  over. Only worth revisiting for turns that carry no tools.
- **A3/A4 (prompt line).** The line clears the envelope as well as A1 does, but it costs 11
  tool calls against A1 and adds nothing on top of it. The acceptance bar (≥5 points over
  A1) is not met.

**B (the hinted re-sample)** was measured separately: 208 re-samples per arm, drawn from the
baseline arm's own turns. Blind repeats the request as production did; hinted appends the
note.

| | blind | hinted |
|---|---|---|
| delivered | 160/208 (77%) | 181/208 (87%) |
| wrapped the answer again | 124 (60%) | 15 (7%) |
| executed a tool call | 52 | 52 |
| recovered after a protocol/empty rejection | 6/39 (15%) | 16/39 (41%) |

Honest caveat carried into production: `bad_words` does not reduce the *total* number of
rejected turns (22 → 21). It removes the transcript envelope, and the model then invents
other shapes (`<reason_to_be_optimistic>`, repeated `<react_to_message>` blocks). That
residue is what D's unwrap and B's note exist for, and it is the thing to watch next.

No seed plumbing was needed: dialog requests set no seed, so re-samples already differ.

## Delivery

1. Pod experiment, report with the table and a recommendation.
2. PR: `bad_words` plumbing with a routing-config override, retry context and the hint,
   recursive envelope unwrap, tests built from the replayed production samples.
3. PR: prompt changes that won, with the prompt pin tests updated.
4. Deploy on the owner's word, verified with the same SQL used to diagnose this.
