# Memory compact prompt contract

## Problem

On ninfer, memory extraction prefill took about 3.3 s per call. We measured what the prompt is made of on 2026-10-02:

- The prefix cache already covers the whole stable part of the prompt. That is 3,067 tokens: the system turn plus the tool schema.
- Everything after it is new tokens for each window.
- 72% of those new tokens are `<msg id entry user author at>` attributes, about 70 tokens per message:
  - The RFC 3339 timestamp alone is 24%, because Qwen tokenizes every digit separately.
  - The 10-digit Telegram user id is 13%.
  - `entry="msg:<id>"` is 11%, and it duplicates `id`.
- Message text is only 21%.

Resolution and subject-merge inputs have the same problem in smaller form: they repeat JSON keys for every card. Prefill throughput is about 1,490 tok/s, which is the 3090's FP16 tensor ceiling. So the lever is fewer tokens, not faster compute.

## Decision

Keep the meaning of every field and change only the wire form. Two techniques:

- **Legend.** Shared reference data is stated once per call.
- **Local numbers.** The model sees `u3`, `m12` and `c4` instead of storage ids, and the code maps the numbers back.

The output keeps its descriptive keys (`fact_text`, `why_durable`, `reason`). Only the id fields carry local numbers.

### Extraction user message

```
<run>supergroup · 2026-09-28 16:00 → 2026-09-28 17:00 UTC</run>
<people>
u1 Галина
u2 Лори @lori_k
u3 Плотва bot
</people>
<cards>
c|type|subject|fact|conf|age|flag
c1|preference|Лори|Лори не ест мясо|0.8|3w|
</cards>
<chat>
# 2026-09-28 Mon
<m1 u1 16:08>Привет, Лизок❤️</m>
<m2 u2 16:09 fwd>…</m>
</chat>
Based on the window above, return the JSON object described in the system prompt.
```

Rules for this message:

- People are numbered in order of first message. They are keyed by user id, or by display name when the user id is 0.
- Cards are numbered in their existing subject-grouped order.
- Messages are numbered in window order.
- Times are UTC. A `# date weekday` line starts each day.
- `fwd` and `auto` mark a forwarded message and a channel's automatic forward.
- In message text only `<` is escaped. In table cells `|` becomes `/`, and whitespace is collapsed everywhere.
- An empty `<people>` or `<cards>` block is omitted.

### Extraction answer

- `source_message_ids` holds m numbers. `user_id` holds a u number. `old_card_id` and `into_card_id` hold c numbers.
- `source_entry_ids` is removed from the schema, because the message number implies it.
- `openplotva_memory::resolve_prompt_aliases(input, output)` maps the numbers back once per `extract` answer, before the gates run:
  - an unknown message number is dropped;
  - an unknown person becomes 0;
  - an unknown card becomes 0.
- The existing gates then reject unsourced cards and unknown card ids, as before.

### Resolution and subject merge

Both inputs become header rows. The answers are unchanged, because they already use indexes.

```
<candidates>
#0 identity|Кирилл|lives_in|Кирилл живёт в Новосибирске
  0 identity|Кирилл|lives_in|Кирилл живёт в Казани|0.8|2mo
</candidates>
```

```
<subject>Лена</subject>
<cards>
i|type|predicate|fact|sal|obs|age
0|preference|likes|Лена любит джаз|0.8|3|2w
</cards>
```

## Invariants

- `PROMPT_VERSION` stays `chat_memory_daily_v6`. Bumping it would make `SQL_SKIP_SUPERSEDED_MEMORY_RUNS` skip the whole queued backlog. The change is the wire form; card semantics do not change.
- The prefix-cache boundary does not move. The system turn and the tool schema stay byte-stable, and every per-run byte follows them.
- Telegram user ids, message ids and card ids never reach the model.
- The run input budget keeps its unit, estimated tokens. The compact form lets more messages fit under the same `MEMORY_MAX_EXTRACTION_BATCH_INPUT_TOKENS`, so windows are split into fewer continuation runs.

## Verification

- Unit tests:
  - numbering and leakage: no storage id appears in the payload;
  - alias mapping: unknown numbers are dropped;
  - the exact row formats for resolution and merge;
  - the updated aifarm request assertions;
  - the eval schema snapshot.
- `tools/prompt-eval` mirrors the renderers and maps answers back before its checks. The v6 and compact prompts run on the same fixtures against the production model (forced tool mode), and their pass rates are compared.
- After deploy, the following must not get worse:
  - `prefix_cache_hit_tokens` stays 3,067;
  - computed prefill tokens per call and per message;
  - gate counters (`unsourced`, `unknown_card_ids`);
  - cards per run.
