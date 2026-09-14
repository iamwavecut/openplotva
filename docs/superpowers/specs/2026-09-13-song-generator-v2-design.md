# Song generator v2 ("Song Director") — design

Date: 2026-09-13. Scope: the `!song` / `/song` / `generate_song` flow, the LLM step
that writes song material, the farm music request, the delivered message, and a
retake affordance. Music generation itself stays on the farm `music-api` (YuE2).

## Why

Owner review of the demo tracks produced two conclusions:

1. What makes the generator good is the **material** handed to the music model:
   a long, three-layer tag list (sound, character, structure) written in the
   genre's own vocabulary, lyrics in the genre's native form, and the freedom to
   re-roll seeds of the same material. Short tag lists, cross-genre softeners
   ("pads, piano, strings" in an aggressive genre), script-generated melodies and
   any post-processing all produced rejects.
2. The current flow could not produce that material by construction:
   - `prompts/music/song_reprompt.prompt` asked for 3–7 tags and a fixed
     `[Verse 1]/[Chorus]/[Verse 2]/[Chorus]` shape of 4–8 lines, and carried a
     Russian classical-versification block (Fet.Online, iambic tetrameter) that
     contradicts song lyrics in any language.
   - `normalize_song_style` rejected any style outside 3–7 tags and required the
     BPM tag last; `has_song_minimum_structure` rejected rap verses, short
     bridges, intros, outros and instrumentals.
   - `build_song_release_prompt` appended ", песня о <topic>" to the tag list.
   - `audio_config` carried no `max_seconds`; seeds and durations were dropped.
   - The lyric language came from the Telegram UI language, not the request.
   - The multi-step song agent (`prompts/agentic/song_system.prompt`) produced
     pseudo-lyrics such as `[Instrumental - fast tremolo picking]` and, on the
     Heretic model, malformed bytes; it then fell back to the reprompt above.
   - Nothing stored the material, so a listener could not ask for another take.

## Pipeline

```
request text ──┐
topic ─────────┼─► context (history + memory, best effort, time-boxed)
language hint ─┘            │
                            ▼
              Song Director (one structured LLM call, JSON schema)
   analysis → title, language, vocals, genre, bpm, key,
   sound[], character[], structure[], vocal_style, references[],
   duration_seconds, lyrics
                            │
                            ▼
        validate + normalize (code): language, tag layers, lyrics form,
        instrumental rule, duration → compile tag string (deterministic order)
                            │
                            ▼
        farm music-api: <prompt>tags</prompt>[<lyrics>…</lyrics>]
        audio_config { format, vocal_language, max_seconds }
                            │
                            ▼
        persist generated_songs row (material, brief, seed, duration)
        rich message: title · style line · audio · lyrics · tags · footer
        inline keyboard: 🎲 Ещё тейк → re-enqueue the SAME material, new seed
```

### Song Director prompt (`prompts/music/song_director.prompt`)

A single system prompt in English that encodes the method:

- Read the request as a listener's brief: explicit genre or the genre implied by
  mood words; vocal or instrumental (explicit words, or instrumental-by-default
  genres); language policy — explicit instruction, then the language of the
  request text, then the UI hint; target duration; vocal gender and delivery.
- Write the production brief in three layers, each in the genre's vocabulary:
  **sound** (instruments, bass/lead/drum design, textures), **character**
  (energy, mood, era, attitude), **structure** (intro, verses, builds, drops,
  breakdowns, switch-ups, fills, second drop, outro). English only, concrete
  nouns, no negations, no words from other genres, one BPM, optional key,
  optional track/artist "style" references as a secondary hint.
- A compact genre cheat-sheet (BPM, drums, harmony, sound words, structure,
  vocal delivery, lyric form) for the families the bot's audience asks for.
- Lyrics rules per genre and language: hook-first chorus, concrete imagery,
  natural stress, section markers, length derived from the target duration,
  no filler, no meta commentary, no placeholder pseudo-lyrics, one language.
- Output: the JSON object described by the schema; `analysis` first so the
  model plans before it writes.

### Code contract (`openplotva-media::acestep`)

- `SongPromptRequest { topic, request_text, user_full_name, language_hint,
  context, user_id, message_id }`.
- `SongPromptPayload` mirrors the schema. `normalize_song_prompt_payload`:
  - language must be one of `SUPPORTED_SONG_LANGUAGES`;
  - `vocals` ∈ {male, female, duet, choir, instrumental};
  - tags: trimmed, whitespace-collapsed, Latin letters/digits and
    ` #+&/'()-.` only, 2–80 chars, de-duplicated; at least 10 in total;
  - lyrics: section markers canonicalized (`Intro`, `Verse N`, `Pre-Chorus`,
    `Chorus`, `Bridge`, `Outro`; hook/refrain/drop → Chorus, breakdown →
    Bridge, others → Verse), placeholder lines such as `[Instrumental - …]`
    removed, empty sections dropped; vocal songs need ≥ 2 sections, ≥ 8 lines
    and a chorus, ≤ 80 lines; the script of the lyrics must match the language
    (Cyrillic for ru/uk/be, Latin otherwise);
  - instrumental: `vocals == instrumental` or no lyrics left → lyrics empty and
    an `instrumental` tag; duration clamped to 45–360 s (default 180);
  - vocal songs: duration clamped to 60–360 s (default 200); the farm cap is
    duration + 45 s (song length still follows the lyrics).
- `compile_song_tags`: genre → `NNN BPM` → key → vocals/vocal style →
  sound → character → structure → references. `style` = the compiled string,
  `raw_style` = a compact human line (`genre · BPM · key · vocals`).
- `build_song_release_prompt` no longer appends the topic.
- `CompletionRequest.max_seconds` → `audio_config.max_seconds`;
  `CompletionResult` carries `seed` and `duration_seconds` from `usage`.

### Persistence (`migrations/187_generated_songs`)

`generated_songs(id, job_id, retake_of, chat_id, thread_id, user_id,
user_full_name, trigger_message_id, result_message_id, request_text, topic,
title, vocal_language, vocals, tags, style_summary, lyrics, duration_seconds,
brief jsonb, seed, audio_seconds, created_at)`. Written after generation,
before the message is sent (the message needs the id); `result_message_id` is
filled after the send.

### Retake

Callback `{"a":"song_rt","song":"<id>"}` on the song message. The handler loads
the row, then goes through the same `SongScheduler::schedule_song` gate as a
fresh request (service availability, audio permission, VIP of the presser,
rate limit, 2 active jobs per user, delivery obligation) with
`SongScheduleRequest.retake` carrying the stored material. The job params
carry `lyrics`, `style`, `vocal_language` and `meta.song`, and the material
provider short-circuits: no LLM call, same tags and lyrics, new seed.

### Removed

- `SongAgentMaterialProvider`, `SongAgentSettings`, `parse_song_material`,
  `prompts/agentic/song_system.prompt` and `LLM_AGENTIC_SONG_ENABLED`. Context
  gathering moves into the director call (history + memory search results as
  a context block); the tool loop is not needed for song material.
- `prompts/music/song_reprompt.prompt`, the 3–7 tag accumulator, the fixed
  section validator, the "song about <topic>" suffix.

### Testing

Unit tests in `openplotva-media` (payload normalization, tag compiler, lyrics
canonicalization, instrumental rule, completion request with `max_seconds`,
usage parsing), `openplotva-llm` (director request shape), `openplotva-prompts`
(prompt roles and variables), `openplotva-telegram` (callback data, keyboard,
routing), `openplotva-app` (material provider short-circuit, effects persist
+ keyboard, retake handler scheduling with material, rich message layout).
Live verification after deploy: a `!song` request in the owner's DM, the
generated_songs row, the tag string in the delivered message, and a retake.

## Update 2026-09-14

- The retake button and its callback path were removed on the owner's request (no extra buttons under songs). `generated_songs` stays as the tracing record of what the music model heard; `retake_of` remains an unused nullable column.
- The delivered message no longer shows the full tag list (only the style line, the audio and the lyrics).
- The director prompt regained the Russian versification craft from the previous prompt, adapted to song form: one syllabo-tonic metre per section with natural stresses, exact masculine/feminine rhymes, cross rhyme with alternating endings by default, modern vocabulary without clichés, and the rhyme-first self-check; rap keeps its rhythmic rhymes. The same craft is stated for Ukrainian and Belarusian.
