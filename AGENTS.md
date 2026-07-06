# OpenPlotva Agent Notes

## Operating Rules

- Answer the user in their language; write code, comments, docs, and commits in English unless asked otherwise.
- Verify repository facts before claiming them. Do not describe crate ownership, runtime flow, commands, or deployment state from memory.
- Favor broad product progress over auxiliary scaffolding. Avoid placeholders, provenance notes, and speculative abstractions.
- Dirty user changes are normal in this migrated repository; account for them and continue unless they directly conflict with the task. Never reset, clean, push, deploy, restart services, or rewrite history unless explicitly asked in the current request.
- Never commit secrets: live `.env` values, Telegram tokens, provider keys, database dumps, Redis snapshots, private file IDs, or smoke inputs.
- Report exact verification commands and results. If checks are skipped, say so and name the risk.

## Code Style

- Prefer self-explanatory code: clear names, typed data, small cohesive functions, and explicit boundaries over comments.
- Add comments only for non-obvious intent, invariants, protocol constraints, concurrency/lifetime reasoning, or safety notes.
- Remove obsolete comments instead of updating them mechanically. Do not leave TODOs, commented-out alternatives, or release-irrelevant history in code.
- Use `cargo fmt --all` after Rust edits. Match nearby conventions before introducing new patterns.
- Use typed errors in library/domain crates; use `anyhow` at application boundaries.
- Avoid `unwrap()` in runtime code. Use `expect()` only for local, obvious invariants.
- In async code, do not hold locks across `.await`; make cancellation, queue draining, and worker lifetime explicit.

## Architecture Boundaries

- `openplotva-app` is the composition root for config wiring, runtime startup, handlers, workers, and HTTP route assembly.
- Domain crates must not depend on web, Telegram, SQLx, or vendor SDKs unless the crate owns that integration boundary.
- Declare traits and adapters close to their consumers. Extend existing crates before creating new top-level crates.
- Keep vendor request/response types inside the owning integration crate or app boundary.
- `openplotva-telegram` owns Bot API builders, transport helpers, update sources, callback helpers, HTML handling, deduplication, rate limits, and outbound persistence.
- `openplotva-storage` owns SQLx Postgres, Redis stores, embedded migrations, and persistence adapters.
- `openplotva-web` owns admin/settings assets, WebApp signatures, login/auth helpers, and cookie/signature primitives.
- Provider-specific LLM, media, memory, shield, history, payment, and queue code should stay in the crate/module that already owns that boundary.
- Prompt templates stay as `.prompt` files under `prompts/`; preserve partial names, role markers, cache-sensitive system text, and template data shape.

## External Contracts

- Treat these as public contracts: Telegram payloads and callback data, commands, HTTP routes, GraphQL/runtime API shape, DB schema meaning, Redis keys/order/TTLs, prompts, shipped web assets, provider/payment requests, and operator-visible diagnostics.
- Preserve payload shape, lifecycle behavior, ordering, TTLs, error visibility, and security checks unless the user explicitly approves a behavior change.
- Prefer the smallest Rust-native implementation that preserves the contract. Do not copy convoluted internal control flow when the external artifact is the same.
- Sanitize Telegram-visible HTML at the boundary before sending, splitting, deduping, or persisting.
- Treat Settings WebApp state, Telegram login data, payment callbacks, provider credentials, and admin/runtime tokens as security boundaries.

## Web UI / Design System

- The admin UI (`web/admin/`) is a token-driven component library. Route every interactive element through the `pl-*` custom elements (`web/admin/components.js`) and token-backed classes; never write raw `<button>/<input>/<select>/<textarea>/<table>`, `onclick=`/`onsubmit=`, `el.onclick =`, inline `style=`, hex/`rgb()` colors, or native `alert()/confirm()` in `web/admin/`.
- Feedback is non-blocking via `PL.toast`; dialogs use `PL.alert`/`PL.confirm`. Every async load shows loading (skeleton/`pl-table` state), empty, and error states.
- Colors/spacing/type/motion/elevation/z-index live only in `web/admin/tokens.css`. `admin.css`/`components.css` reference tokens; no literals.
- Editing or adding any `web/` asset requires updating the matching `sha256` constant in `crates/openplotva-web/src/lib.rs` and running `cargo test -p openplotva-web` (the guard + hash tests enforce all of the above).
- Run the `openplotva-design-system-review` skill before merging admin UI changes. The Settings WebApp (`web/settings/`, Framework7 + Telegram theme) is a separate boundary, out of scope for this library.

## Useful Checks

- Formatting: `cargo fmt --all`.
- Focused Rust tests: `cargo test -p <crate> <filter>`.
- Local/runtime smokes when relevant: `tools/local-smoke.sh`, `tools/service-smoke.sh`, `tools/provider-smoke.sh`, `tools/update-queue-smoke.sh`, `tools/live-update-injection-smoke.sh`, `tools/container-smoke.sh`.

## Delivery & Review

The default path to production for every feature or fix:

- **Branch + PR.** Ship each change on its own `feat/...` or `fix/...` branch and open a PR into `main`. Never commit straight to `main`, and never self-attribute the work to Claude or any AI in branch names, commit messages, or PR text.
- **Green locally first.** Before opening the PR, run `cargo fmt --all`, `cargo clippy --workspace --all-targets -- -D warnings`, and the relevant `cargo test`. For any `web/admin/` change, update the matching `sha256` in `crates/openplotva-web/src/lib.rs`, run `cargo test -p openplotva-web`, and run the `openplotva-design-system-review` skill.
- **Watch the PR on a ~60s loop until checks settle.** On EVERY iteration poll the CI check statuses AND the review artifacts. Reviewers (PR-Agent, Qodo, Danger) usually post before CI finishes, so never gate artifact-handling on green CI; act as soon as anything actionable appears.
- **Bots edit ONE comment in place — re-read bodies, never gate on counts.** PR-Agent keeps a single "Persistent review / PR Reviewer Guide" comment (plus a "PR Code Suggestions" comment) and Qodo keeps a review comment; they EDIT it on every push, so the comment count and the unresolved-review-thread count do NOT change when new findings appear. On every poll, re-fetch and read the FULL body of each bot review comment (`gh api repos/<owner>/<repo>/issues/comments/<id>`, or match by author/title) — do not detect changes by comment count or thread count. Treat every finding in the LATEST body as open until it is fixed in a commit or rebutted in a reply; the bot's "updated to latest commit `<sha>`" line tells you which HEAD its findings apply to. PR-Agent also posts its code suggestions as **inline review-comment threads** (`gh api repos/<owner>/<repo>/pulls/<N>/comments`) — a separate artifact from the persistent guide comment; fetch and read those thread bodies every poll too, not only the guide.
- **Handle review artifacts objectively, not reflexively.** If a finding or code suggestion is valid, fix it (or apply the suggestion) and loop again. If it is wrong, harmful, or low-value churn, reply on the thread explaining why and resolve it — do not silently ignore it, and do not apply changes just because a bot suggested them. Address non-blocking warnings too (e.g. Danger asking migrations for up/down notes and a representative storage check).
- **Merge only when fully green AND every finding is handled.** Immediately before merging, re-read the FINAL body of every bot review comment (PR-Agent persistent review + PR code suggestions, Qodo review, Danger) at the exact commit you are merging, and confirm each finding is fixed-in-a-commit or rebutted-in-a-reply — not merely that the comment/thread counts are unchanged. Every inline review thread must additionally be replied to and **resolved** (GraphQL `resolveReviewThread`), so the unresolved-thread count is genuinely 0 at the merge commit. Do not tell the user a PR is "ready to merge" until you have actually read each inline suggestion body and resolved its thread — stating the criteria is not the same as verifying them. A lone `CANCELLED`/failing **PR-Agent code suggestions** check is a known infra flake: confirm the suggestions still posted as inline threads and handle those, then do not block the merge on that check alone. Then `gh pr merge <N> --merge`.
- **Deploy only when the user asks.** Deploy is the `deploy-production.yml` workflow run against `main` (authorized by the repo owner). After it reports success, spend a couple of minutes verifying: the running image matches the merged commit, the service is healthy, and recent logs are error-free; spot-check the changed behavior against real data where you can. Report what you verified and any remaining risk. If a check is skipped, say so and name the risk.
- **Migrations** ship as numbered up/down pairs with compatibility notes and a representative SQLx/storage check. Data backfills are one-way — say so in the down migration. Build indexes on hot or large tables with `CREATE INDEX CONCURRENTLY` (outside a transaction) so the migration does not block writes during deploy.
