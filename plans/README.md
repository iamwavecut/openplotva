# Optimization plans

This directory is the active optimization program. Historical execution transcripts
001–011 and dated Superpowers handoffs were reconciled against current source and
retired; their durable outcomes and the complete document inventory live in
[Execution outcomes](../docs/decisions/execution-outcomes.md).

| Plan | Outcome | Local status |
|---|---|---|
| 012 | Compile prompts once and inject `PromptStore` | Implemented and benchmarked |
| 013 | Remove the dead token-estimator service | Implemented after live no-op proof |
| 014 | Use one runtime GraphQL output model | Implemented; exact SDL preserved |
| 015 | Replace the recursive update chain with typed routing | Safe allocation-reducing stage complete; contract-changing stage deferred |
| 016 | Supervise workers with named phases and one shutdown deadline | Implemented; 48-task deadline test passes |
| 017 | Build one redacted LLM trace without request clones | Implemented and benchmarked |
| 018 | Make `OutboundCommand` the Telegram send source of truth | Compatibility phase implemented; legacy deletion awaits expiry proof |
| 019 | Retire stale execution docs | Complete locally; 6,384 net lines removed in documentation scope |
| 020 | Complete the required Taskman admin UI | Implemented locally; targeted browser, REST, GraphQL, asset, design, and full workspace tests pass; service smoke environment-blocked before browser |

Current plans preserve compatibility boundaries and verification requirements. A local
implementation status is not a commit, PR, merge, deployment, or production claim.
Completed work leaves this directory only after delivery or an explicit retirement
decision; Git remains the full execution history.
