# Contributing

## Ground rules

- Keep changes focused and production-oriented.
- Do not commit secrets, credentials, database dumps, `.env` files, Telegram tokens, provider keys, or live smoke inputs.
- Prefer clear code over explanatory comments. Add comments only when they explain a non-obvious invariant, protocol requirement, or safety constraint.
- Preserve public API shapes, Telegram payloads, database schema semantics, Redis key semantics, prompts, payment behavior, provider request shapes, and operator-visible diagnostics unless a maintainer approves a breaking change.
- Do not add placeholder runtime paths, fake production consumers, `todo!`, or `unimplemented!` code.

## Local checks

Run focused checks while developing, then broaden before publishing changes:

```sh
cargo fmt --check
cargo clippy --workspace --all-targets -- -D warnings
cargo test --workspace
```

Useful smoke checks:

```sh
tools/local-smoke.sh
tools/service-smoke.sh
tools/provider-smoke.sh
tools/update-queue-smoke.sh
tools/container-smoke.sh
```

Use live smoke scripts only with disposable services and explicit credentials.
