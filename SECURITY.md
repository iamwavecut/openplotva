# Security Policy

OpenPlotva is under active private development. Treat security issues and operational details as private until maintainers publish a public reporting address.

## Reporting

Report suspected vulnerabilities privately to the repository maintainers. Do not open public issues for secrets, authentication bypasses, Telegram Bot API abuse paths, payment/provider defects, database exposure, or production configuration leaks unless maintainers have made the repository public and published a public reporting address.

## Sensitive Data

Never commit:

- Telegram bot tokens or webhook secrets.
- Provider API keys.
- Gemini, OpenRouter, Together, AIFarm, ACE-Step, Pruna, ModelScope, AIHorde, Serper, or database credentials.
- `.env` files with live values.
- Production database dumps or Redis snapshots.
- Private Telegram file IDs or chat/user IDs used for live smokes.

Use ignored local files or environment variables for smoke-test inputs. Redact secrets from logs and handoff notes.

## Supported Security Surface

Security-sensitive changes must preserve the published runtime contracts for:

- Telegram login and WebApp HMAC validation.
- Admin cookies, runtime API bearer tokens, and TLS pinning.
- Payment invoice, pre-checkout, refund, and subscription control requests.
- Settings permission checks, deputies, and chat membership freshness.
- Provider credentials, request headers, and error diagnostics.
