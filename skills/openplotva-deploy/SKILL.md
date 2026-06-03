---
name: openplotva-deploy
description: Use this skill to trigger, monitor, and triage OpenPlotva production deployments through GitHub Actions and GitHub CLI.
---

# OpenPlotva Deploy

Use this skill when deploying OpenPlotva to `geta.moe` or another Docker Compose target.

## Rules

- Production deploys are manual-only GitHub Actions runs.
- Use GitHub CLI for workflow dispatch and monitoring.
- Do not deploy by direct SSH unless the user explicitly asks for a direct server action.
- Direct SSH is allowed for read-only triage: container status, logs, health checks, and server file presence.
- Never print `.env.production`, tokens, provider keys, or Telegram credentials.
- The production env file is server-local. The deploy script creates `/home/wavecut/openplotva/.env.production` from `OPENPLOTVA_PRODUCTION_ENV_B64` only when the file is absent.
- The deploy job is an idempotent apply: it starts missing backing services, preserves already-running backing services, recreates only the `openplotva` app service, and verifies `/api/health` plus `/api/ready`.
- When OpenPlotva-owned volumes are empty and matching older production volumes exist on the target, the script performs a one-time data import before starting the new stack.
- After a successful deploy, the workflow deletes old GHCR app image versions older than 24 hours unless they match the current or deployed image tag.

## Production Config Invariants

Preserve these invariants during incidents and deploys:

- Dialog primary is AI Farm Discovery, not the external pool:
  `DIALOG_PROVIDER=aifarm`, `DISCOVERY_BASE_URL=<AI Farm Discovery>`,
  `DIALOG_DISCOVERY_SERVICE_NAME=llm-openai`.
- `DIALOG_AIFARM_POOL_BASE_URLS`, `DIALOG_AIFARM_POOL_MODELS`, and
  `DIALOG_AIFARM_POOL_API_KEY` configure the first overflow fallback pool. They
  are separate from Discovery and must stay separate.
- `dialog_aifarm_fallback_jobs` is expected in readiness. It is the threshold
  drainer for queue overflow or primary stalls. Do not disable it by setting
  `PERSISTENT_QUEUE_DIALOG_AIFARM_FALLBACK_WORKERS=0` unless explicitly
  requested.
- Do not change high/low fallback watermarks just to silence a symptom; inspect
  queue diagnostics and job events first.
- Runtime debug API exposure is controlled by the published compose port. The
  container listens on `RUNTIME_API_HOST:RUNTIME_API_PORT`; the host bind may be
  Tailscale-only or public via `OPENPLOTVA_RUNTIME_API_PUBLISH_HOST`.
- Runtime TLS pins are generated from the live certificate and can change on
  every container restart.

## Workflow

Workflow file:

```bash
.github/workflows/deploy-production.yml
```

Required GitHub secrets:

- `GETA_SSH_PRIVATE_KEY`
- `GETA_SSH_KNOWN_HOSTS`
- `GHCR_PULL_TOKEN` for server-side app image pulls.
- `OPENPLOTVA_PRODUCTION_ENV_B64` for first deploy to a target without an existing `/home/wavecut/openplotva/.env.production`.

Target server requirements:

- Docker Engine.
- Docker Compose plugin.
- SSH user allowed to run Docker.

## Commands

Deploy production:

```bash
gh workflow run deploy-production.yml \
  --repo iamwavecut/openplotva
```

Find and watch the newest deploy run:

```bash
run_id="$(gh run list \
  --repo iamwavecut/openplotva \
  --workflow deploy-production.yml \
  --limit 1 \
  --json databaseId \
  --jq '.[0].databaseId')"
gh run watch "$run_id" --repo iamwavecut/openplotva --exit-status
```

## Read-Only Triage

Check production containers:

```bash
ssh geta.moe 'docker ps --format "{{.Names}} {{.Status}} {{.Ports}}" | grep -E "openplotva|postgresql|dragonfly|embedder|token-estimator"'
```

Check health:

```bash
ssh geta.moe 'curl -fsS http://127.0.0.1:8080/api/health && curl -fsS http://127.0.0.1:8080/api/ready'
```

Check routing invariants without printing secrets:

```bash
ssh geta.moe 'docker exec openplotva-openplotva-1 sh -lc '"'"'
for k in DISCOVERY_BASE_URL DIALOG_PROVIDER DIALOG_DISCOVERY_SERVICE_NAME DIALOG_MODEL \
  DIALOG_AIFARM_POOL_BASE_URLS DIALOG_AIFARM_POOL_MODELS \
  PERSISTENT_QUEUE_DIALOG_AIFARM_FALLBACK_WORKERS RUNTIME_API_HOST RUNTIME_API_PORT; do
  eval v="${'"'"'$k'"'"'-}"
  case "$k" in
    DISCOVERY_BASE_URL|DIALOG_AIFARM_POOL_BASE_URLS|DIALOG_AIFARM_POOL_MODELS)
      printf "%s=<configured>\n" "$k"
      ;;
    *)
      printf "%s=%s\n" "$k" "$v"
      ;;
  esac
done
'"'"''
```

Check logs without exposing env:

```bash
ssh geta.moe 'docker logs --tail=160 openplotva-openplotva-1'
```

## Acceptance

- The workflow has no dispatch inputs.
- Only `iamwavecut` can start or re-run the deploy job.
- Missing backing services are created and healthy.
- Already-running backing services are not recreated.
- `openplotva` is recreated with the new image.
- `/api/health` and `/api/ready` pass after deploy.
