---
name: openplotva-deploy
description: Use this skill to trigger, monitor, and triage OpenPlotva production deployments through GitHub Actions and GitHub CLI.
---

# OpenPlotva Deploy

Use this skill when deploying OpenPlotva to `geta.moe` or checking deployment readiness.

## Rules

- Production deploys are manual-only GitHub Actions runs.
- Use GitHub CLI for workflow dispatch and monitoring.
- Do not deploy by direct SSH unless the user explicitly asks for a direct server action.
- Direct SSH is allowed for read-only triage: container status, logs, health checks, and server file presence.
- Never print `.env.production`, tokens, provider keys, or Telegram credentials.
- The production env file is server-local. The deploy script creates `/home/wavecut/openplotva/.env.production` by copying `/home/wavecut/go-plotva/.env` on `geta.moe` if the new file is absent.
- After successful `first-cutover` or `redeploy`, the workflow deletes GHCR package versions older than 24 hours unless they match the currently deployed image tag.

## Workflow

Workflow file:

```bash
.github/workflows/deploy-production.yml
```

Operations:

- `prepare`: build/push GHCR image, upload deploy assets, verify server prerequisites, and do not stop services.
- `first-cutover`: stop the old Go app, create Postgres backup, run safe DB maintenance, flush Dragonfly DB, and start Rust.
- `redeploy`: pull/start a new Rust image without backup, Redis flush, or Go stack changes.

Required GitHub secrets:

- `GETA_SSH_PRIVATE_KEY`
- `GETA_SSH_KNOWN_HOSTS`
- `GHCR_PULL_TOKEN` for server-side image pulls.
- `GHCR_CLEANUP_TOKEN` with package read/delete permissions for post-deploy GHCR cleanup.

## Commands

Prepare without touching running services:

```bash
gh workflow run deploy-production.yml \
  --repo iamwavecut/openplotva \
  --ref main \
  -f ref=main \
  -f operation=prepare
```

First cutover:

```bash
gh workflow run deploy-production.yml \
  --repo iamwavecut/openplotva \
  --ref main \
  -f ref=main \
  -f operation=first-cutover \
  -f confirm=geta.moe/openplotva
```

Redeploy Rust:

```bash
gh workflow run deploy-production.yml \
  --repo iamwavecut/openplotva \
  --ref main \
  -f ref=main \
  -f operation=redeploy \
  -f confirm=geta.moe/openplotva
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
ssh geta.moe 'docker ps --format "{{.Names}} {{.Status}} {{.Ports}}" | grep -E "plotva|openplotva|dragonfly|postgres"'
```

Check Rust health after cutover:

```bash
ssh geta.moe 'curl -fsS http://127.0.0.1:8080/api/health && curl -fsS http://127.0.0.1:8080/api/ready'
```

Check logs without exposing env:

```bash
ssh geta.moe 'docker logs --tail=160 openplotva-openplotva-1'
```

Check Go app is stopped after cutover:

```bash
ssh geta.moe 'docker ps --format "{{.Names}}" | grep -qx go-plotva-app-1 && echo "Go app still running" || echo "Go app stopped"'
```

## Acceptance

- `prepare` must not stop `go-plotva-app-1`.
- `first-cutover` must report a non-empty backup path under `/home/wavecut/openplotva/backups`.
- `first-cutover` and `redeploy` must pass `/api/health` and `/api/ready`.
- After cutover, ask the operator to issue `/admin_runtime_token`, then use the runtime API skill with the returned token and TLS pin.
