# Request Tracing

Investigate a specific request or bounded failure window. Keep read-only diagnosis separate from operational mutations and synthetic probes.

## Anchor the request

- Resolve the symptom, observation window, and available update/message, chat, dialog, LLM-run, or job identifiers. Private identities are correlation data for this task, not content for public reports or reusable files.
- Check the running revision, effective route, and current schema/API fields. A local checkout or a historical successful deployment does not identify the currently running implementation.
- Start with the narrowest available request evidence. Record timestamps and inspect related earlier events only when needed to explain queue waits or retries. Distinguish pre-existing backlog from failures created during the window.
- Retrieve minimal trace metadata first. Read message/request bodies only when the task needs them; omit raw payloads, credentials, and private file references from saved/public evidence.

## Follow the observed path

The possible path is `Telegram ingress → handler/session → LLM/tool → persisted job → provider → outbound delivery → Telegram receipt`. Tool-free text can skip the job/provider stages; deferred, merged, or intentionally suppressed updates may not produce a separate reply. Follow the actual route rather than requiring every request to visit every stage.

| Boundary | Evidence to inspect | Conclusion it supports |
|---|---|---|
| Ingress and handling | `updatesRuntime`, scoped logs, current durable update/lease records through `sqlRead` where available | Received, claimed, started, completed, timed out, or still waiting; worker availability alone is insufficient |
| Dialog/session and LLM | `llmRequests` filtered by correlation keys, session obligations, effective routing and fallback records | LLM attempts, selected route, finish/error state, tool call or intentional handoff; attempts are not distinct user interactions |
| Persisted work | `taskmanQueueDiagnostics`, `taskmanJobs`, `taskmanJob`, messages/events | Queue age, claim, progress, retry, completion, and the relation between requested and returned outputs |
| Provider execution | Scoped provider/client logs and per-attempt timings/errors, using the configured route | Provider request accepted or completed, usable result produced, error or truncated response; HTTP success alone may contain no result |
| Outbound and receipt | Current Telegram outbox, delivery obligations, job messages/events and transport acknowledgement | Prepared, persisted, sending, retrying, ambiguous, terminal failure, or confirmed accepted delivery |

Use the queries already documented in the entrypoint. Discover current SQL keys and status definitions from the owning modules/migrations before joining records. An empty outbound table cannot prove that a direct send succeeded; verify the route used by that revision.

For each visited stage record the correlation key, status, start/end time, observed wait/execution time, and evidence source. Separate retry time from queue wait and provider execution. If clocks or retained records cannot establish a duration, leave it unknown.

## Topic-specific checks

- **Text/dialog:** Check session batching, deferred/merged updates, suppression, tool handoff, and delivery obligations before treating an update without a reply as failed. Inspect the actual primary/fallback path; preserve the entrypoint's Discovery/pool/drainer distinctions.
- **Search:** Distinguish tool selection, search-provider result, citation/repair attempts, final composed answer, and delivered text. A successful search call does not establish that composition or citation repair produced a user response.
- **Images/edits/albums:** Follow each requested source image and expected output through job/result events. Check draw-provider availability and upload/download/send errors. One completed job or one receipt does not prove every requested album result arrived. Preserve the difference between source-file resolution, provider rendering, and Telegram media delivery.
- **Music:** Correlate the original request, director/provider execution, expected output count, usable audio artifacts, and Telegram audio/media acknowledgement. Do not deduce successful delivery from a song-generation status or progress message.
- **Duplicates:** Compare interaction, job, send operation, and Telegram receipt identities. Multiple LLM attempts or retries can still represent one interaction; separate duplicate processing from duplicate accepted sends.

## Narrow the failure

- Compare the last confirmed stage with the next required stage. Scope warnings/errors by correlation and time; do not attribute unrelated historical errors to this request.
- Use current queue/lease ages, worker activity, and dependency timings to distinguish waiting capacity, a missing claim, stalled execution, and stalled delivery. Do not change worker counts, fallback watermarks, or priorities to make readiness look green.
- A provider result and a completion status do not prove an outbound send. A `resultMessageID` is supporting evidence; confirm the send state and receipt semantics for that path. Report ambiguous delivery as ambiguous rather than retrying it during an audit.
- State an exact cause only when the boundary evidence supports it. Otherwise report the localized failure and the missing evidence needed to distinguish remaining explanations.

## Probes and stopping condition

Virtual dialog `SAFE` suppresses real media side effects but still performs routing, LLM calls, and local writes. It is not a read-only query. Use it only when a probe is authorized; use a unique session and the documented cleanup. `REAL`, provider replay, restarts, cache purges, and configuration changes require appropriate operational scope.

Stop when the requested path is explained, or identify a concrete access/retention gap. Report the revision/window, observed stage sequence, last confirmed stage, next unconfirmed boundary, and affected outcome. When a fix or deployment is separately requested, prove the same path afterward and name unobserved branches instead of treating health/readiness as complete verification.
