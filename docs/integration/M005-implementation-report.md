# M005 S04 implementation report

feature_off_compatibility: PASS
native_responses_hedge_wired: PASS
default_enabled: false
production_deployed: false

## Scope

This patch implements the bounded OpenAI latency-health core, scalable shadow telemetry persistence, a provider-neutral two-attempt semantic hedge coordinator, and a native OpenAI Responses gateway adapter.

The Responses integration is wired in the real handler but remains disabled by default. When disabled or request-ineligible, the existing single-primary path is unchanged. No frontend or production deployment files are changed.

## Native Responses integration

Eligible requests are narrowly limited to stateless streaming text requests on the OpenAI-compatible HTTP Responses path. WebSocket, image, compact, `store:true`, `previous_response_id`, encrypted/reference-only continuation, and Grok requests remain on the baseline path.

For an eligible request:

- the already-acquired primary scheduler lease is transferred to the request-scoped hedge policy;
- at most one distinct secondary account is selected;
- parent/child and sibling credential-owner overlaps are rejected;
- secondary scheduler probe leases are released before admission;
- dispatch uses a fresh immediate slot and fresh profit, schedulability, platform, capability, incident, and duplicate-cost checks;
- a concurrency change causes one bounded release/reacquire/recheck cycle and fails closed if state changes again;
- primary and secondary attempts each use isolated Gin contexts and reuse production `OpenAIGatewayService.Forward` transformations;
- only the coordinator writes winner frames to the client;
- the winner account, result, usage, response headers, and runtime context are promoted to the existing scheduler-reporting and single-user-billing path;
- loser state is discarded, loser provider-cost exposure is telemetry-only, and no second user charge is created;
- client cancellation cancels both isolated upstream attempts while baseline detached forwarding semantics remain unchanged;
- a successful winner emits one `response.completed` event and one `[DONE]` sentinel.

## Composition and lifecycle

`AdaptiveLatencyRuntime` is built in the server composition root with Redis telemetry cache, PostgreSQL rollup repository, leader lock, advisory-fence connection, and transition guard. Construction does not start background work. The final cleanup provider starts the runtime only after successful composition and stops it before Redis/PostgreSQL teardown.

Runtime stats distinguish configured, constructed, adapter-wired, and active state. Adapter wiring is present even while the feature is disabled; active remains false until configuration enables hedging.

## Persistence and observability

- Redis hot-window telemetry remains bounded by account/cohort/attempt keys and TTL.
- PostgreSQL stores fixed-cardinality rollups and bounded hedge/loser exposure fields.
- Telemetry does not persist request bodies, output text, raw errors, credentials, user/API-key IDs, IPs, or raw User-Agents.
- Unknown loser usage is recorded conservatively at potential duplicate cost.

## Verification

Fresh verification on the final tree:

- `go test ./... -count=1` from `backend/`: PASS.
- `go test ./internal/config ./internal/service ./internal/handler -count=1`: PASS.
- `go test -race ./internal/service -run 'Test(OpenAIHedge|AdaptiveLatencyRuntime|M005|ReleaseFuncOpenAIHedge)' -count=1`: PASS.
- `go test -race -tags unit ./internal/handler -run '^TestOpenAIGatewayHandlerResponses_Hedge' -count=1`: PASS.
- Handler E2E covers primary win, secondary win, feature-off parity, feature-on request ineligibility, no secondary candidate, client cancellation of both attempts, initialization failure after primary lease transfer, winner-only output/header/account/usage, one terminal outcome, and exact scheduler lease release counts.

## Remaining operational work

This patch does not enable the flag, deploy, or claim production latency improvement. A later slice must deploy the disabled patch, enable a bounded cohort, compare organic first-meaningful/TTFT and tail latency, and roll back on cost, error-rate, or cancellation regressions.
