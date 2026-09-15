# Sukko Engineering Principles

These are the engineering principles that govern the Sukko platform codebase.
Code comments throughout the repository cite them by section — a comment like
`// per §VII` refers to section VII of this document. Architecture decisions
are recorded separately in [`docs/adr/`](adr/).

This is the published form of the project's internal engineering rules; the
two are kept in sync on every amendment.

## I. Configuration

- **Env vars**: Every configurable parameter MUST use `env:` struct tags. `envDefault:` is the **single source of truth** for defaults — MUST reflect production-intended values.
- **Helm/Compose**: MUST NOT duplicate Go defaults. Override via env vars ONLY when a deployment needs a value different from the Go default (e.g., Kubernetes service discovery addresses). Helm templates MUST NOT compute derived values — derived config MUST be computed in Go.
- **Magic numbers/strings**: Magic numbers MUST be named constants or configuration. Magic strings are forbidden — config-class strings (URLs, addresses, topic names, namespace prefixes) and code-level symbolic strings (error codes, metric labels, log field keys used in multiple places) MUST be named constants — defined once, referenced everywhere.
- **Validation**: All config MUST be validated at startup with clear error messages. Hysteresis thresholds: `lower < upper`. Enum values MUST be validated against allowed sets. Invalid config MUST cause immediate startup failure — no silent degradation.
- **Shared fields**: Fields shared across services (e.g., `Environment`, `LogLevel`, `LogFormat`) MUST be defined once in `platform.BaseConfig` and embedded — never duplicated. Shared/internal packages MUST NOT define their own defaults for deployment-level settings; they MUST receive these values from callers.
- **Constructors**: Constructors accepting config from external or unvalidated sources MUST validate required fields and return an error when critical values are missing or empty — silent defaulting that could cause the system to run with wrong state is forbidden. Constructors receiving config structs already validated by `Validate()` at startup are exempt — the config layer is the single validation boundary.
- **CLI flags**: MUST use layered defaults — load from env vars first, use the config value as the flag default. Precedence: CLI flag > env var > `envDefault`. MUST NOT define their own independent defaults. Example: `flag.Int("shards", cfg.NumShards, "...")` where `cfg.NumShards` comes from `env:"WS_NUM_SHARDS" envDefault:"3"`.
- **Inline comments (operator docs)**: Every struct field with `env:"X"` (where X ≠ `"-"`) MUST carry an inline comment on the same line as the field (`// operator-facing description`). This comment is the single source of truth for the docs pipeline — it becomes the description in the configuration reference. Block comments above the field are for Go developers only (section headers, GoDoc). Missing inline comment = pre-commit failure (`ws/scripts/check-config-comments`). Quality: starts with the env var's effect (not Go field name), uses env var names when cross-referencing, one clear sentence, no internal code references.

## II. Defense in Depth

Every layer MUST validate its inputs. Never assume upstream validation. The gateway validates, and the server validates again. Input validation at ALL system boundaries is mandatory.

## III. Error Handling

All errors MUST be wrapped with context using `fmt.Errorf("operation: %w", err)`. Sentinel errors MUST be defined for expected conditions. Ignored errors MUST have an explicit comment explaining why. Silent failures are forbidden.

## IV. Graceful Degradation

Optional dependencies MUST use noop implementations or nil-guarded feature flags — never half-initialized state. Multi-step cleanup MUST continue on individual failures. Health endpoints MUST report degraded state. Retry logic MUST use exponential backoff with a cap. **Domain exception**: Application-layer webhook HTTP delivery uses a fixed retry schedule (`webhookRetrySchedule`: 1s, 5s, 30s, 2m, 10m) and an indefinite degraded-state maintenance poll — this matches industry-standard behavior (Stripe, GitHub, Pusher, Ably all use fixed schedules for webhook delivery). The §IV exponential backoff rule applies to infrastructure-layer reconnection (Kafka, Valkey) only. Slow WebSocket clients MUST be detected and disconnected via circuit breaker.

## V. Structured Logging

All logging MUST use zerolog with structured fields (Str, Int, Dur, Err). Appropriate log levels MUST be used (Debug/Info/Warn/Error/Fatal). No `log.Printf` or `fmt.Println`.

**Panic Recovery** — All panic recovery MUST use `defer logging.RecoverPanic(...)`. Inline `defer func() { recover() }()` is forbidden — it silently swallows panics without logging, making production debugging impossible. This applies to both goroutine entry points (`wg.Go` functions, per VII) and inline protective wrappers (e.g., closing resources that may panic due to concurrent state).

## VI. Observability

Every significant operation MUST have Prometheus metrics. Metric names MUST use `ws_` prefix (server), `gateway_` prefix (gateway), `provisioning_` prefix (provisioning service), or `webhook_` prefix (webhook-worker service) with units (`_seconds`, `_bytes`, `_total`). Labels MUST be used sparingly to avoid cardinality explosion. Histograms MUST be used for latency, not summaries. **Excluded from Prometheus**: The tester service (`cmd/tester`) is a test/debugging tool, not a production service — it uses its own built-in `metrics.Collector` (atomic counters) and `stats.Histogram` with SSE streaming for real-time observability. Prometheus integration MUST NOT be added to the tester.

**Tracing and Profiling** — Distributed tracing (OpenTelemetry) and profiling (pprof/Pyroscope) are opt-in, disabled by default — zero overhead when off (no goroutines, no allocations, noop `TracerProvider`). Tracing MUST cover cold paths only (auth, config changes, provisioning, consumer setup) — hot paths (proxy, broadcast fan-out, write pump) are covered by Prometheus histograms and MUST NOT be traced. Spans MUST be dropped silently when the exporter is slow or unreachable.

## VII. Concurrency Safety

High-performance, thousands of concurrent connections per pod. Wrong primitives cause panics, deadlocks, goroutine leaks, and silent corruption. Every concurrent pattern MUST follow the rules below.

**Design Preference** — Prefer goroutine ownership over shared memory with locks. When a piece of state needs concurrent access, the first choice SHOULD be a dedicated goroutine that owns the state and communicates via channels (Go proverb: "share memory by communicating"). Mutexes are acceptable for simple read-heavy caches (`sync.RWMutex`) and atomic counters, but for stateful operations (connection lifecycle, subscription tracking, auth flow), a single-owner goroutine with channel-based communication is safer and eliminates lock-ordering concerns.

**Goroutine Lifecycle** — All goroutines MUST be launched via `wg.Go()` (Go 1.25+), which handles `Add(1)` before launch and `Done()` after the function returns. The function passed to `wg.Go()` MUST follow this structure:
1. `defer logging.RecoverPanic(...)` MUST be the FIRST `defer` inside the function body.
2. The function MUST NOT call `wg.Done()` — `wg.Go()` calls it automatically. Calling `Done()` inside a `wg.Go()` function causes a double-Done, driving the WaitGroup counter negative and panicking at runtime.
3. The goroutine MUST check `ctx.Done()` in its main loop via `select` for shutdown signaling.
4. `wg.Wait()` MUST be called in the shutdown/stop path to ensure all goroutines have exited before resources are released.

**`wg.Go()` with inline closures** (preferred for short-lived or context-capturing goroutines):
```go
wg.Go(func() {
    defer logging.RecoverPanic(logger, "component_name", nil)
    doWork(ctx)
})
```

**`wg.Go()` with named methods** (preferred for long-lived goroutines with their own loops):
```go
wg.Go(s.runLoop)
// Inside runLoop: NO defer wg.Done() — wg.Go handles it.
func (s *Service) runLoop() {
    defer logging.RecoverPanic(s.logger, "runLoop", nil)
    for { select { case <-s.ctx.Done(): return } }
}
```

**CRITICAL — Modernization safety rule**: When converting legacy `wg.Add(1); go method()` patterns to `wg.Go(method)`, the `defer wg.Done()` inside the method body MUST be removed in the same change. Failing to do so causes double-Done. Both the call site and the method body MUST be updated atomically — never one without the other.

Shutdown ordering MUST be: cancel context → `wg.Wait()` for goroutines → close channels → release resources. Reversing this order (e.g., closing a channel before its goroutine exits) causes panics.

**Channels** — Channel type MUST match usage pattern:
- **Signal/stop channels** (`chan struct{}`): Used for shutdown signaling. MUST be closed by exactly one goroutine. If multiple goroutines may attempt close, MUST use `sync.Once` to guard the `close()` call. Sending on a closed channel panics — this is unrecoverable in production.
- **Data channels** (e.g., `chan OutgoingMsg`): MUST be buffered with a size matching throughput requirements. Unbuffered channels MUST NOT be used in hot paths (message distribution, broadcast fan-out) because a single slow receiver blocks all senders.
- **Semaphore channels** (`chan struct{}` with capacity = limit): Used for resource limiting (max connections, max goroutines). Acquire MUST be non-blocking (`select` with `default` case) to reject callers at capacity rather than queueing them indefinitely.
- **Fan-out sends** (broadcast to multiple subscribers): MUST use non-blocking `select` with `default` to skip slow consumers. Dropped messages MUST be counted via Prometheus metrics (`_dropped_total`). A single slow subscriber MUST NOT block delivery to all other subscribers.
- **Channel close rules**: Only the sender side MUST close a channel — never the receiver. After closing, no further sends are permitted (panic). When a channel may be closed from multiple code paths, guard with `sync.Once`. When draining a channel before reuse (e.g., `sync.Pool`), use `select` with `default` in a loop.

**WaitGroups** — `sync.WaitGroup` is for goroutine lifecycle tracking only. `wg.Go(func())` is the ONLY permitted launch pattern — manual `wg.Add(1); go func() { defer wg.Done(); ... }()` is legacy and MUST NOT be introduced in new code. Additional constraints:
- `wg.Wait()` SHOULD have a timeout mechanism (e.g., wrapper with `context.WithTimeout`) to detect stuck goroutines during shutdown rather than hanging indefinitely.
- WaitGroups MUST NOT be reused after `Wait()` returns for a given set of goroutines.

**Mutexes** — Locks MUST protect data, not code:
- `sync.RWMutex` MUST be used for read-heavy data (caches, subscription maps, metrics snapshots) where reads vastly outnumber writes. `sync.Mutex` MUST be used only when writes are as frequent as reads.
- Critical sections MUST be minimal: lock → read/write shared data → unlock. Mutexes MUST NOT be held across I/O operations (network calls, disk reads, channel sends, HTTP requests). Holding a lock across I/O blocks all other goroutines waiting for that lock, destroying throughput.
- `defer mu.Unlock()` / `defer mu.RUnlock()` MUST be used to prevent deadlocks from early returns or panics. Inline `Unlock()` without defer is forbidden.
- Nested mutex acquisition (locking mutex A while holding mutex B) MUST follow a consistent global ordering to prevent deadlocks. If ordering cannot be guaranteed, restructure to avoid nesting.
- Mutex values MUST NOT be copied. Structs containing a mutex MUST be passed by pointer and MUST NOT be assigned by value.

**Atomics** — Lock-free operations for hot-path counters and flags:
- `atomic.Int64` MUST be used for hot-path counters (messages sent/received, bytes, connection counts) instead of mutex-protected `int64`. Lock contention on frequently-incremented counters degrades throughput under load.
- `atomic.Bool` MUST be used for status flags read frequently in hot paths (health status, circuit breaker state, shutdown flag).
- `atomic.Value` SHOULD be used for periodic snapshot caching (e.g., subscriber lists rebuilt on subscription change, read lock-free on every broadcast). `Store()` replaces the snapshot; readers use `Load()` with zero contention.

**sync.Pool** — `sync.Pool` MUST be used for frequent allocations in hot paths (per-connection `Client` objects, message buffers). Objects retrieved via `Get()` MUST be fully reset before reuse: drain all channels (non-blocking `select` loop), clear all maps, zero all fields. Returning a partially-reset object causes state leakage between connections.

**sync.Once** — `sync.Once` MUST be used when an operation must execute exactly once across concurrent goroutines: connection close (`net.Conn.Close()`) and singleton initialization. Calling `Close()` twice on a `net.Conn` panics — `sync.Once` prevents this. Channel close guarding is covered in Channels close rules above.

**Message Pipeline Protection** — The message delivery pipeline (ingestion → broadcast bus → shard fan-out → per-client write pump → transport write) is the critical hot path. This applies to all transport types (WebSocket, SSE/gRPC stream, future Web Push). Feature-level operations (auth refresh, subscription management, metrics, provisioning lookups) MUST NOT block this path: no locks held across pipeline calls (`forwardFrame`, `sendToClient`, `bus.Publish`, write pump sends), no waiting for backend responses that stalls client message reads, and no observational interception (subscription tracking, metrics) that adds forwarding latency.

## VIII. Testing

Tests MUST be run with Go's race detector (`-race` flag) in local development and CI. The race detector is the primary automated enforcement mechanism for VII (Concurrency Safety). Test runs without `-race` MUST NOT be considered passing. Tests MUST be table-driven for multiple cases. Mocks MUST use interfaces. `t.Parallel()` MUST NOT be used on tests with shared resources (databases, external services, `*_shared_test.go`). Edge cases MUST be covered (empty, nil, max values, error paths).

**Test coverage for all changes is mandatory — no exceptions.** Bug fixes: reproduce and verify the fix; strengthen any test that missed it. Enhancements/refactors: update tests for changed behavior; add tests for new paths. New features: cover happy path, error paths, edge cases, and concurrency safety.

## IX. Security

**Rate Limiting** — Rate limiting MUST be applied at multiple levels (global, per-IP, per-tenant). Rate limit responses MUST use HTTP 429 with `Retry-After` header.

**Secrets Management** — Secrets MUST never appear in logs, error messages, or API responses. Sensitive keys and credentials (license keys, VAPID private keys, push provider credentials, API secrets) MUST be encrypted at rest in storage and decrypted only at the point of access — never stored as plaintext in databases, config files, or context stores. Encryption keys MUST be managed separately from the data they protect. Credential rotation MUST be supported without downtime — all credentials (JWT signing keys, API keys, VAPID keys, push provider credentials, admin tokens) MUST be rotatable via API while the system continues serving.

**Authentication & Replay Protection** — JWT validation MUST verify signature, `exp` (required; tokens without it MUST be rejected), and `iss` (when configured). Replay mitigation: tokens ≤15 min lifetime; `jti` SHOULD be used for critical operations; auth refresh MUST issue a new token, not extend the old one. API key comparison MUST be constant-time. Webhook endpoints (e.g., license reload) MUST validate a shared secret — default deny if unconfigured.

**Tenant Isolation** — Every data path MUST enforce tenant boundaries. Cross-tenant data leakage is a critical severity bug. Kafka topics, broadcast subjects, WebSocket subscriptions, and push subscriptions MUST be scoped to the authenticated tenant. Provisioning API MUST enforce `RequireTenant()` middleware — a tenant JWT MUST NOT access another tenant's resources. Database queries MUST include `tenant_id` in WHERE clauses — no unscoped queries that could return cross-tenant data.

**Admin & Operator Endpoints** — Admin and tenant auth is always enforced — there is no disabled or anonymous mode. All admin and operator endpoints MUST require authentication (admin JWT or equivalent). All tenant endpoints MUST require tenant JWT or API key authentication. Default deny — if no admin token is configured, admin endpoints MUST reject all requests. The `/config` endpoint MUST redact sensitive fields (fields tagged `redact:"true"`).

**Transport Security** — TLS MUST be enforced for all external-facing endpoints in production. Internal service-to-service communication (gRPC, Valkey, Kafka) SHOULD use TLS when crossing network boundaries. TLS configuration MUST support custom CA certificates for private PKI.

**CORS** — CORS allowed origins MUST be explicitly configured — no wildcard (`*`) in production. Preflight responses MUST NOT cache longer than `CORS_MAX_AGE` (default 3600s). Only required headers and methods MUST be allowed.

**Dependency Security** — Dependencies with known CVEs MUST NOT be merged. `govulncheck` SHOULD be run in CI. Dependency updates MUST be reviewed for breaking changes and security implications.

**Audit Trail** — Security-relevant operations MUST be audit-logged: tenant lifecycle changes, key creation/revocation, credential rotation, license reload, admin authentication attempts (success and failure), and rate limit triggers.

**Code Annotations** — `//nolint` or `#nosec` MUST include thorough written justification explaining why the suppression is safe. Debug and profiling endpoints (`/debug/pprof/`) MUST be disabled by default — they expose memory contents, goroutine stacks, and CPU profiles. They MUST only be enabled via explicit opt-in (`PPROF_ENABLED=true`) and SHOULD be restricted to internal networks in production. Input validation at boundaries is mandated by II.

## X. Shared Code Consolidation

Before writing any new utility, `internal/shared/` MUST be checked for existing implementations. Duplicate functions, error definitions, constants, and types across packages are forbidden. HTTP utilities MUST use `shared/httputil/`. Auth helpers MUST use `shared/auth/`. New shared code MUST have tests. Conversely, types, functions, constants, interfaces, and structs used by only one service MUST live in that service's package — never in `internal/shared/`, even if they serve a similar purpose to shared types. The shared package is exclusively for code referenced by multiple services. Service-specific code in shared violates separation of concern and creates false coupling.

## XI. Prior Art Research

Before designing any new feature or protocol extension, research how established real-time services solve the same problem (Pusher, Ably, Socket.IO, Phoenix Channels, Centrifugo, PubNub). Document: (1) the common industry pattern, (2) edge cases and failure modes mature implementations handle, (3) where Sukko deviates and why. "Not invented here" solutions to solved problems are forbidden.

## XII. API Design

**REST** — All external-facing APIs (admin, CLI, third-party) MUST use REST over HTTP/JSON.
- Endpoints MUST be versioned via URL path (`/api/v1/`). Health, readiness, and metrics endpoints MUST be at root level (no version).
- Routes MUST be resource-oriented: `POST` (create, 201), `GET` (read, 200), `PATCH` (partial update, 200), `PUT` (full replace, 200), `DELETE` (remove, 200). State-change actions use `POST` on sub-resources (`/suspend`, `/reactivate`).
- Error responses MUST use the `httputil.ErrorResponse` format: `{"code": "UPPER_SNAKE_CASE", "message": "human-readable"}`. HTTP status codes MUST map to semantics: 400 validation, 401 authn, 403 authz, 404 not found, 409 conflict, 500 internal.
- List endpoints MUST support pagination (`?limit=N&offset=M`) with defaults and max caps. Responses: `{items, total, limit, offset}`.
- All response writing MUST use `shared/httputil/` helpers (`WriteJSON`, `WriteError`). Raw `w.Write()` in handlers is forbidden.

**gRPC** — All internal service-to-service communication MUST use gRPC with protobuf.
- Proto files MUST live in `ws/proto/` with package naming `sukko.{service}.v1`. Style: `PascalCase` messages/services, `snake_case` fields, `UPPER_SNAKE_CASE` enums. Code generation via `buf generate`; generated code committed to repo. `buf lint` MUST pass in CI.
- Server-side streaming MUST be used for real-time data push (watch/subscribe). Unary RPCs for request-response.
- gRPC status codes MUST map to domain semantics: `NotFound`, `InvalidArgument`, `FailedPrecondition` (state conflict), `Internal`, `Unavailable` (temporary). Context via `status.Errorf()`.
- gRPC servers MUST run on a dedicated port, separate from HTTP. Both listeners MUST support graceful shutdown.
- Interceptors MUST handle: panic recovery (first), structured logging, Prometheus metrics (latency histograms, call counters).
- Stream clients MUST reconnect with exponential backoff and jitter, serve stale cache during disconnection, and reflect stream health in service health endpoints.

## XIII. Feature Gates

Every edition-gated feature MUST be documented in `internal/shared/license/features.go` with: (1) a `Feature` constant with a human-readable string value describing the capability, (2) an entry in `featureEditions` mapping it to the minimum required edition (Community/Pro/Enterprise), (3) a code comment on the constant indicating its status — `// Implemented` or `// Future — not yet implemented`. The `features.go` file is the **single source of truth** for the feature matrix — the docs site auto-generates the editions comparison page from it.

Every implemented gated feature MUST have an `EditionHasFeature()` check at its access boundary (API handler, config validation, or startup gate). Implemented features without gate checks allow Community users to access Pro/Enterprise functionality — this is a security and business logic bug.

New feature implementations MUST check the feature matrix first: if a `Feature` constant exists for the capability being built, the implementation MUST wire the gate check. Adding new gated features MUST follow: (1) add `Feature` constant with `// Future` comment, (2) add `featureEditions` entry, (3) when implementing, add `EditionHasFeature()` check and update comment to `// Implemented`.

## XIV. Tooling Boundary

The project has two command execution surfaces: **Taskfile** (developer toolchain) and **sukko-cli** (operator platform management). They MUST NOT call each other — they are independent tools with no runtime dependency.

- **Taskfile** operates on infrastructure and build artifacts: Go compilation, Docker builds, Helm releases, Terraform state, Kubernetes resources. It MUST NOT invoke `sukko-cli` commands or make provisioning API calls (`/api/v1/*`).
- **CLI** operates on the Sukko platform: tenants, keys, rules, license, subscriptions. It MUST NOT invoke Taskfile tasks.
- **Neither tool depends on the other being installed.** A developer can use Taskfile without CLI. An operator can use CLI without Taskfile.

**Sole exception**: `taskfiles/e2e.yml` may invoke `sukko` CLI commands for **data-path and platform-behavior E2E test orchestration**. This covers (a) edition enforcement validation (loading license tokens, restarting services, asserting claims and limits across Community/Pro/Enterprise editions), (b) edition-gated message-pipeline verification (e.g. the kafka-ingest round-trip, where a Pro license is the enabling toggle for the Kafka backend), and (c) the full validate-suite battery (WS pub-sub, SSE, REST publish, push, auth, provisioning, isolation, revocation, webhooks, …) run against a compose-booted stack. The CLI is the test driver, not the subject under test. No other Taskfile may reference or depend on `sukko-cli` — all CLI invocation MUST stay inside `taskfiles/e2e.yml`. If E2E orchestration outgrows shell-in-Taskfile (complex control flow, cross-suite state), it SHOULD move to a dedicated Go test binary rather than expanding this exception.

## XV. Simplicity (KISS)

Every feature, design, and implementation MUST be easy to reason about. Complexity is a cost, not a sign of thoroughness.

**Design**: Prefer explicit, mutually exclusive modes over layered fallback chains. A system with two clearly named operating modes is always easier to reason about than one with three silent fallbacks. If explaining how something works requires the word "unless" more than once, redesign it.

**Code**: Silent fallbacks, dual-purpose flags, and implicit mode detection are forbidden. If a value means two different things depending on context, split it into two values. If an operation behaves differently in different environments, the difference MUST be explicit in configuration — never inferred at runtime from the presence or absence of other values.

**Requirements**: Every feature MUST be describable to someone who hasn't read the code. If a requirement cannot be stated in one sentence without a footnote, it is too complex — simplify the design, not the sentence.

## XVI. Cross-Repo Awareness

The Sukko platform spans sibling repositories that MUST stay in sync whenever this repo adds, removes, or changes APIs, configuration, CLI-facing behavior, or user-visible features. Before any change is considered complete and before any PR is merged, cross-repo impact MUST be assessed and explicitly documented.

**Sibling repositories** (under the same GitHub organization):

- **`cli`** — Operator CLI. Impact triggers:
  - Provisioning API request/response schema changes → CLI command flags or JSON serialization may need updating
  - Tester service API changes (new body fields, new env vars, removed fields) → `sukko test run` flags may need a companion change
  - Auth mechanism changes, token format changes, or API key format changes → CLI auth flows may need updating
  - The CLI has its own engineering principles — CLI changes follow those, not this document

- **`docs`** — Documentation site. Impact triggers:
  - New or changed environment variables (name, default, type, description) → the configuration reference MUST be updated
  - User-facing error messages, WebSocket protocol message types, HTTP error codes/error schemas, connection close codes, env var names/defaults/types, CLI command/flag behavior, feature gate additions, or user-visible CLI output format → docs update required. When a change is user-visible but doesn't match any listed category, assess cross-repo impact explicitly — default is to assess, not skip.
  - New or changed REST API endpoints → API reference MUST be updated
  - Feature gate additions to `features.go` → the editions comparison page auto-generates from it, but guide content may still need updating
  - Any docs page added, removed, renamed, or significantly repurposed → `static/llms.txt` MUST also be updated — it is the primary LLM discovery index for the documentation site; a stale index means AI assistants cannot discover or reference new content

- **`cmd/tester`** (this repo) — Integration test service. Changes here that affect the tester API surface or configuration also affect the CLI (`sukko test run`) and the docs (Tester configuration reference).

**Requirements:**

1. Every change with any of the above impact triggers MUST document the required docs changes — listing the exact files and changes needed in the `docs` repo. These changes are **in scope** for the feature, not deferred follow-ups.
2. Every change with CLI impact triggers MUST document the changes needed in the `cli` repo. If the CLI changes are explicitly deferred, the deferral MUST be stated and tracked, not silently omitted.
3. Every code review MUST check for cross-repo impact: if the diff adds env vars, changes API schemas, or modifies tester config, a finding MUST be surfaced if the corresponding docs or CLI updates are absent.
4. Cross-repo changes that are in scope MUST be committed to their respective repos as part of the same logical change — they may be in separate PRs but MUST be referenced from each other.

## XVII. In-Repo Documentation & Contract Maintenance

OpenAPI, AsyncAPI, and in-repo behavioral documentation are authoritative artifacts used by clients, the docs site, and external integrators. They MUST be kept in sync with the code — a spec or guide that lags the implementation is a broken contract.

**Spec files** (all under `ws/docs/`):
- `openapi/provisioning.openapi.yaml` — Provisioning REST API
- `openapi/gateway.openapi.yaml` — Gateway REST API (publish, admin endpoints)
- `asyncapi/client-ws.asyncapi.yaml` — Client WebSocket protocol (channels, message types, auth bindings)

**OpenAPI impact triggers** — update the corresponding `.openapi.yaml` when:
- An endpoint is added, removed, or its URL path changes
- A request or response schema gains, loses, or renames a field
- An HTTP status code or error code (`code` field) is added or removed
- An authentication scheme or security requirement changes

**AsyncAPI impact triggers** — update `client-ws.asyncapi.yaml` when:
- A WebSocket message type is added, removed, or its payload schema changes
- A channel name format changes (e.g., `{tenant}.{topic}` pattern)
- A subscribe or publish operation is added or removed
- An auth binding (header name, token format) changes

**Requirements:**
1. Every change with any of the above triggers MUST name the exact contract file(s) and the change(s) needed, in the PR description. If no triggers apply, state "No API contract changes required."
2. Every code review MUST check for API contract and documentation impact: if the diff touches handler routing, request/response structs, error codes, WebSocket protocol types, tester suites, tester-visible env vars, or API endpoints tested by the tester, a finding MUST be surfaced if the corresponding artifact is not updated in the same PR. The in-scope artifacts are: `ws/docs/openapi/provisioning.openapi.yaml`, `ws/docs/openapi/gateway.openapi.yaml`, `ws/docs/asyncapi/client-ws.asyncapi.yaml`, and `ws/docs/e2e-testing.md`.
3. If the diff adds a new tester suite, changes an error code in the provisioning or tester API, adds or removes a tester-visible env var, changes a WebSocket message type, or adds an API endpoint tested by the tester, a finding MUST be surfaced if `ws/docs/e2e-testing.md` is not updated in the same PR.
4. OpenAPI/AsyncAPI spec updates and `ws/docs/e2e-testing.md` updates MUST be committed in the same PR as the code change — they are not documentation, they are contracts.

## XVIII. Architectural & Style Consistency

All services MUST implement equivalent functionality using the same patterns, conventions, and abstractions. Inconsistency across services creates cognitive load, increases defect surface, and impedes cross-team maintenance.

**Scope** — Consistency is required across: error handling patterns, logging field names and levels, configuration struct layout and env var naming conventions, metric naming (service prefix excepted), concurrency primitives, shutdown sequences, middleware ordering, and gRPC interceptor setup.

**Exceptions** — A service MAY deviate from a shared pattern only when: (1) its operational profile makes the shared pattern harmful (e.g., the tester service MUST NOT emit Prometheus metrics), or (2) the deviation is documented in this document or in a code comment stating why. Silent deviation is forbidden.

**New code alongside existing parallels** — When implementing a feature or fix that has a parallel in another service, that parallel MUST be reviewed first. If it is correct, the new code MUST follow it. If it is incorrect, both MUST be fixed — the new code establishes the correct pattern and a follow-up task MUST be filed for the sibling.

**Code review** — Cross-service consistency MUST be checked during code review when a pattern is introduced in one service that already exists (or should exist) in another, or when a refactor diverges from the established pattern in a sibling service.

## XIX. Decision Records

Durable engineering decisions are recorded as Architecture Decision Records in [`docs/adr/`](adr/) — any choice that is likely to be challenged, expensive to reverse, or needed by a future contributor MUST be recorded at the moment it is made. Accepted ADRs are never edited — they are superseded by new ones. ADRs capture the decision, its context and consequences, and the rejected alternatives.

Planning artifacts are ephemeral — there are no per-feature specification or plan documents in the repository; the durable outputs of design work are ADRs, contract files (§XVII), and committed documentation.

## Governance

- These principles supersede ad-hoc practice in the codebase.
- **Correctness over pattern**: When existing code is incorrect — whether it violates these principles, Go best practices, robustness principles, testability, Go idioms, readability, security, or performance — correctness wins. NEVER replicate a broken pattern just because the codebase uses it. Broken code is not precedent; it is a bug. Code MUST be evaluated against: (1) these principles, (2) Go best practices and idioms, (3) robustness and error handling correctness, (4) testability, (5) readability, (6) security, (7) performance. If a pre-existing deficiency is discovered during a change, fix both the new code and the pre-existing deficiency.
- All code changes MUST verify compliance with these principles.
