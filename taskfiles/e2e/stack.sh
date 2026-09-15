#!/usr/bin/env bash
# Shared setup helpers for the e2e stack targets in taskfiles/e2e.yml
# (the `cell` runner, kafka-ingest, push-validate). Sourced — do not execute directly.
#
# Constitution §X: the license/admin-key/boot/readiness/battery blocks are defined
# once here rather than copy-pasted per target. The parametrized cell runner
# (e2e_boot_cell + e2e_readiness_gate + e2e_kafka_ready + e2e_run_battery) is the
# single source of truth for booting an E2E (edition, backend, suites) cell.

# e2e_ensure_dev_keypair
# Guarantees the dev license keypair (sukko.dev.key + sukko.dev.pub) exists before any
# source build. Every source-built grid cell now compiles with -tags sukko_e2e (the
# GO_BUILD_TAGS env in taskfiles/e2e.yml), which //go:embeds keys/sukko.dev.pub — so the
# file MUST exist for the build to succeed even in Community cells that mint no license
# (the embedded dev key is simply unused there). Regenerates when EITHER half is missing:
# a stale sukko.dev.key without the matching .pub would still fail the embed.
e2e_ensure_dev_keypair() {
  if [ ! -f "ws/internal/shared/license/keys/sukko.dev.key" ] || [ ! -f "ws/internal/shared/license/keys/sukko.dev.pub" ]; then
    (cd ws && go run ./internal/shared/license/genkeys) >&2
  fi
}

# e2e_mint_license <edition> <org> <expiry>
# Ensures the dev signing key exists, mints a dev license token for the given
# edition with the given expiry, and echoes the token. Must run BEFORE boot:
# edition-gated backends (e.g. MESSAGE_BACKEND=kafka) fail-fast at startup without
# the right edition, so `sukko license set` after boot is not usable. <expiry> is
# passed verbatim to `gentoken --expires` (e.g. +1y for a valid cell, -1d for the
# expired→Community degradation cell); it has no default here — the caller's Task
# var owns it (§I single-source).
e2e_mint_license() {
  local edition="$1" org="$2" expiry="$3"
  e2e_ensure_dev_keypair
  (cd ws && go run ./internal/shared/license/gentoken \
    --key internal/shared/license/keys/sukko.dev.key \
    --edition "$edition" --org "$org" --expires "$expiry")
}

# e2e_gen_admin_key <abs_path>
# Generates the tester admin keypair: raw 64-byte private key written to
# <abs_path> (mounted into the tester by the compose override), base64 public
# key echoed (wired to provisioning as ADMIN_BOOTSTRAP_KEY).
e2e_gen_admin_key() {
  local path="$1"
  (cd ws && go run ./cmd/gen-admin-key "$path")
}

# e2e_readiness_gate <provisioning_url> <want_edition> <want_expired>
# Asserts provisioning /edition reports the expected edition AND expired flag before
# any suite runs. Fails CLOSED (§XV/§XVIII) — a stack running the wrong edition, or a
# valid-license cell silently running on an expired/degraded license, would skip or
# misvalidate edition-gated suites, and a silent skip is indistinguishable from green.
# Both fields are validated for presence AND type (string edition, boolean expired);
# an unreachable, non-JSON, or malformed /edition is a failure, never a defaulted pass.
# The expired→Community downgrade is resolved synchronously at license load and .expired
# is recomputed per request, so this asserts ONCE with no retry (a retry would mask a
# genuinely mis-configured `exp`).
e2e_readiness_gate() {
  local prov_url="$1" want_edition="$2" want_expired="$3" body edition expired
  if ! body=$(curl -sf "$prov_url/edition"); then
    echo "FAIL: /edition unreachable at $prov_url" >&2
    return 1
  fi
  if ! echo "$body" | jq empty 2>/dev/null; then
    echo "FAIL: /edition returned non-JSON body" >&2
    return 1
  fi
  # Fail closed if either field is absent or the wrong JSON type (never treat an absent
  # field as its zero value). jq -e sets exit status from the boolean result.
  if ! echo "$body" | jq -e '(.edition | type) == "string"' >/dev/null 2>&1; then
    echo "FAIL: /edition .edition absent or not a string" >&2
    return 1
  fi
  if ! echo "$body" | jq -e '(.expired | type) == "boolean"' >/dev/null 2>&1; then
    echo "FAIL: /edition .expired absent or not a boolean" >&2
    return 1
  fi
  edition=$(echo "$body" | jq -r '.edition')
  expired=$(echo "$body" | jq -r '.expired')
  if [ "$edition" != "$want_edition" ]; then
    echo "FAIL: expected edition=$want_edition, got $edition" >&2
    return 1
  fi
  if [ "$expired" != "$want_expired" ]; then
    echo "FAIL: expected expired=$want_expired, got $expired" >&2
    return 1
  fi
  echo "  edition: $edition, expired: $expired ✓"
}

# e2e_boot_cell <edition> <backend> <admin_key_abs_path> <expiry> <compose_cmd...>
# The single boot block for an E2E cell. Derives license from the edition
# (Community sets NO key — the no-license default; Pro/Enterprise mint an
# edition-scoped token BEFORE boot with the given <expiry>, since edition-gated
# backends fail-fast at startup), generates the tester admin key, and boots the
# build-from-source stack with the requested message backend. `<compose_cmd...>`
# is the full compose invocation (base + backend override [+ profiles]) passed as
# trailing words. The `--wait` is bounded by E2E_WAIT_TIMEOUT (default 300s) so a
# never-healthy dependency cannot stretch the job toward the CI limit.
e2e_boot_cell() {
  local edition="$1" backend="$2" admin_key_path="$3" expiry="$4"
  shift 4
  local token="" admin_key
  # The source build below compiles with -tags sukko_e2e, which embeds sukko.dev.pub —
  # so the dev keypair must exist for EVERY cell, including Community cells that mint no
  # license (without this, their build fails at the //go:embed). Minting cells regenerate
  # it too via e2e_mint_license; the guard is idempotent.
  e2e_ensure_dev_keypair
  if [ "$edition" != "community" ]; then
    echo "=== Mint $edition token, expiry=$expiry (before boot) ===" >&2
    token=$(e2e_mint_license "$edition" "E2E ${edition}" "$expiry")
  else
    echo "=== Community cell — no license key (dev key still embedded for the sukko_e2e build) ===" >&2
  fi
  admin_key=$(e2e_gen_admin_key "$admin_key_path")
  echo "=== Boot $backend-mode stack (build from source, edition=$edition) ===" >&2
  # SUKKO_LICENSE_KEY empty ⇒ Community. MESSAGE_BACKEND=direct equals the Go
  # default (a valid, non-empty value — safe through the compose bare-key passthrough).
  MESSAGE_BACKEND="$backend" \
  SUKKO_LICENSE_KEY="$token" \
  ADMIN_BOOTSTRAP_KEY="$admin_key" \
  CREDENTIALS_ENCRYPTION_KEY="$(openssl rand -hex 32)" \
  WEBHOOK_INTERNAL_TOKEN="$(openssl rand -hex 24)" \
  "$@" up -d --build --wait --wait-timeout "${E2E_WAIT_TIMEOUT:-300}"
}

# e2e_kafka_ready <compose_cmd...>
# Kafka-backend readiness gate (in addition to e2e_readiness_gate's /edition check):
# asserts Redpanda cluster health before any suite runs. This mirrors the proven,
# CI-green kafka-ingest readiness (§XVIII).
#
# NOTE: it deliberately does NOT gate on ws-server /ready. /ready is the #179 *control-plane*
# registry-snapshot gate (topic→tenant map applied) — it is NOT the *data-plane* Kafka consumer
# partition-assignment signal, so it never addressed the consumer-subscription-timing race it was
# first added for. That race is absorbed the same way kafka-ingest absorbs it: each validate suite
# provisions its own throwaway tenant and waits out delivery on the tester side. Gating on /ready
# here only added a failure mode (a cold build-from-source stack can take >60s to apply the first
# snapshot) without buying determinism.
e2e_kafka_ready() {
  echo "=== Kafka readiness: rpk cluster health ===" >&2
  "$@" exec -T redpanda rpk cluster health
}



# _e2e_suite_scoped_checks <list> <want_suite>
# Splits a ;-separated list of `<suite>:<check name>` entries and echoes (one per line)
# the <check name>s whose <suite> equals <want_suite>. Entry separator is `;` because
# check names contain spaces (e.g. "connection limit rejection"); suite and check names
# never contain `;`. Empty entries are ignored. Used to scope both the skip allow-list
# and the require-pass list to the suite currently being judged, so a name allowed for
# one suite is never silently tolerated in another.
_e2e_suite_scoped_checks() {
  local list="$1" want_suite="$2" entry esuite echeck
  local IFS=';'
  for entry in $list; do
    [ -n "$entry" ] || continue
    esuite="${entry%%:*}"
    echeck="${entry#*:}"
    if [ "$esuite" = "$want_suite" ]; then
      printf '%s\n' "$echeck"
    fi
  done
}

# e2e_battery_verdict <suite_name> <allowed_skips> <require_pass>   (raw stream on STDIN)
# The generic anti-vacuous guard for one validate suite: extracts the final report JSON
# line (progress lines ignored) and returns 0 IFF ALL hold: the report .status is "pass",
# every skipped check is declared for THIS suite in <allowed_skips>, and every check named
# for THIS suite in <require_pass> is present AND "pass". Any undeclared skip, any
# fail/error, or a required check that is absent/skipped fails the cell. <allowed_skips>
# and <require_pass> are ;-separated `<suite>:<check>` lists (default empty). Prints a
# one-line verdict. Kept stdin-driven so it is testable with canned fixtures
# (battery_guard_test.sh) without booting a stack.
e2e_battery_verdict() {
  local suite="$1" allowed_skips="${2:-}" require_pass="${3:-}"
  local report status fails errs allowed required skip_names undeclared="" rq st missing=""
  report=$(grep '"test_type"' | tail -1)
  if [ -z "$report" ]; then
    echo "    $suite: NO REPORT (suite emitted no test_type report)" >&2
    return 1
  fi
  if ! echo "$report" | jq empty 2>/dev/null; then
    echo "    $suite: report is not valid JSON" >&2
    return 1
  fi
  status=$(echo "$report" | jq -r '.status // "missing"')
  # Include each failed check's .error — the compose stack is torn down with -v, so this
  # line is the only surviving evidence of WHY a check failed.
  fails=$(echo "$report" | jq -r '[.checks[]? | select(.status=="fail") | .name + (if (.error // "") != "" then " (" + .error + ")" else "" end)] | join(", ")')
  # status=="error" reports (setup/dispatch failure before any check ran) carry the cause
  # in the top-level .errors array, not in .checks — print it or the run is undiagnosable.
  errs=$(echo "$report" | jq -r '(.errors // []) | join("; ")')
  # Fails on any fail/error (status != pass), regardless of the allow-list.
  if [ "$status" != "pass" ]; then
    echo "    $suite: $status (failed checks: ${fails:-<none listed>}${errs:+; errors: $errs})" >&2
    return 1
  fi
  # Skips: tolerate ONLY those whose <suite>:<check> is declared for this suite.
  allowed=$(_e2e_suite_scoped_checks "$allowed_skips" "$suite")
  skip_names=$(echo "$report" | jq -r '.checks[]? | select(.status=="skip") | .name')
  while IFS= read -r sk; do
    [ -n "$sk" ] || continue
    if ! printf '%s\n' "$allowed" | grep -Fxq "$sk"; then
      undeclared="$undeclared${undeclared:+, }$sk"
    fi
  done <<< "$skip_names"
  if [ -n "$undeclared" ]; then
    echo "    $suite: UNDECLARED SKIPPED CHECKS: $undeclared" >&2
    return 1
  fi
  # Require-pass: each check named for this suite MUST be present AND pass (present-and-pass,
  # mirrors push_validate_guard). Guards against a vacuous green where a required boundary
  # check is legitimately absent on some editions (so it can't live in the shared verdict).
  required=$(_e2e_suite_scoped_checks "$require_pass" "$suite")
  while IFS= read -r rq; do
    [ -n "$rq" ] || continue
    st=$(echo "$report" | jq -r --arg n "$rq" 'first(.checks[]? | select(.name==$n) | .status) // "absent"')
    if [ "$st" != "pass" ]; then
      missing="$missing${missing:+, }$rq=$st"
    fi
  done <<< "$required"
  if [ -n "$missing" ]; then
    echo "    $suite: REQUIRED-PASS CHECKS not present-and-pass: $missing" >&2
    return 1
  fi
  echo "    $suite: pass ✓"
}

# e2e_run_battery <tester_token> <allowed_skips> <require_pass> <suite...>
# Runs each suite hermetically (isolated HOME/XDG_CONFIG_HOME so no ambient CLI
# context leaks in and suppresses the throwaway-tenant auto-create) and applies
# e2e_battery_verdict to each. <allowed_skips> and <require_pass> are ;-separated
# `<suite>:<check>` lists (default empty), passed as dedicated NAMED leading args so
# they are unambiguously separated from the variadic suite list; each verdict scopes
# them to its own suite. All suites run even after one fails; returns non-zero listing
# the failed suites. The report is parsed rather than trusting the CLI exit code — the
# extraction pipe masks it.
e2e_run_battery() {
  local tester_token="$1" allowed_skips="$2" require_pass="$3"
  shift 3
  local suites=("$@")
  local tmpcfg failed="" suite rc
  tmpcfg=$(mktemp -d)
  for suite in "${suites[@]}"; do
    echo "--- suite: $suite ---"
    rc=0
    HOME="$tmpcfg" XDG_CONFIG_HOME="$tmpcfg/.config" SUKKO_TESTER_TOKEN="$tester_token" \
      sukko test validate --suite "$suite" --follow --output json 2>/dev/null \
      | e2e_battery_verdict "$suite" "$allowed_skips" "$require_pass" || rc=$?
    [ "$rc" -eq 0 ] || failed="$failed $suite"
  done
  rm -rf "$tmpcfg"
  if [ -n "$failed" ]; then
    echo "=== battery FAILED:$failed ===" >&2
    e2e_dump_service_logs
    return 1
  fi
  echo "=== battery PASSED (${suites[*]}) ==="
}


# e2e_dump_service_logs
# Failure-time evidence capture: on a red battery, dump the tail of the gateway,
# server, and provisioning container logs so intermittent failures (e.g. the
# tenant-B delivery-warmup stuck state, #242) leave a diagnosable trail in CI
# instead of vanishing with `compose down -v`. Container-name matching keeps it
# compose-override-agnostic; every step is best-effort (the dump must never mask
# the battery's own exit status).
e2e_dump_service_logs() {
  echo "=== service logs (failure evidence; tail 200 each) ===" >&2
  for name in $(docker ps -a --format '{{.Names}}' 2>/dev/null | grep -E 'ws-gateway|ws-server|provisioning' || true); do
    echo "--- $name ---" >&2
    docker logs --tail 200 "$name" >&2 2>&1 || true
  done
  # Kafka consumer-group offsets vs log-end — the discriminator for "ingest 0"
  # failures (#255): committed-offset == log-end with lag 0 means the record was
  # fetched and dropped (registry-miss silent drop); lag >= 1 means it was never
  # fetched. rpk speaks the Kafka protocol and ships in the redpanda image, so
  # this only runs when a redpanda broker is present (kafka cells).
  local rp
  rp=$(docker ps --format '{{.Names}}' 2>/dev/null | grep -E 'redpanda' | head -1 || true)
  if [ -n "$rp" ]; then
    echo "=== kafka consumer-group state (failure evidence) ===" >&2
    docker exec "$rp" rpk group describe local-shared-consumer >&2 2>&1 || true
    echo "--- topic list (name/partitions) ---" >&2
    docker exec "$rp" rpk topic list >&2 2>&1 || true
    echo "=== end kafka state ===" >&2
  fi
  echo "=== end service logs ===" >&2
}

# push_validate_guard
# Anti-vacuous-green guard for the `push` delivery suite. Reads the raw `sukko test
# validate` stream on STDIN, extracts the final report JSON line (progress lines are
# ignored), and asserts ALL THREE conditions — omitting any one lets a real regression
# ship green:
#   (a) the "push delivery" check is present AND status=="pass". Catches a receiver-host
#       skip (emitted status:"skip" → not "pass") AND a pre-delivery abort / early
#       "push available" 503/403 skip (the check is absent — the suite returns before
#       appending it).
#   (b) NO check has status=="skip" (empty allow-list). Defensive backstop for any future
#       check that skips WITHOUT early-returning; in the current suite (a) already catches
#       every skip path.
#   (c) the report .status field is "pass". The load-bearing backstop for the append-only
#       back half of the suite (credential/channel-config CRUD, subscribe, multiprovider),
#       which only pass/fail: a back-half failure leaves "push delivery":pass + no skip but
#       report .status:"fail" (validate.go computes .status=fail iff any check failed; a
#       skip leaves it "pass"). Without (c) the guard exits 0 on a failed back-half check.
# Exits non-zero with a clear message (naming any failing/skipped checks) on any breach.
push_validate_guard() {
  local report status delivery skips fails
  report=$(grep '"test_type"' | tail -1)
  if [ -z "$report" ]; then
    echo "FAIL: push-validate guard: no report line (suite emitted no test_type report — pre-report abort)" >&2
    return 1
  fi
  if ! echo "$report" | jq empty 2>/dev/null; then
    echo "FAIL: push-validate guard: report is not valid JSON" >&2
    return 1
  fi
  # Keep .error in the printed summary — dropping it makes a red run undiagnosable from CI
  # logs alone (the stack is torn down with -v, so this line is the only surviving evidence).
  echo "$report" | jq '{status, checks:[.checks[]? | {name,status} + (if (.error // "") != "" then {error:.error} else {} end)]}'
  status=$(echo "$report" | jq -r '.status // "missing"')
  delivery=$(echo "$report" | jq -r '.checks[]? | select(.name=="push delivery") | .status')
  skips=$(echo "$report" | jq -r '[.checks[]? | select(.status=="skip") | .name] | join(",")')
  fails=$(echo "$report" | jq -r '[.checks[]? | select(.status=="fail") | .name + (if (.error // "") != "" then " (" + .error + ")" else "" end)] | join(", ")')
  # (a) delivery present AND pass
  if [ "$delivery" != "pass" ]; then
    echo "FAIL: push-validate guard: 'push delivery' status=${delivery:-<absent>} (want pass)" >&2
    return 1
  fi
  # (b) no skipped checks (empty allow-list)
  if [ -n "$skips" ]; then
    echo "FAIL: push-validate guard: skipped checks (empty allow-list): $skips" >&2
    return 1
  fi
  # (c) report status pass (backstops the append-only back-half checks)
  if [ "$status" != "pass" ]; then
    echo "FAIL: push-validate guard: report status=$status (failed checks: ${fails:-<none listed>})" >&2
    return 1
  fi
  echo "  push-validate guard: delivery=pass, no skips, report=pass ✓"
}

# ===========================================================================
# Sentinel cell helpers (cell:community-direct-sentinel)
# ---------------------------------------------------------------------------
# Named constants + six PURE verdicts (stdin/args only — no clock, no docker,
# fixture-covered by sentinel_guard_test.sh) + thin glue (docker/clock I/O around
# the verdicts, untested by design). See ws/docs/e2e-testing.md ("Sentinel HA cell").
# ===========================================================================

# Sentinel topology timing knobs. These MUST equal the pinned directives in
# taskfiles/e2e/valkey-sentinel.override.yml (lockstep-checked by sentinel_guard_test.sh).
E2E_SENTINEL_DOWN_AFTER_MS=5000
E2E_SENTINEL_FAILOVER_TIMEOUT_MS=10000
# Pinned static IPs + subnet for the all-IP sentinel topology. Monitoring the master by a STABLE IP
# (not a container hostname) is what lets failover survive the master's death: a Docker DNS name stops
# resolving the instant the container dies, which stalls +odown and trips TILT (the bug this fixes).
# These are the lockstep source of truth — mirrored, non-interpolable, into valkey-sentinel.override.yml
# and asserted equal by sentinel_guard_test.sh. 10.89.0.0/24 sits OUTSIDE Docker's default address
# pools (172.16.0.0/12 + 192.168.0.0/16) so the user-defined subnet cannot "Pool overlaps" at boot.
E2E_SENTINEL_MASTER_IP=10.89.0.10       # master `ipv4_address` + `sentinel monitor mymaster <IP>` in the override
E2E_SENTINEL_REPLICA_IP=10.89.0.11      # replica `ipv4_address` + `--replica-announce-ip <IP>` in the override
E2E_SENTINEL_SUBNET_CIDR=10.89.0.0/24   # the `sentinel-net` ipam subnet in the override
# Safety margin (seconds): the delivery probe is a whole `sukko test validate --suite pubsub`
# run (~tens of seconds), so allow room for up to two probe re-runs plus compose-network
# jitter on top of the sentinel detection+failover window.
E2E_RECOVERY_SAFETY_MARGIN_S=75
# The SOLE ms→s conversion seam (÷1000): the deadline and every elapsed value the recovery
# verdict consumes are SECONDS (the live loop measures elapsed via `date +%s` deltas — bash
# $SECONDS is NOT special under Task's mvdan/sh shell, so it must not be used for timing).
E2E_RECOVERY_DEADLINE_S=$(( (E2E_SENTINEL_DOWN_AFTER_MS + E2E_SENTINEL_FAILOVER_TIMEOUT_MS) / 1000 + E2E_RECOVERY_SAFETY_MARGIN_S ))
# Headroom gate: a recovery that consumes MORE than this fraction of the deadline reds
# the cell — proof the deadline is not flake-tight (widen it if this trips).
E2E_RECOVERY_HEADROOM_PCT=66

# e2e_sentinel_role_verdict   (raw `SENTINEL master mymaster` reply on STDIN)
# PURE. Returns 0 IFF the reply is a genuine sentinel master record for `mymaster`
# (name=mymaster, flags contain "master", num-other-sentinels=2, quorum=2). Fails closed on an
# empty reply or a data-node error reply (`ERR unknown command`). Backs Scenario 2
# AC-3: proves the configured endpoints are genuinely SENTINEL-role, not three data nodes that
# would satisfy len(addrs)>1 while leaving failover untested.
e2e_sentinel_role_verdict() {
  local reply prev="" line name="" flags="" others="" quorum=""
  reply=$(cat)
  if [ -z "$reply" ]; then
    echo "    sentinel-role: EMPTY reply (sentinel unreachable / wrong port)" >&2
    return 1
  fi
  if printf '%s' "$reply" | grep -qiE 'unknown (sub)?command|^ERR|wrong number of arguments'; then
    echo "    sentinel-role: data-node reply, not a sentinel: $(printf '%s' "$reply" | head -1)" >&2
    return 1
  fi
  # `SENTINEL master` returns a flat key/value list, one token per line: each key line is
  # followed by its value line. Walk pairs (prev = key, current = value).
  while IFS= read -r line; do
    case "$prev" in
      name) name="$line" ;;
      flags) flags="$line" ;;
      num-other-sentinels) others="$line" ;;
      quorum) quorum="$line" ;;
    esac
    prev="$line"
  done <<EOF
$reply
EOF
  if [ "$name" = "mymaster" ] && printf '%s' "$flags" | grep -q "master" && [ "$others" = "2" ] && [ "$quorum" = "2" ]; then
    echo "    sentinel-role: mymaster present (flags=$flags, other-sentinels=$others, quorum=$quorum) ✓"
    return 0
  fi
  echo "    sentinel-role: FAILED (name='$name' flags='$flags' others='$others' quorum='$quorum'; want mymaster/master/2/2)" >&2
  return 1
}

# e2e_sentinel_mode_log_verdict   (ws-server logs on STDIN)
# PURE. Returns 0 IFF the broadcast bus logged Sentinel mode: a `"mode":"sentinel"` line is
# present AND no `"mode":"direct"` line is (the two are emitted only by the broadcast bus's
# connect log, valkey.go:170-179). Positive control for Scenario 1 AC-1a — discriminates a
# genuinely Sentinel-engaged run from an accidentally-direct-but-working one.
e2e_sentinel_mode_log_verdict() {
  local logs
  logs=$(cat)
  if printf '%s\n' "$logs" | grep -q '"mode":"direct"'; then
    echo "    mode-log: found a DIRECT-mode broadcast-bus line — Sentinel branch NOT taken" >&2
    return 1
  fi
  if printf '%s\n' "$logs" | grep -q '"mode":"sentinel"'; then
    echo "    mode-log: broadcast bus mode=sentinel ✓"
    return 0
  fi
  echo "    mode-log: no \"mode\":\"sentinel\" broadcast-bus line found" >&2
  return 1
}

# e2e_recovery_verdict <killed_master> <deadline_s> <headroom_pct>   (outcome stream on STDIN)
# PURE. The stream is append-only outcome lines from the failover loop:
#   promoted <new_master> <elapsed_s>   | delivered <elapsed_s> | nodelivery <elapsed_s> <err…>
# A recovery is CREDITED iff a `delivered` line appears AFTER a `promoted` line whose master
# differs from <killed_master> (signal-first-by-presence: the whole stream is scanned, so a
# delivered following earlier nodelivery lines is still found — a delivery is never discarded
# just because the loop's prior poll ticked nodelivery). Returns 0 iff credited AND the
# recovery elapsed ≤ deadline×headroom_pct/100 (headroom gate — a recovery consuming
# more than the headroom, incl. anything at/after the deadline, reds the cell so the deadline
# gets widened). Fails closed on: no valid promotion (h: promoted==killed), delivery only
# before promotion (g: old-master false-positive), no delivery at all (b/c: deadline expired).
e2e_recovery_verdict() {
  local killed="$1" deadline="$2" headroom_pct="$3"
  local line kind promoted_ok=0 promoted_master="" recovery_elapsed="" limit
  while read -r kind rest; do
    case "$kind" in
      promoted)
        set -- $rest
        if [ "${1:-}" != "$killed" ] && [ -n "${1:-}" ]; then
          promoted_ok=1
          promoted_master="$1"
        fi
        ;;
      delivered)
        set -- $rest
        # Credit only the FIRST delivery that follows a valid promotion.
        if [ "$promoted_ok" = 1 ] && [ -z "$recovery_elapsed" ]; then
          recovery_elapsed="${1:-}"
        fi
        ;;
      *) : ;; # nodelivery / blank — ignored (scan continues; signal-first-by-presence)
    esac
  done
  if [ -z "$recovery_elapsed" ]; then
    echo "    recovery: NO credited delivery (valid promotion seen=$promoted_ok, new_master='$promoted_master' vs killed='$killed'; deadline ${deadline}s expired)" >&2
    return 1
  fi
  limit=$(( deadline * headroom_pct / 100 ))
  if [ "$recovery_elapsed" -le "$limit" ]; then
    echo "    recovery: delivered ${recovery_elapsed}s after promotion to '$promoted_master' (≤ ${limit}s = ${headroom_pct}% of ${deadline}s) ✓"
    return 0
  fi
  echo "    recovery: FLAKE-TIGHT — delivered at ${recovery_elapsed}s > headroom ${limit}s (${headroom_pct}% of ${deadline}s deadline); widen the deadline" >&2
  return 1
}

# e2e_sentinel_failover_log_verdict   (sentinel event logs on STDIN)
# PURE. Returns 0 IFF the failover completed cleanly: a `+switch-master mymaster` line is present AND
# there are ZERO `Failed to resolve hostname` lines AND ZERO `+tilt` transitions. The latter two are
# the fingerprint of the hostname-resolution regression this cell guards (a dead master's DNS name
# fails to resolve → sentinels stall and enter TILT → no failover). Fails closed with a specific
# reason so a regression that still limps to a promotion inside the deadline (possible on one runner's
# DNS timing) is caught by CAUSE, not just outcome.
e2e_sentinel_failover_log_verdict() {
  local logs
  logs=$(cat)
  if printf '%s\n' "$logs" | grep -qF 'Failed to resolve hostname'; then
    echo "    failover-log: 'Failed to resolve hostname' present — hostname re-resolution regressed (monitor-by-IP broken)" >&2
    return 1
  fi
  if printf '%s\n' "$logs" | grep -qF '+tilt'; then
    echo "    failover-log: sentinel entered +tilt — failover was suspended (resolution thrash / clock skew)" >&2
    return 1
  fi
  if printf '%s\n' "$logs" | grep -qF '+switch-master mymaster'; then
    echo "    failover-log: +switch-master mymaster present, no resolve failures, no tilt ✓"
    return 0
  fi
  echo "    failover-log: no '+switch-master mymaster' line — sentinels never promoted a new master" >&2
  return 1
}

# e2e_master_addr_reduce   (raw `SENTINEL get-master-addr-by-name` reply on STDIN)
# PURE. The reply is two lines — `ip\nport` — so emit ONLY the ip (the first line). Extracting this
# into a pure step (rather than an inline `head -1` in the glue) makes the ip/port split — the seam
# between "the promoted replica IP" and the trailing data port "6379" — fixture-testable.
e2e_master_addr_reduce() {
  head -1
}

# e2e_lockstep_verdict <want> <got>   (args only, no STDIN)
# PURE value-in → exit-code-out compare: returns 0 IFF want == got, else 1 with a drift message. Used
# by sentinel_guard_test.sh's lockstep checks so the FAIL side is fixture-testable (feed a drifted
# `got`, assert exit 1) — unlike a counter-mutating side-effect, which only ever runs against the
# in-sync file.
e2e_lockstep_verdict() {
  local want="$1" got="$2"
  if [ "$got" = "$want" ]; then
    echo "    lockstep: '$got' == '$want' ✓"
    return 0
  fi
  echo "    lockstep: DRIFT — got '$got', want '$want'" >&2
  return 1
}

# --- Thin glue (docker/clock I/O around the pure verdicts; untested by design) ---

# e2e_sentinel_ready <compose…>
# Queries a sentinel for the master record and pipes it to e2e_sentinel_role_verdict
# (Scenario 2 AC-3). Exits non-zero (fails the cell) if the endpoints are not genuine sentinels.
e2e_sentinel_ready() {
  echo "  Sentinel role check (SENTINEL master mymaster)…"
  "$@" exec -T valkey-sentinel-1 valkey-cli -p 26379 SENTINEL master mymaster 2>/dev/null \
    | e2e_sentinel_role_verdict
}

# e2e_sentinel_mode_log_check <compose…>
# Pipes ws-server logs to e2e_sentinel_mode_log_verdict — the positive control that the broadcast
# bus took the Sentinel branch (Scenario 1 AC-1a). Fails the cell if mode=sentinel is absent.
e2e_sentinel_mode_log_check() {
  echo "  Broadcast-bus mode=sentinel positive control…"
  "$@" logs ws-server 2>&1 | e2e_sentinel_mode_log_verdict
}

# e2e_sentinel_failover <tester_token> <compose…>
# P2 chaos phase: hard-kill the master, then (phase 1) wait for the sentinels to
# promote a NEW master — proof a real failover occurred — and (phase 2) probe broadcast-delivery
# recovery THROUGH the promoted master. Structuring promotion BEFORE the delivery probe means a
# credited delivery is unambiguously post-promotion (no old-master false-positive) and the logged
# elapsed values are real. Every outcome line is BOTH written to the stream the pure
# e2e_recovery_verdict judges AND echoed (`P2>`) so the CI log — the only surviving evidence after
# `down -v` — shows the genuine failover timeline. Delivery probe reuses e2e_run_battery (§X;
# tester-driver); cold-recovery instrument (a fresh pubsub round-trip proves the
# server-side bus followed the sentinels to the promoted master). Fails closed if EITHER gate reds:
# (1) e2e_recovery_verdict (credited post-promotion delivery within the headroom) OR
# (2) e2e_sentinel_failover_log_verdict (+switch-master with zero +tilt / zero resolve-failure — the
# cause-level discriminator that catches a hostname-monitoring regression).
e2e_sentinel_failover() {
  local token="$1"; shift
  local killed killed_cid newmaster stream start promoted=0 sentinel_logs rc_recovery rc_log
  stream=$(mktemp)
  # emit: machine line → stream (for the verdict) AND human line → log (durable evidence).
  emit() { echo "$1" >> "$stream"; echo "    P2> $1"; }

  killed=$("$@" exec -T valkey-sentinel-1 valkey-cli -p 26379 SENTINEL get-master-addr-by-name mymaster 2>/dev/null | e2e_master_addr_reduce)
  killed_cid=$("$@" ps -q valkey)
  echo "  pre-kill master: ${killed:-<unknown>} (container ${killed_cid:-<none>})"
  echo "  SIGKILL the master container (valkey)…"
  docker kill -s SIGKILL "$killed_cid" >/dev/null
  # Elapsed is measured via `date +%s` deltas — bash $SECONDS is NOT special under Task's
  # mvdan/sh shell (it stays 0), which would vacuate the deadline AND the headroom gate.
  start=$(date +%s)

  # Phase 1 — wait for the sentinels to promote a replica (bounded by the derived deadline). A
  # promotion requires the master to be marked +odown (≥ down-after-milliseconds) + a successful
  # election, so its presence is the proof a real failover happened (not delivery-never-broke).
  echo "  [phase 1] waiting for sentinel promotion (down-after ${E2E_SENTINEL_DOWN_AFTER_MS}ms)…"
  while [ "$(( $(date +%s) - start ))" -lt "$E2E_RECOVERY_DEADLINE_S" ]; do
    newmaster=$("$@" exec -T valkey-sentinel-1 valkey-cli -p 26379 SENTINEL get-master-addr-by-name mymaster 2>/dev/null | e2e_master_addr_reduce)
    if [ -n "$newmaster" ] && [ "$newmaster" != "$killed" ]; then
      emit "promoted $newmaster $(( $(date +%s) - start ))"
      promoted=1
      break
    fi
    sleep 1
  done
  [ "$promoted" = 1 ] || echo "    P2> (no promotion within ${E2E_RECOVERY_DEADLINE_S}s)"

  # Phase 2 — probe broadcast delivery until it recovers through the promoted master (bounded).
  # A probe that starts under the deadline runs to completion and emits its outcome before the
  # loop re-checks — so a delivery is never discarded at the boundary (deadline-tie guarantee).
  echo "  [phase 2] probing broadcast delivery through the promoted master…"
  while [ "$(( $(date +%s) - start ))" -lt "$E2E_RECOVERY_DEADLINE_S" ]; do
    # Stamp elapsed at CONFIRMATION (after the probe), not at probe start — the probe is a full
    # `sukko test validate` run (~tens of seconds), so a start-stamp would undercount recovery
    # latency by a whole probe and could false-pass the headroom gate. Conservative: the
    # recovery elapsed is when delivery was PROVEN working.
    if e2e_run_battery "$token" "" "pubsub:public round-trip" pubsub >/dev/null 2>&1; then
      emit "delivered $(( $(date +%s) - start ))"
      break
    fi
    emit "nodelivery $(( $(date +%s) - start )) probe-failed"
    sleep 1
  done

  # Durable evidence (the stack is torn down with -v): the sentinel's own failover event log. Capture
  # once, reuse for both the evidence display and the log verdict. `+tilt` / `Failed to resolve` are
  # included in the shown events so a regression's fingerprint is visible in the CI log.
  echo "  [evidence] sentinel-1 failover events:"
  sentinel_logs=$("$@" logs valkey-sentinel-1 2>&1)
  printf '%s\n' "$sentinel_logs" | grep -iE '\+sdown|\+odown|\+switch-master|\+failover-state|\+tilt|Failed to resolve' | tail -12 | sed 's/^/    ev> /'

  # TWO independent gates — BOTH must pass (fail closed if either reds):
  #  (1) recovery: a credited post-promotion delivery within the headroom (from the outcome stream).
  #  (2) failover-log: the sentinels actually reached +switch-master with ZERO resolve-failures / ZERO
  #      +tilt — the CAUSE-level discriminator that catches a hostname-monitoring regression even if a
  #      promotion still squeaked through inside the deadline.
  # Capture BOTH verdicts without aborting under the live `set -e` cell shell — the file's
  # errexit-safe idiom is `|| rc=$?` (cf. e2e_boot_cell / e2e_battery_verdict). This keeps both
  # gates actually evaluated (so both diagnostics print even when one reds) and the cleanup reached,
  # then combines — fail closed if EITHER the recovery OR the cause-level log verdict red.
  rc_recovery=0
  e2e_recovery_verdict "$killed" "$E2E_RECOVERY_DEADLINE_S" "$E2E_RECOVERY_HEADROOM_PCT" < "$stream" || rc_recovery=$?
  rc_log=0
  printf '%s\n' "$sentinel_logs" | e2e_sentinel_failover_log_verdict || rc_log=$?
  rm -f "$stream"
  [ "$rc_recovery" -eq 0 ] && [ "$rc_log" -eq 0 ] && return 0
  return 1
}
