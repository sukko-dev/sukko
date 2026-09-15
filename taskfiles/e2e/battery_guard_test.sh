#!/usr/bin/env bash
# Fixture tests for the pure e2e guards in taskfiles/e2e/stack.sh — the anti-vacuous verdict
# the cell runner applies to every validate suite. All run OUTSIDE the Docker stack (canned streams on stdin, exit-code assertions),
# so the guards' contracts have durable regression coverage — not a one-time manual proof
# (Constitution §VIII).
#
# Coverage:
#   • e2e_battery_verdict base guard — each non-pass fixture isolates ONE load-bearing assertion:
#       (a) report .status == "pass"     → status-fail  (the ONLY (b)✓ (a)✗ shape)
#       (b) NO undeclared skip           → check-skip    (the ONLY (a)✓ (b)✗ shape)
#     plus fail-closed paths: no report line, non-JSON report, status=error (with .errors text).
#   • Skip allow-list matrix — per-suite scoped tolerance of NAMED skips only; a fail /
#     error / undeclared / cross-suite name still reds the cell; multiple + empty lists.
#   • Require-pass list — named checks must be present AND pass; absent/skipped fails.
#   • Name coupling — the exported Go check-name constant ↔ the e2e.yml ALLOWED_SKIPS string.
#
# Run: bash taskfiles/e2e/battery_guard_test.sh   (wired into CI before the stack boots).
set -uo pipefail

SCRIPT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=./stack.sh
. "$SCRIPT_DIR/stack.sh"

fails=0

# run_case <want_exit> <name> <raw-stream>
# Pipes the stream to the guard (suite name "channels", empty allow-list/require-pass),
# compares the exit code to <want_exit>.
run_case() {
  local want="$1" name="$2" stream="$3" got
  printf '%s\n' "$stream" | e2e_battery_verdict "channels" >/dev/null 2>&1
  got=$?
  if [ "$got" -eq "$want" ]; then
    echo "ok   - $name (exit $got)"
  else
    echo "FAIL - $name: exit $got, want $want"
    fails=$((fails + 1))
  fi
}

# run_case_ext <want_exit> <name> <suite> <allowed_skips> <require_pass> <raw-stream>
# Same as run_case but exercises the per-suite skip allow-list and require-pass lists.
run_case_ext() {
  local want="$1" name="$2" suite="$3" allowed="$4" require="$5" stream="$6" got
  printf '%s\n' "$stream" | e2e_battery_verdict "$suite" "$allowed" "$require" >/dev/null 2>&1
  got=$?
  if [ "$got" -eq "$want" ]; then
    echo "ok   - $name (exit $got)"
  else
    echo "FAIL - $name: exit $got, want $want"
    fails=$((fails + 1))
  fi
}

# A leading progress line (no "test_type") verifies the guard's grep|tail -1 extraction
# selects the report line, not stray output.
PROGRESS='{"event":"progress","msg":"running channels suite"}'

# 1. Fully-green run → guard passes (exit 0).
run_case 0 "pass" "$PROGRESS
{\"test_type\":\"validate\",\"suite\":\"channels\",\"status\":\"pass\",\"checks\":[{\"name\":\"subscribe\",\"status\":\"pass\"},{\"name\":\"unsubscribe\",\"status\":\"pass\"}]}"

# 2. A check failed → report status=fail. ISOLATES assertion (a): without it a failed suite
#    would ship green.
run_case 1 "status-fail" "$PROGRESS
{\"test_type\":\"validate\",\"suite\":\"channels\",\"status\":\"fail\",\"checks\":[{\"name\":\"subscribe\",\"status\":\"pass\"},{\"name\":\"unsubscribe\",\"status\":\"fail\"}]}"

# 3. A check skipped but report status still pass (a skip leaves .status=pass) → ISOLATES
#    assertion (b): the only shape where (a)✓ but (b)✗. Without it, silently vanished coverage
#    reads as green.
run_case 1 "check-skip" "$PROGRESS
{\"test_type\":\"validate\",\"suite\":\"channels\",\"status\":\"pass\",\"checks\":[{\"name\":\"subscribe\",\"status\":\"pass\"},{\"name\":\"unsubscribe\",\"status\":\"skip\"}]}"

# 4. No report line at all (suite aborted / connect failure before emitting a report) → guard
#    fails closed.
run_case 1 "no-report" "$PROGRESS
{\"event\":\"progress\",\"msg\":\"still connecting\"}"

# 5. Report line present but not valid JSON (truncated/garbled stream) → guard fails closed
#    rather than treating an unparseable report as a pass.
run_case 1 "invalid-json" "$PROGRESS
{\"test_type\":\"validate\",\"suite\":\"channels\",\"status\":\"pass\",\"checks\":[ THIS IS NOT JSON"

# 6. status="error" report (setup/dispatch failed before any check ran — e.g. auth setup):
#    the cause lives in the top-level .errors array, NOT in .checks. The guard must fail AND
#    surface that text — the stack is torn down with -v, so the verdict line is the only
#    surviving evidence (this exact shape made run 29587714847 undiagnosable from CI logs).
ERROR_STREAM="$PROGRESS
{\"test_type\":\"validate\",\"suite\":\"channels\",\"status\":\"error\",\"errors\":[\"auth setup: create tenant: HTTP 403\"]}"
run_case 1 "status-error" "$ERROR_STREAM"
error_out=$(printf '%s\n' "$ERROR_STREAM" | e2e_battery_verdict "channels" 2>&1)
if printf '%s' "$error_out" | grep -q "auth setup: create tenant: HTTP 403"; then
  echo "ok   - status-error surfaces .errors text"
else
  echo "FAIL - status-error: verdict did not print .errors text: $error_out"
  fails=$((fails + 1))
fi

# ---------------------------------------------------------------------------
# Skip allow-list matrix — per-suite, scoped tolerance of NAMED skips only.
# Streams use suite "edition-limits" and the check "connection limit rejection" (spaces in
# the name — the ;-separated entry format must preserve them).
# ---------------------------------------------------------------------------
EL_SKIP='{"test_type":"validate","suite":"edition-limits","status":"pass","checks":[{"name":"tenant limit rejection","status":"pass"},{"name":"connection limit rejection","status":"skip"}]}'
EL_TWO_SKIP='{"test_type":"validate","suite":"edition-limits","status":"pass","checks":[{"name":"connection limit rejection","status":"skip"},{"name":"shard count","status":"skip"}]}'
EL_SKIP_FAILED='{"test_type":"validate","suite":"edition-limits","status":"fail","checks":[{"name":"connection limit rejection","status":"fail","error":"accepted beyond limit"}]}'
EL_ERROR='{"test_type":"validate","suite":"edition-limits","status":"error","errors":["setup failed"],"checks":[{"name":"connection limit rejection","status":"skip"}]}'

# allowed skip → pass
run_case_ext 0 "allowlist: declared skip passes" "edition-limits" \
  "edition-limits:connection limit rejection" "" "$PROGRESS
$EL_SKIP"
# undeclared skip (empty allow-list) → fail
run_case_ext 1 "allowlist: undeclared skip fails" "edition-limits" \
  "" "" "$PROGRESS
$EL_SKIP"
# a fail with an allowed NAME still fails (allow-list tolerates skip status only)
run_case_ext 1 "allowlist: fail with allowed name still fails" "edition-limits" \
  "edition-limits:connection limit rejection" "" "$PROGRESS
$EL_SKIP_FAILED"
# status=error with the allowed name present → fail
run_case_ext 1 "allowlist: status=error with allowed name fails" "edition-limits" \
  "edition-limits:connection limit rejection" "" "$PROGRESS
$EL_ERROR"
# multiple allowed names → pass
run_case_ext 0 "allowlist: multiple allowed skips pass" "edition-limits" \
  "edition-limits:connection limit rejection;edition-limits:shard count" "" "$PROGRESS
$EL_TWO_SKIP"
# empty allow-list + any skip → fail (edition-limits variant of the check-skip case)
run_case_ext 1 "allowlist: empty list + skip fails" "edition-limits" \
  "" "" "$PROGRESS
$EL_SKIP"
# allowed name scoped to a DIFFERENT suite → fail (cross-suite name must not leak tolerance)
run_case_ext 1 "allowlist: name allowed for other suite does not apply" "edition-limits" \
  "channels:connection limit rejection" "" "$PROGRESS
$EL_SKIP"

# ---------------------------------------------------------------------------
# Require-pass list — named checks MUST be present AND pass. Used by the expired
# cell so a legitimately-absent check (Enterprise unlimited path) can't ship a vacuous green.
# ---------------------------------------------------------------------------
RP_PRESENT='{"test_type":"validate","suite":"edition-limits","status":"pass","checks":[{"name":"tenant limit rejection","status":"pass"}]}'
RP_ABSENT='{"test_type":"validate","suite":"edition-limits","status":"pass","checks":[{"name":"routing rules limit rejection","status":"pass"}]}'
RP_SKIPPED='{"test_type":"validate","suite":"edition-limits","status":"pass","checks":[{"name":"tenant limit rejection","status":"skip"}]}'

# required check present-and-pass → pass
run_case_ext 0 "require-pass: present and pass" "edition-limits" \
  "" "edition-limits:tenant limit rejection" "$PROGRESS
$RP_PRESENT"
# required check absent → fail (the vacuous-green guard)
run_case_ext 1 "require-pass: absent required check fails" "edition-limits" \
  "" "edition-limits:tenant limit rejection" "$PROGRESS
$RP_ABSENT"
# required check present but skipped (allowed as a skip, but still not "pass") → fail
run_case_ext 1 "require-pass: present-but-skipped fails" "edition-limits" \
  "edition-limits:tenant limit rejection" "edition-limits:tenant limit rejection" "$PROGRESS
$RP_SKIPPED"
# empty require-pass → no enforcement (absent check is fine)
run_case_ext 0 "require-pass: empty list does not enforce" "edition-limits" \
  "" "" "$PROGRESS
$RP_ABSENT"

# ---------------------------------------------------------------------------
# rest-publish require-pass (delivery anti-vacuous) — the paid grid cells declare TWO required
# checks (";"-separated: "rest publish → ws receive" AND "rest publish → sse receive"). A silently
# ABSENT delivery check (e.g. the tester regresses to only emitting "response format") or a
# non-pass one must red the cell. Fixtures use non-ASCII "→" in the names — the ;-split must
# preserve it verbatim.
# ---------------------------------------------------------------------------
RP_REQ='rest-publish:rest publish → ws receive;rest-publish:rest publish → sse receive'
RESTP_BOTH='{"test_type":"validate","suite":"rest-publish","status":"pass","checks":[{"name":"response format","status":"pass"},{"name":"rest publish → ws receive","status":"pass"},{"name":"rest publish → sse receive","status":"pass"}]}'
RESTP_ABSENT='{"test_type":"validate","suite":"rest-publish","status":"pass","checks":[{"name":"response format","status":"pass"},{"name":"rest publish → ws receive","status":"pass"}]}'
RESTP_WS_FAIL='{"test_type":"validate","suite":"rest-publish","status":"fail","checks":[{"name":"response format","status":"pass"},{"name":"rest publish → ws receive","status":"fail","error":"not received"},{"name":"rest publish → sse receive","status":"pass"}]}'

# both required delivery checks present-and-pass → pass
run_case_ext 0 "rest-publish: both delivery checks pass" "rest-publish" \
  "" "$RP_REQ" "$PROGRESS
$RESTP_BOTH"
# sse delivery check silently absent (only produce + ws emitted) → fail (the vacuous-green guard)
run_case_ext 1 "rest-publish: absent sse delivery check fails" "rest-publish" \
  "" "$RP_REQ" "$PROGRESS
$RESTP_ABSENT"
# a required delivery check present but failed → fail
run_case_ext 1 "rest-publish: failed ws delivery check fails" "rest-publish" \
  "" "$RP_REQ" "$PROGRESS
$RESTP_WS_FAIL"

# ---------------------------------------------------------------------------
# Name cross-file coupling (bash half) — the e2e ALLOWED_SKIPS string ↔ the tester's
# exported check-name constant. bash cannot import a Go const, so grep its VALUE out of the
# source and assert taskfiles/e2e.yml declares edition-limits:<value>. (Go half:
# TestConnLimitRejectionCheckName asserts the emitted CheckResult.Name equals the constant.)
# ---------------------------------------------------------------------------
REPO=$(cd "$SCRIPT_DIR/../.." && pwd)
CONST_VAL=$(grep -E 'ConnLimitRejectionCheckName[[:space:]]*=[[:space:]]*"' \
  "$REPO/ws/cmd/tester/runner/edition_limits.go" | sed -E 's/.*=[[:space:]]*"([^"]*)".*/\1/')
if [ -z "$CONST_VAL" ]; then
  echo "FAIL - name-coupling: could not find exported ConnLimitRejectionCheckName constant in edition_limits.go"
  fails=$((fails + 1))
elif grep -qF "edition-limits:${CONST_VAL}" "$REPO/taskfiles/e2e.yml"; then
  echo "ok   - name-coupling: e2e.yml ALLOWED_SKIPS uses edition-limits:${CONST_VAL} (matches Go constant)"
else
  echo "FAIL - name-coupling: no ALLOWED_SKIPS entry 'edition-limits:${CONST_VAL}' in e2e.yml (constant renamed without updating the allow-list)"
  fails=$((fails + 1))
fi

echo
if [ "$fails" -eq 0 ]; then
  echo "battery_guard_test: all cases passed ✓"
  exit 0
fi
echo "battery_guard_test: $fails case(s) failed ✗"
exit 1
