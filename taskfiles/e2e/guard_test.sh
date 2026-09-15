#!/usr/bin/env bash
# Fixture test for push_validate_guard (taskfiles/e2e/stack.sh).
#
# Runs OUTSIDE the Docker stack: feeds canned raw `sukko test validate` streams to the
# guard on stdin and asserts its exit code, so the guard's anti-vacuous contract has
# durable regression coverage — not a one-time manual proof (Constitution §VIII). Each
# non-pass fixture isolates ONE of the guard's three assertions, so a guard that silently
# drops an assertion is caught here:
#   (a) "push delivery" present AND pass  → delivery-skip / delivery-absent / delivery-fail
#   (b) no check status=="skip"           → non-delivery-skip (the ONLY (a)✓ (c)✓ (b)✗ shape)
#   (c) report .status=="pass"            → crud-fail        (the ONLY (a)✓ (b)✓ (c)✗ shape)
#
# Run: bash taskfiles/e2e/guard_test.sh   (wired into CI before the stack boots).
set -uo pipefail

SCRIPT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=./stack.sh
. "$SCRIPT_DIR/stack.sh"

fails=0

# run_case <want_exit> <name> <raw-stream>
# Pipes the stream to the guard, compares the exit code to <want_exit>.
run_case() {
  local want="$1" name="$2" stream="$3" got
  printf '%s\n' "$stream" | push_validate_guard >/dev/null 2>&1
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
PROGRESS='{"event":"progress","msg":"running push suite"}'

# 1. Fully-green run → guard passes (exit 0).
run_case 0 "pass" "$PROGRESS
{\"test_type\":\"validate\",\"suite\":\"push\",\"status\":\"pass\",\"checks\":[{\"name\":\"push available\",\"status\":\"pass\"},{\"name\":\"push delivery\",\"status\":\"pass\"},{\"name\":\"credential create\",\"status\":\"pass\"}]}"

# 2. Delivery skipped (receiver host unset) → (a) fails: delivery status != pass.
run_case 1 "delivery-skip" "$PROGRESS
{\"test_type\":\"validate\",\"suite\":\"push\",\"status\":\"pass\",\"checks\":[{\"name\":\"push available\",\"status\":\"pass\"},{\"name\":\"push delivery\",\"status\":\"skip\"}]}"

# 3. Delivery check absent (early push-available skip / pre-delivery abort) → (a) fails: absent.
run_case 1 "delivery-absent" "$PROGRESS
{\"test_type\":\"validate\",\"suite\":\"push\",\"status\":\"pass\",\"checks\":[{\"name\":\"push available\",\"status\":\"pass\"}]}"

# 4. Delivery failed (e.g. PUSH_DRY_RUN=true, no real POST) → (a) fails: delivery status=fail.
#    This is the exact regression the feature exists to catch and the only fixture
#    exercising assertion (a)'s fail branch.
run_case 1 "delivery-fail" "$PROGRESS
{\"test_type\":\"validate\",\"suite\":\"push\",\"status\":\"fail\",\"checks\":[{\"name\":\"push delivery\",\"status\":\"fail\"}]}"

# 5. A back-half CRUD check failed → delivery pass + no skip, but report status=fail.
#    ISOLATES assertion (c): without it the guard would exit 0 here (false green).
run_case 1 "crud-fail" "$PROGRESS
{\"test_type\":\"validate\",\"suite\":\"push\",\"status\":\"fail\",\"checks\":[{\"name\":\"push delivery\",\"status\":\"pass\"},{\"name\":\"channel config get\",\"status\":\"fail\"}]}"

# 6. A back-half check skipped without early-return → delivery pass + report pass, but a skip.
#    ISOLATES assertion (b): the only shape where (a)✓ (c)✓ but (b)✗.
run_case 1 "non-delivery-skip" "$PROGRESS
{\"test_type\":\"validate\",\"suite\":\"push\",\"status\":\"pass\",\"checks\":[{\"name\":\"push delivery\",\"status\":\"pass\"},{\"name\":\"multiprovider android\",\"status\":\"skip\"}]}"

# 7. No report line at all (suite aborted before emitting a report) → guard fails closed.
run_case 1 "no-report" "$PROGRESS
{\"event\":\"progress\",\"msg\":\"still connecting\"}"

echo
if [ "$fails" -eq 0 ]; then
  echo "guard_test: all cases passed ✓"
  exit 0
fi
echo "guard_test: $fails case(s) failed ✗"
exit 1
