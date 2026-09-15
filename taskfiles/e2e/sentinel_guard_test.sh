#!/usr/bin/env bash
# Fixture tests for the PURE sentinel-cell verdicts in taskfiles/e2e/stack.sh — the six decisions
# the cell:community-direct-sentinel runner delegates to (recovery, sentinel-role, mode=sentinel
# log, failover-log, master-addr reduce, lockstep compare), plus the name-lockstep check. All run OUTSIDE the Docker stack (canned streams
# on stdin, exit-code assertions), so the verdict contracts have durable regression coverage,
# not a one-time manual proof (§VIII).
#
# Coverage:
#   • e2e_recovery_verdict — signal-first-by-presence + the headroom gate + every
#     fail-closed path (no promotion, promotion==killed, delivery-before-promotion, no delivery,
#     over-headroom). NOTE on "deadline-tie": a pure stream-consuming verdict has no select
#     race (that lives in the LIVE loop, which appends the in-flight outcome before breaking —
#     e2e_sentinel_failover). The verdict's guarantee is that a `delivered` line is credited by
#     PRESENCE even when it follows earlier `nodelivery` lines (case "signal after nodelivery").
#     A delivery at/after the deadline is > headroom → reds, so it is a fail case, not
#     an exit-0 case.
#   • e2e_sentinel_role_verdict — genuine sentinel master record vs empty / data-node reply.
#   • e2e_sentinel_mode_log_verdict — present / absent / wrong-mode (direct) classifier.
#   • e2e_sentinel_failover_log_verdict — +switch-master present AND zero +tilt AND zero
#     resolve-failure; each negative fixture otherwise-passes and drifts ONE condition.
#   • e2e_master_addr_reduce — the two-line get-master-addr `ip\nport` reply → ip only.
#   • e2e_lockstep_verdict — pure value-in→exit-out compare; every pinned override literal (master
#     IP ×2, replica IP ×2, subnet CIDR, resolve/announce-hostnames, timing knobs) match→0, plus a
#     synthetic drift→1 per value-type proving the fail-closed side.
#   • Name-lockstep — the cell's REQUIRE_PASS uses <suite>:<value> where <value> is the tester's
#     deliveryCheck() name arg.
#
# Run: bash taskfiles/e2e/sentinel_guard_test.sh   (wired into CI before the stack boots).
set -uo pipefail

SCRIPT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
REPO=$(cd "$SCRIPT_DIR/../.." && pwd)
# shellcheck source=./stack.sh
. "$SCRIPT_DIR/stack.sh"

fails=0

# run_recovery <want_exit> <name> <killed> <deadline> <headroom> <stream>
run_recovery() {
  local want="$1" name="$2" killed="$3" deadline="$4" headroom="$5" stream="$6" got
  printf '%s\n' "$stream" | e2e_recovery_verdict "$killed" "$deadline" "$headroom" >/dev/null 2>&1
  got=$?
  if [ "$got" -eq "$want" ]; then echo "ok   - $name (exit $got)"; else
    echo "FAIL - $name: exit $got, want $want"; fails=$((fails + 1)); fi
}

# run_role <want_exit> <name> <reply>
run_role() {
  local want="$1" name="$2" reply="$3" got
  printf '%s' "$reply" | e2e_sentinel_role_verdict >/dev/null 2>&1
  got=$?
  if [ "$got" -eq "$want" ]; then echo "ok   - $name (exit $got)"; else
    echo "FAIL - $name: exit $got, want $want"; fails=$((fails + 1)); fi
}

# run_modelog <want_exit> <name> <logs>
run_modelog() {
  local want="$1" name="$2" logs="$3" got
  printf '%s\n' "$logs" | e2e_sentinel_mode_log_verdict >/dev/null 2>&1
  got=$?
  if [ "$got" -eq "$want" ]; then echo "ok   - $name (exit $got)"; else
    echo "FAIL - $name: exit $got, want $want"; fails=$((fails + 1)); fi
}

# ---------------------------------------------------------------------------
# e2e_recovery_verdict (killed=$E2E_SENTINEL_MASTER_IP, deadline=90s, headroom=66% → limit=59s).
# Streams use the LIVE address form the monitor-by-IP topology now emits: killed = the master's pinned
# IP, promoted = the replica's pinned IP, built from the stack.sh constants.
# ---------------------------------------------------------------------------
run_recovery 0 "recovery: delivered in headroom after promotion" \
  "$E2E_SENTINEL_MASTER_IP" 90 66 "promoted $E2E_SENTINEL_REPLICA_IP 3
delivered 10"
run_recovery 0 "recovery: delivered is credited by PRESENCE after nodelivery ticks (signal-first)" \
  "$E2E_SENTINEL_MASTER_IP" 90 66 "promoted $E2E_SENTINEL_REPLICA_IP 3
nodelivery 20 probe-failed
nodelivery 40 probe-failed
delivered 50"
run_recovery 1 "recovery: over-headroom delivery reds (flake-tight)" \
  "$E2E_SENTINEL_MASTER_IP" 90 66 "promoted $E2E_SENTINEL_REPLICA_IP 3
delivered 75"
run_recovery 1 "recovery: at/after deadline reds (> headroom)" \
  "$E2E_SENTINEL_MASTER_IP" 90 66 "promoted $E2E_SENTINEL_REPLICA_IP 3
delivered 95"
run_recovery 1 "recovery: no promotion ever (deadline expired, no delivery)" \
  "$E2E_SENTINEL_MASTER_IP" 90 66 "nodelivery 20 probe-failed
nodelivery 40 probe-failed"
run_recovery 1 "recovery: promotion present but never delivered" \
  "$E2E_SENTINEL_MASTER_IP" 90 66 "promoted $E2E_SENTINEL_REPLICA_IP 3
nodelivery 20 probe-failed
nodelivery 60 probe-failed"
run_recovery 1 "recovery: delivery BEFORE promotion → old-master false-positive not credited" \
  "$E2E_SENTINEL_MASTER_IP" 90 66 "delivered 5
promoted $E2E_SENTINEL_REPLICA_IP 10"
run_recovery 1 "recovery: promoted master == killed master → not a real promotion" \
  "$E2E_SENTINEL_MASTER_IP" 90 66 "promoted $E2E_SENTINEL_MASTER_IP 10
delivered 20"

# ---------------------------------------------------------------------------
# e2e_sentinel_role_verdict — genuine sentinel record vs empty / data-node
# ---------------------------------------------------------------------------
# ip is cosmetic for the role verdict (it checks name/flags/others/quorum), but keep it consistent
# with the pinned topology instead of a stale literal. ($'…' does not interpolate, so concatenate.)
VALID_MASTER=$'name\nmymaster\nip\n'"$E2E_SENTINEL_MASTER_IP"$'\nport\n6379\nflags\nmaster\nnum-other-sentinels\n2\nquorum\n2'
run_role 0 "role: valid mymaster sentinel record passes" "$VALID_MASTER"
run_role 1 "role: empty reply fails closed" ""
run_role 1 "role: data-node error reply fails closed" "ERR unknown command 'SENTINEL'"
run_role 1 "role: wrong master name fails" $'name\nothermaster\nflags\nmaster\nnum-other-sentinels\n2\nquorum\n2'
run_role 1 "role: too-few sentinels fails" $'name\nmymaster\nflags\nmaster\nnum-other-sentinels\n0\nquorum\n2'

# ---------------------------------------------------------------------------
# e2e_sentinel_mode_log_verdict — present / absent / wrong-mode
# ---------------------------------------------------------------------------
run_modelog 0 "mode-log: sentinel line present passes" \
  '{"level":"info","mode":"sentinel","message":"Connecting to Valkey Sentinel"}'
run_modelog 1 "mode-log: no mode line fails" \
  '{"level":"info","message":"some other log"}'
run_modelog 1 "mode-log: direct-mode line fails (wrong mode)" \
  '{"level":"info","mode":"direct","message":"Connecting to Valkey (direct mode)"}'
run_modelog 1 "mode-log: direct present even if sentinel also present fails (mixed → red)" \
  '{"mode":"sentinel"}
{"mode":"direct"}'

# ---------------------------------------------------------------------------
# e2e_sentinel_failover_log_verdict — +switch-master present AND zero +tilt AND zero
# resolve-failure. Each NEGATIVE fixture is otherwise-passing and drifts exactly ONE condition, so each of
# the three gates is proven to fire independently (no over-determined green; discriminate on cause).
# ---------------------------------------------------------------------------
run_failover_log() {
  local want="$1" name="$2" logs="$3" got
  printf '%s\n' "$logs" | e2e_sentinel_failover_log_verdict >/dev/null 2>&1
  got=$?
  if [ "$got" -eq "$want" ]; then echo "ok   - $name (exit $got)"; else
    echo "FAIL - $name: exit $got, want $want"; fails=$((fails + 1)); fi
}
run_failover_log 0 "failover-log: +switch-master present, no tilt, no resolve-fail → pass" \
  "1:X +sdown master mymaster $E2E_SENTINEL_MASTER_IP 6379
1:X +odown master mymaster $E2E_SENTINEL_MASTER_IP 6379
1:X +switch-master mymaster $E2E_SENTINEL_MASTER_IP 6379 $E2E_SENTINEL_REPLICA_IP 6379"
run_failover_log 1 "failover-log: +tilt present → fail (switch-master otherwise present)" \
  "1:X +switch-master mymaster $E2E_SENTINEL_MASTER_IP 6379 $E2E_SENTINEL_REPLICA_IP 6379
1:X +tilt #tilt mode entered"
run_failover_log 1 "failover-log: 'Failed to resolve hostname' present → fail (switch-master otherwise present)" \
  "1:X +switch-master mymaster $E2E_SENTINEL_MASTER_IP 6379 $E2E_SENTINEL_REPLICA_IP 6379
1:X # Failed to resolve hostname 'valkey'"
run_failover_log 1 "failover-log: no +switch-master → fail (never promoted)" \
  "1:X +sdown master mymaster $E2E_SENTINEL_MASTER_IP 6379
1:X +odown master mymaster $E2E_SENTINEL_MASTER_IP 6379"

# ---------------------------------------------------------------------------
# e2e_master_addr_reduce — the two-line `ip\nport` get-master-addr reply reduces to the ip only.
# ---------------------------------------------------------------------------
run_addr_reduce() {
  local want="$1" name="$2" reply="$3" got
  got=$(printf '%s\n' "$reply" | e2e_master_addr_reduce)
  if [ "$got" = "$want" ]; then echo "ok   - $name (got '$got')"; else
    echo "FAIL - $name: got '$got', want '$want'"; fails=$((fails + 1)); fi
}
run_addr_reduce "$E2E_SENTINEL_REPLICA_IP" "addr-reduce: two-line ip/port reply → ip only" \
  "$E2E_SENTINEL_REPLICA_IP
6379"

# ---------------------------------------------------------------------------
# Lockstep (i): every pinned value in the override MUST equal its stack.sh source-of-truth constant.
# The compare is the PURE e2e_lockstep_verdict (value-in → exit-out), so BOTH the match side (the real
# extracted value) AND the drift side (a synthetic wrong value) are fixture-tested — a counter
# side-effect would only ever run against the in-sync file, never proving the fail-closed path.
# Extraction (grep from the override) is glue; the compare is the tested pure verdict.
# ---------------------------------------------------------------------------
OVERRIDE="$REPO/taskfiles/e2e/valkey-sentinel.override.yml"

# run_lockstep <want_exit> <name> <want> <got>
run_lockstep() {
  local want_exit="$1" name="$2" want="$3" got="$4" rc
  e2e_lockstep_verdict "$want" "$got" >/dev/null 2>&1; rc=$?
  if [ "$rc" -eq "$want_exit" ]; then echo "ok   - $name (exit $rc)"; else
    echo "FAIL - $name: exit $rc, want $want_exit"; fails=$((fails + 1)); fi
}

# --- Extraction glue: pull each pinned literal out of the override (master block precedes replica) ---
ip_re='([0-9]{1,3}\.){3}[0-9]{1,3}'
xt_mon_ip=$(grep -E 'sentinel monitor mymaster ' "$OVERRIDE" | awk '{print $4}' | head -1)
xt_replicaof_ip=$(grep -oE "replicaof ${ip_re}" "$OVERRIDE" | grep -oE "$ip_re" | head -1)
xt_announce_ip=$(grep -oE "replica-announce-ip ${ip_re}" "$OVERRIDE" | grep -oE "$ip_re" | head -1)
xt_ipv4_master=$(grep -oE "ipv4_address: ${ip_re}" "$OVERRIDE" | grep -oE "$ip_re" | sed -n 1p)
xt_ipv4_replica=$(grep -oE "ipv4_address: ${ip_re}" "$OVERRIDE" | grep -oE "$ip_re" | sed -n 2p)
xt_subnet=$(grep -oE "subnet: ${ip_re}/[0-9]+" "$OVERRIDE" | grep -oE "${ip_re}/[0-9]+" | head -1)
xt_resolve=$(grep -oE 'resolve-hostnames (no|yes)' "$OVERRIDE" | awk '{print $2}' | head -1)
xt_announce=$(grep -oE 'announce-hostnames (no|yes)' "$OVERRIDE" | awk '{print $2}' | head -1)
xt_down_after=$(grep -E 'down-after-milliseconds mymaster ' "$OVERRIDE" | awk '{print $NF}' | head -1)
xt_failover=$(grep -E 'failover-timeout mymaster ' "$OVERRIDE" | awk '{print $NF}' | head -1)

# Match side — the real override value equals its constant (exit 0):
run_lockstep 0 "lockstep: master IP (sentinel monitor directive)" "$E2E_SENTINEL_MASTER_IP"  "$xt_mon_ip"
run_lockstep 0 "lockstep: master IP (--replicaof)"                 "$E2E_SENTINEL_MASTER_IP"  "$xt_replicaof_ip"
run_lockstep 0 "lockstep: master IP (ipv4_address)"                "$E2E_SENTINEL_MASTER_IP"  "$xt_ipv4_master"
run_lockstep 0 "lockstep: replica IP (--replica-announce-ip)"      "$E2E_SENTINEL_REPLICA_IP" "$xt_announce_ip"
run_lockstep 0 "lockstep: replica IP (ipv4_address)"               "$E2E_SENTINEL_REPLICA_IP" "$xt_ipv4_replica"
run_lockstep 0 "lockstep: subnet CIDR (ipam)"                      "$E2E_SENTINEL_SUBNET_CIDR" "$xt_subnet"
run_lockstep 0 "lockstep: resolve-hostnames no"                    "no"                        "$xt_resolve"
run_lockstep 0 "lockstep: announce-hostnames no"                   "no"                        "$xt_announce"
run_lockstep 0 "lockstep: down-after-milliseconds knob"            "$E2E_SENTINEL_DOWN_AFTER_MS" "$xt_down_after"
run_lockstep 0 "lockstep: failover-timeout knob"                   "$E2E_SENTINEL_FAILOVER_TIMEOUT_MS" "$xt_failover"

# Drift side — a synthetic wrong value MUST red (exit 1), proving the fail-closed path per value-type:
run_lockstep 1 "lockstep drift: master IP mismatch reds"    "$E2E_SENTINEL_MASTER_IP"   "10.89.0.99"
run_lockstep 1 "lockstep drift: replica IP mismatch reds"   "$E2E_SENTINEL_REPLICA_IP"  "10.89.0.99"
run_lockstep 1 "lockstep drift: subnet mismatch reds"       "$E2E_SENTINEL_SUBNET_CIDR" "10.99.0.0/24"
run_lockstep 1 "lockstep drift: resolve-hostnames yes reds" "no"                        "yes"
run_lockstep 1 "lockstep drift: knob mismatch reds"         "$E2E_SENTINEL_DOWN_AFTER_MS" "9999"

# ---------------------------------------------------------------------------
# Lockstep (ii): the cell's REQUIRE_PASS uses <suite>:<value> where <value> is the exact
# deliveryCheck() name arg in the tester source (the pubsub:/reconnect: prefix is the e2e.yml
# scoping convention that _e2e_suite_scoped_checks strips). Extract the Go value, assert the
# composed <suite>:<value> appears in taskfiles/e2e.yml — name-lockstep. (Mirrors the
# battery_guard_test.sh:218-233 extract-then-assert precedent.)
# ---------------------------------------------------------------------------
E2E_YML="$REPO/taskfiles/e2e.yml"
check_require_pass() {
  local gofile="$1" suite="$2" want_hint="$3" val
  val=$(grep -oE "deliveryCheck\(\"${want_hint}\"" "$REPO/ws/cmd/tester/runner/$gofile" \
    | sed -E 's/.*deliveryCheck\("([^"]*)".*/\1/' | head -1)
  if [ -z "$val" ]; then
    echo "FAIL - name-lockstep: deliveryCheck(\"${want_hint}\") not found in $gofile (check renamed)"
    fails=$((fails + 1))
  elif grep -qF "${suite}:${val}" "$E2E_YML"; then
    echo "ok   - name-lockstep: e2e.yml REQUIRE_PASS uses ${suite}:${val} (matches $gofile)"
  else
    echo "FAIL - name-lockstep: no '${suite}:${val}' in e2e.yml REQUIRE_PASS ($gofile check renamed without updating the cell)"
    fails=$((fails + 1))
  fi
}
check_require_pass "validate_pubsub.go" "pubsub" "public round-trip"
check_require_pass "validate_reconnect.go" "reconnect" "post-reconnect delivery"

echo
if [ "$fails" -eq 0 ]; then
  echo "sentinel_guard_test: all cases passed ✓"
  exit 0
fi
echo "sentinel_guard_test: $fails case(s) failed ✗"
exit 1
