# Cross-SDK conformance: behavioral parity vectors

Language-neutral scenario fixtures that every Sukko client SDK (`sukko-go`, `sukko-js`,
`sukko-py`) replays through its **pure state machines**. All SDKs must produce the identical
canonical action sequence for the same vector — this is how cross-SDK behavioral parity is
*measured*, not asserted in prose.

This directory is the **canonical home** (platform ADR-0023): the vectors are a contract
artifact, versioned with the AsyncAPI (`../asyncapi/client-ws.asyncapi.yaml`, currently
v1.4.2). Each SDK vendors a checksum-pinned copy into its own test data (sukko-go ADR-0003,
sukko-js ADR-0004, sukko-py ADR-0004) and never vendors from a sibling SDK.

## Schema

```jsonc
{
  "name": "recovery/gap-replay-basic",     // "<machine>/<case>"
  "machine": "recovery",                    // "recovery" | "auth" | "subscriptions"
  "contract_version": "1.4.1",              // the AsyncAPI version these effects are defined against
  "description": "one-line intent",
  "inputs": [                               // ordered; each is one of:
    { "event": "gap", "channel": "acme.orders", "last_pos": "acme.orders:4" },  // a named event + canonical (snake_case) payload keys
    { "advance": 10000 },                   // virtual-time advance in ms (drives timing-gated paths deterministically)
    { "backpressure": true }                // consumer stalled (true) / resumed (false). A client-side
                                            // condition, NOT a wire effect — abstracts over JS's
                                            // transport.pause() and Py's blocking put. While stalled,
                                            // recovery frames stop arriving, so a detection deadline must
                                            // SUSPEND (not fire): the silence is the consumer's, not the
                                            // server's. Bindings map it to their park/suspension signal.
  ],
  "expect": [                               // ordered canonical actions the machine emits:
    { "action": "send_replay", "channel": "acme.orders", "from_pos": "acme.orders:4" }
  ]
}
```

**Canonical encoding**: `action` tags and all payload keys are `snake_case`; object keys are
compared **order-insensitively**, the action **list** is compared **order-sensitively**.
Vectors specify **observable effects only** — frames the client sends and user-visible events
it surfaces — never internal FSM state, and abstract over each SDK's delivery mechanics
(event emitter vs bounded channel vs async iterator), so one corpus binds to all three idioms.

## Machines

- **`recovery`** — the live-gap / reconnect-replay / Direct-degrade FSM. Canonical actions:
  `send_replay{channel, from_pos}`, `send_reconnect{client_id, last_pos}`,
  `emit_possible_gap{channel}`, `raise_recovery_interrupted{channel}`.
- **`auth`** — token refresh / escalation single-flight FSM (vectors added with that machine).
- **`subscriptions`** — subscribe/unsubscribe grant-diff FSM (vectors added with that machine).

## Meta-verification

The platform additionally runs the vectors' inputs against a live compose stack and confirms the
expected frames actually occur, so the corpus is verified against the running server rather than
hand-trusted (added with Tier 2).
