# Real-key fixtures (`sukko_realkey`)

This directory holds the **real-key anti-circularity fixtures** — one per embedded
production public key. It is **populated only by the KMS key-delivery PR** (the
sukko-license `key-sync.yml` workflow), never hand-authored.

**Contract:**

- One file per embedded key, named `<fingerprint>.json`, where `<fingerprint>` is the
  full 64-char lowercase SHA-256 hex over the signing key's SPKI DER (the same value as
  the key's line in `keys/sukko.fingerprints`).
- Schema = the golden-vector JSON schema reused from `feat/license-p256`
  (`payload_b64` / `sig_b64` / `key`), signed by the real KMS key.
- The claim set is the pinned GOLDEN payload with a **fixed-past `exp`** (`fixtureExp`),
  so a leaked fixture is a permanently-**expired**, inert license — never a usable
  enterprise credential. The proof is the signature verifying against the embedded key.

The `sukko_realkey`-tagged test (`realkey_test.go`) loads every fixture here, asserts each
verifies against its embedded key and that `ParseAndVerify` returns `ErrLicenseExpired`,
and **hard-fails** (never skips) if a fixture is missing. Empty in the placeholder era.
