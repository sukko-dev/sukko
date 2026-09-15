//go:build !sukko_e2e

package license

import _ "embed"

// embeddedPublicKeyBytes is the committed license public key for normal (production)
// builds. Today this is a labeled PLACEHOLDER dev P-256 key (keys/sukko.pub) whose
// private half was generated once and discarded — pre-launch no GCP Cloud KMS key
// exists yet. In the KMS-provisioning wave the sukko-license DK-1 workflow replaces
// it (and keys/sukko.fingerprints) via a reviewed cross-repo PR with the production
// KMS public key(s). genkeys MUST NOT write to these paths — it writes keys/sukko.dev.*
// instead — so an e2e run cannot clobber the committed key. The bundle holds
// one "PUBLIC KEY" block in steady state, two during a rotation overlap.
//
//go:embed keys/sukko.pub
var embeddedPublicKeyBytes []byte

// embeddedFingerprintManifest is the committed "<fingerprint> <key-version>" manifest
// matching keys/sukko.pub in lockstep (bijection). Today it pins the
// placeholder key with the "placeholder" sentinel version; the delivery PR replaces both.
//
//go:embed keys/sukko.fingerprints
var embeddedFingerprintManifest []byte
