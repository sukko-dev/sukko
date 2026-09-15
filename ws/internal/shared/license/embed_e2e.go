//go:build sukko_e2e

package license

import _ "embed"

// embeddedPublicKeyBytes under the `sukko_e2e` build tag is the regenerated dev/e2e
// public key (keys/sukko.dev.pub), whose private half (keys/sukko.dev.key) genkeys
// emits for the tester/gentoken signer. This lets the e2e harness validate license
// keys it mints at runtime while the committed production key (keys/sukko.pub) stays
// pristine for the embedded-key proof — an explicit, mutually-exclusive
// compile-time embed selection, not a runtime fallback (§XV).
//
// keys/sukko.dev.pub and keys/sukko.dev.fingerprints are gitignored and produced by
// `go run ./genkeys`; the e2e build runs genkeys before compiling (taskfiles/e2e), so
// both exist at build time.
//
//go:embed keys/sukko.dev.pub
var embeddedPublicKeyBytes []byte

// embeddedFingerprintManifest under sukko_e2e is the dev manifest genkeys writes
// (keys/sukko.dev.fingerprints, "<dev-fp> local"), matching keys/sukko.dev.pub.
//
//go:embed keys/sukko.dev.fingerprints
var embeddedFingerprintManifest []byte
