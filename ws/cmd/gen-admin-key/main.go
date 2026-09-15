// Command gen-admin-key generates an Ed25519 admin keypair for the kafka-ingest E2E
// harness. It writes the raw 64-byte private key (seed||public — the format the tester's
// TESTER_ADMIN_KEY_FILE expects) to the path in os.Args[1], and prints the base64-encoded
// 32-byte public key (the format provisioning's ADMIN_BOOTSTRAP_KEY expects) to stdout.
//
// This is a bridge tool for taskfiles/e2e.yml. It will be removed once the devstack
// command (issue #144) owns wired-stack setup by reusing the shared auth packages directly.
package main

import (
	"crypto/ed25519"
	"crypto/rand"
	"encoding/base64"
	"fmt"
	"os"
)

func main() {
	if len(os.Args) != 2 {
		fmt.Fprintln(os.Stderr, "usage: gen-admin-key <private-key-out-path>")
		os.Exit(2)
	}
	privPath := os.Args[1]

	pub, priv, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		fmt.Fprintf(os.Stderr, "generate keypair: %v\n", err)
		os.Exit(1)
	}

	// 0644 so the tester container (which may run as a different uid than the host) can
	// read the bind-mounted key. This is a disposable, gitignored E2E credential regenerated
	// every run — never a production key.
	if err := os.WriteFile(privPath, priv, 0o644); err != nil { //nolint:gosec // G306: disposable E2E test key, must be container-readable across uids
		fmt.Fprintf(os.Stderr, "write private key %q: %v\n", privPath, err)
		os.Exit(1)
	}

	// Program OUTPUT, not logging: the harness captures stdout into
	// ADMIN_BOOTSTRAP_KEY. Written via os.Stdout directly (Constitution V
	// governs logging; this is the tool's data output).
	if _, err := fmt.Fprintln(os.Stdout, base64.StdEncoding.EncodeToString(pub)); err != nil {
		fmt.Fprintf(os.Stderr, "write public key to stdout: %v\n", err)
		os.Exit(1)
	}
}
