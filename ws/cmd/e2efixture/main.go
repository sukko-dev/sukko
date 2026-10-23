// Command e2efixture provisions a throwaway, fully-usable tenant on an
// already-booted e2e stack and prints the SUKKO_IT_* environment the language-SDK
// live suites read. It is the §XIV-sanctioned bridge for driving an external SDK
// runner inside the source-built grid (ADR-0040 Tier2): the grid's `cell:sdk-*`
// tasks boot a stack with e2e_boot_cell, then eval this helper's output to point
// the SDK suite at it — rather than a taskfile making raw /api/v1/* calls.
//
// It deliberately reuses the tester's own provisioning code (cmd/tester/auth), so
// the registered-ES256-key requirement, admin-key auth, channel rules and kafka
// routing are provisioned exactly as the in-process tester does them — no second,
// drifting implementation.
//
// Output (stdout, nothing else) is a block of `export` lines meant to be eval'd:
//
//	eval "$(go run ./cmd/e2efixture --admin-key "$KEY" --edition pro)"
//
// All diagnostics go to stderr so stdout stays a clean, evaluable fixture. The
// throwaway tenant is intentionally NOT cleaned up here — it must outlive this
// short-lived process for the suite that follows; the cell's `compose down -v`
// teardown wipes the provisioning DB.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"os"
	"time"

	"github.com/rs/zerolog"

	"github.com/sukko-dev/sukko/cmd/tester/auth"
	provauth "github.com/sukko-dev/sukko/internal/provisioning/auth"
	"github.com/sukko-dev/sukko/internal/shared/routing"
)

// jwtLifetime bounds the minted client JWT. Kept at the §IX replay ceiling (≤15m);
// a live suite runs in well under a minute, and reconnects reuse the same token.
const jwtLifetime = 15 * time.Minute

// fixtureKeyName labels the registered signing key and api-key for this fixture.
const fixtureKeyName = "e2e-fixture"

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, "e2efixture:", err)
		os.Exit(1)
	}
}

func run() error {
	provURL := flag.String("provisioning-url", "http://localhost:18080", "base URL of the booted provisioning API")
	wsURL := flag.String("ws-url", "ws://localhost:13000/ws", "gateway WebSocket URL the SDK suite connects to")
	adminKeyPath := flag.String("admin-key", "", "path to the raw 64-byte Ed25519 admin private key (from gen-admin-key)")
	edition := flag.String("edition", "community", "the stack's license edition, exported as SUKKO_IT_EDITION so edition-gated cells (de)activate")
	channelBase := flag.String("channel-base", "general.smoke", "channel suffix (under the tenant) the suite targets; must match the provisioned channel rules")
	testID := flag.String("test-id", fmt.Sprintf("sdkgrid-%d", time.Now().UnixNano()), "unique id for the throwaway tenant + key")
	format := flag.String("format", "shell", "output format: 'shell' (export SUKKO_IT_* lines, for go/py suites) or 'json' (a sdk-js .fixture.json object)")
	flag.Parse()

	if *adminKeyPath == "" {
		return errors.New("--admin-key is required (the gen-admin-key output path mounted for this cell)")
	}
	if *format != "shell" && *format != "json" {
		return fmt.Errorf("--format must be 'shell' or 'json', got %q", *format)
	}

	// Diagnostics to stderr only — stdout must stay a clean evaluable fixture.
	logger := zerolog.New(os.Stderr).With().Timestamp().Str("component", "e2efixture").Logger()

	adminKey, err := auth.LoadEd25519PrivateKey(*adminKeyPath)
	if err != nil {
		return fmt.Errorf("loading admin key: %w", err)
	}
	// The kid must match the bootstrap key registered with provisioning at boot.
	adminProvider := auth.NewKeypairAuthProvider(adminKey, provauth.BootstrapAdminKeyID, auth.AdminKeyName)

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	// Create the throwaway tenant, register an ES256 key, and mint a client JWT —
	// exactly the tester's path (so the gateway accepts the token).
	setup, err := auth.Setup(ctx, auth.SetupConfig{
		TestID:               *testID,
		ProvisioningURL:      *provURL,
		AdminProvider:        adminProvider,
		RequireAdminProvider: true, // never silently fall back to an ephemeral key
		JWTLifetime:          jwtLifetime,
		Logger:               logger,
	})
	if err != nil {
		return fmt.Errorf("provisioning tenant + signing key: %w", err)
	}

	// Open channel rules for the suite's channel family (subscribe + publish), and a
	// catch-all routing rule so a publish lands on the default kafka topic. These
	// mirror the tester's pub-sub fixture; a missing routing rule silently drops every
	// publish on the kafka backend, so this fails loud.
	channelRules := map[string]any{
		"public":          []string{"general.*"},
		"publish_public":  []string{"general.*"},
		"default":         []string{"general.*"},
		"publish_default": []string{"general.*"},
	}
	if err := setup.ProvClient.SetChannelRules(ctx, setup.TenantID, channelRules); err != nil {
		return fmt.Errorf("setting channel rules: %w", err)
	}
	routingRules := []map[string]any{
		{"pattern": "**", "ingress_topic": routing.DefaultTopicSuffix, "priority": routing.DefaultCatchAllPriority},
	}
	if err := setup.ProvClient.SetRoutingRules(ctx, setup.TenantID, routingRules); err != nil {
		return fmt.Errorf("setting routing rules: %w", err)
	}

	apiKeyBody, err := setup.ProvClient.CreateAPIKey(ctx, setup.TenantID, fixtureKeyName)
	if err != nil {
		return fmt.Errorf("creating api key: %w", err)
	}
	var apiKey struct {
		KeyID string `json:"key_id"`
	}
	if err := json.Unmarshal(apiKeyBody, &apiKey); err != nil {
		return fmt.Errorf("decoding api key response: %w", err)
	}
	if apiKey.KeyID == "" {
		return fmt.Errorf("api key response had no key_id: %s", apiKeyBody)
	}

	token := setup.TokenFunc(0)
	if token == "" {
		return errors.New("minted an empty client token")
	}
	channel := setup.TenantID + "." + *channelBase

	// stdout is the fixture and nothing else — this is program data output, not logging
	// (§V/forbidigo targets fmt.Print* logging), so build the block and write it directly.
	var block string
	if *format == "json" {
		// The sdk-js e2e harness reads a .fixture.json object (not env). composeProject is
		// omitted — the SDK-grid cells do not bounce a service, so the SSE/round-trip tests
		// never read it.
		out, marshalErr := json.Marshal(map[string]string{
			"wsUrl":   *wsURL,
			"token":   token,
			"apiKey":  apiKey.KeyID,
			"channel": channel,
			"edition": *edition,
		})
		if marshalErr != nil {
			return fmt.Errorf("marshaling json fixture: %w", marshalErr)
		}
		block = string(out) + "\n"
	} else {
		block = fmt.Sprintf(
			"export SUKKO_IT_WS_URL='%s'\n"+
				"export SUKKO_IT_TOKEN='%s'\n"+
				"export SUKKO_IT_API_KEY='%s'\n"+
				"export SUKKO_IT_CHANNEL='%s'\n"+
				"export SUKKO_IT_EDITION='%s'\n",
			*wsURL, token, apiKey.KeyID, channel, *edition)
	}
	if _, err := os.Stdout.WriteString(block); err != nil {
		return fmt.Errorf("writing fixture: %w", err)
	}
	return nil
}
