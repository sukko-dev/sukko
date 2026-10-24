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

// Output formats for --format. shell emits SUKKO_IT_* export lines (go/py suites); json emits
// a sdk-js .fixture.json object.
const (
	formatShell = "shell"
	formatJSON  = "json"
)

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
	format := flag.String("format", formatShell, "output format: 'shell' (export SUKKO_IT_* lines, for go/py suites) or 'json' (a sdk-js .fixture.json object)")
	composeProject := flag.String("compose-project", "", "compose project name of the booted stack; set only for the json format so sdk-js's reconnect test can bounce `<project>-ws-gateway-1` (empty = omit it, and that test skips)")
	flag.Parse()

	if *adminKeyPath == "" {
		return errors.New("--admin-key is required (the gen-admin-key output path mounted for this cell)")
	}
	if *format != formatShell && *format != formatJSON {
		return fmt.Errorf("--format must be %q or %q, got %q", formatShell, formatJSON, *format)
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
	block, err := renderFixture(*format, fixtureFields{
		wsURL:          *wsURL,
		token:          token,
		apiKey:         apiKey.KeyID,
		channel:        channel,
		edition:        *edition,
		composeProject: *composeProject,
	})
	if err != nil {
		return err
	}
	if _, err := os.Stdout.WriteString(block); err != nil {
		return fmt.Errorf("writing fixture: %w", err)
	}
	return nil
}

// fixtureFields are the resolved values a fixture block is built from.
type fixtureFields struct {
	wsURL, token, apiKey, channel, edition, composeProject string
}

// renderFixture builds the stdout block for the requested format. It is split out from run()
// so it can be tested without a live stack.
//
// composeProject rides only the json format, and only when set: sdk-js's reconnect test bounces
// the gateway *container* (`docker restart <project>-ws-gateway-1`), so it needs the project
// name — and skips when it is absent. The shell format (go/py suites) never carries it, because
// sdk-go's reconnect test forces a reconnect with an in-process TCP proxy, not a container bounce.
func renderFixture(format string, f fixtureFields) (string, error) {
	switch format {
	case formatJSON:
		obj := map[string]string{
			"wsUrl":   f.wsURL,
			"token":   f.token,
			"apiKey":  f.apiKey,
			"channel": f.channel,
			"edition": f.edition,
		}
		if f.composeProject != "" {
			obj["composeProject"] = f.composeProject
		}
		out, err := json.Marshal(obj)
		if err != nil {
			return "", fmt.Errorf("marshaling json fixture: %w", err)
		}
		return string(out) + "\n", nil
	case formatShell:
		return fmt.Sprintf(
			"export SUKKO_IT_WS_URL='%s'\n"+
				"export SUKKO_IT_TOKEN='%s'\n"+
				"export SUKKO_IT_API_KEY='%s'\n"+
				"export SUKKO_IT_CHANNEL='%s'\n"+
				"export SUKKO_IT_EDITION='%s'\n",
			f.wsURL, f.token, f.apiKey, f.channel, f.edition), nil
	default:
		return "", fmt.Errorf("unknown format %q (want 'shell' or 'json')", format)
	}
}
