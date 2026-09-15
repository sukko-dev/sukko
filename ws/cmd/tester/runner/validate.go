package runner

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"time"

	"github.com/rs/zerolog"
	"github.com/sukko-dev/sukko/cmd/tester/auth"
	"github.com/sukko-dev/sukko/cmd/tester/metrics"
	"github.com/sukko-dev/sukko/cmd/tester/restpublish"
	testerws "github.com/sukko-dev/sukko/cmd/tester/ws"
)

// validatePublicChannel is the channel SUFFIX used by the channels validation
// suite — qualified with the run's tenant via tenantChannel() at use (the old
// value hardcoded a "sukko." tenant prefix that never matched throwaway
// tester tenants, so the subscribe was silently filtered out).
const validatePublicChannel = "validate.public"

// validPublishBody is a minimal well-formed publish payload used in Check 10 (API key REST publish).
// The response is 403 regardless of body content — API keys cannot REST-publish.
var validPublishBody = []byte(`{"channel":"general.test","data":{"x":1}}`)

func runValidate(ctx context.Context, run *TestRun, logger zerolog.Logger) (*metrics.Report, error) {
	suite := run.Config.Suite
	if suite == "" {
		suite = "auth"
	}

	// Channel authorization is provisioning-only: a ruleless tenant is denied
	// every subscribe/publish. Seed permissive rules for the suite tenant so
	// transport-focused suites aren't denied by deny-all; authorization-focused
	// suites overwrite these with their own precise rules afterwards.
	// Seeding failure surfaces as a failing check (the tester's error contract:
	// infra failures produce reports, not hard errors) and skips the suite —
	// running it under deny-all would only produce misleading failures.
	if err := seedDefaultChannelRules(ctx, run); err != nil {
		//nolint:nilerr // Intentional: the tester's error contract surfaces infra
		// failures as failing check results in the report, not as hard errors
		// (see TestRunValidate_* — suites must degrade into reports, not errors).
		return &metrics.Report{
			TestType: "validate:" + suite,
			Status:   metrics.ReportStatusFail,
			Metrics:  run.Collector.Snapshot(),
			Checks: []metrics.CheckResult{{
				Name:   "seed channel rules",
				Status: metrics.CheckStatusFail,
				Error:  err.Error(),
			}},
		}, nil
	}

	var checks []metrics.CheckResult
	var err error

	switch suite {
	case "auth":
		checks, err = validateAuth(ctx, run, logger)
	case "channels":
		checks, err = validateChannels(ctx, run, logger)
	case "ordering":
		checks, err = validateOrdering(ctx, run, logger)
	case "reconnect":
		checks, err = validateReconnect(ctx, run, logger)
	case "ratelimit":
		checks, err = validateRateLimit(ctx, run, logger)
	case "edition-limits":
		checks, err = validateEditionLimits(ctx, run, logger)
	case "pubsub":
		checks, err = validatePubSub(ctx, run, logger)
	case "tenant-isolation":
		checks, err = validateTenantIsolation(ctx, run, logger)
	case "provisioning":
		checks, err = validateProvisioning(ctx, run, logger)
	case "sse":
		checks, err = validateSSE(ctx, run, logger)
	case SuiteRestPublish:
		checks, err = validateRestPublish(ctx, run, logger)
	case "push":
		checks, err = validatePush(ctx, run, logger)
	case "license-reload":
		checks, err = validateLicenseReload(ctx, run, logger)
	case "token-revocation":
		checks, err = validateTokenRevocation(ctx, run, logger)
	case SuiteAPIKey:
		checks, err = validateAPIKey(ctx, run, logger)
	case SuiteUpgrade:
		checks, err = validateUpgrade(ctx, run, logger)
	case SuiteWebhooks:
		checks, err = validateWebhooks(ctx, run, logger, fetchCurrentEdition)
	case SuiteKafkaIngest:
		checks, err = validateKafkaIngest(ctx, run, logger)
	case SuiteHistory:
		checks, err = validateHistory(ctx, run, logger)
	case SuiteGapRecovery:
		checks, err = validateGapRecovery(ctx, run, logger)
	default:
		checks = []metrics.CheckResult{{
			Name:   "unknown suite",
			Status: metrics.CheckStatusFail,
			Error:  "unknown validation suite: " + suite,
		}}
	}

	if err != nil {
		return nil, err
	}

	status := metrics.ReportStatusPass
	for _, c := range checks {
		if c.Status == metrics.CheckStatusFail {
			status = metrics.ReportStatusFail
			break
		}
	}

	return &metrics.Report{
		TestType: "validate:" + suite,
		Status:   status,
		Metrics:  run.Collector.Snapshot(),
		Checks:   checks,
	}, nil
}

func validateAuth(ctx context.Context, run *TestRun, logger zerolog.Logger) ([]metrics.CheckResult, error) { //nolint:unparam // matches validate suite function signature
	var checks []metrics.CheckResult

	// Check 1: Valid JWT accepted (with retry for key registry cache race)
	start := time.Now()
	client, err := connectWithRetry(ctx, run.Config.GatewayURL, run.authResult.TokenFunc(0), logger)
	if err != nil {
		checks = append(checks, metrics.CheckResult{
			Name: "valid JWT accepted", Status: metrics.CheckStatusFail, Error: err.Error(),
		})
	} else {
		checks = append(checks, metrics.CheckResult{
			Name: "valid JWT accepted", Status: metrics.CheckStatusPass,
			Latency: time.Since(start).Round(time.Millisecond).String(),
		})
		_ = client.Close()
	}

	// Check 2: Expired JWT rejected
	expiredToken, err := run.authResult.Minter.MintExpired(1)
	if err != nil {
		checks = append(checks, metrics.CheckResult{
			Name: "expired JWT rejected", Status: metrics.CheckStatusFail, Error: fmt.Sprintf("mint expired: %v", err),
		})
	} else {
		client, err = testerws.Connect(ctx, testerws.ConnectConfig{
			GatewayURL: run.Config.GatewayURL,
			Token:      expiredToken,
			Logger:     logger,
		})
		if err != nil {
			checks = append(checks, metrics.CheckResult{
				Name: "expired JWT rejected", Status: metrics.CheckStatusPass,
			})
		} else {
			_ = client.Close()
			checks = append(checks, metrics.CheckResult{
				Name: "expired JWT rejected", Status: metrics.CheckStatusFail, Error: "expired JWT was accepted",
			})
		}
	}

	// Check 3: Wrong kid rejected
	wrongKidToken, err := run.authResult.Minter.MintWithKid(2, "nonexistent-key")
	if err != nil {
		checks = append(checks, metrics.CheckResult{
			Name: "wrong kid rejected", Status: metrics.CheckStatusFail, Error: fmt.Sprintf("mint wrong kid: %v", err),
		})
	} else {
		client, err = testerws.Connect(ctx, testerws.ConnectConfig{
			GatewayURL: run.Config.GatewayURL,
			Token:      wrongKidToken,
			Logger:     logger,
		})
		if err != nil {
			checks = append(checks, metrics.CheckResult{
				Name: "wrong kid rejected", Status: metrics.CheckStatusPass,
			})
		} else {
			_ = client.Close()
			checks = append(checks, metrics.CheckResult{
				Name: "wrong kid rejected", Status: metrics.CheckStatusFail, Error: "JWT with wrong kid was accepted",
			})
		}
	}

	// Check 4: Wrong tenant rejected
	wrongTenantToken, err := run.authResult.Minter.MintWithTenant(3, "wrong-tenant-id")
	if err != nil {
		checks = append(checks, metrics.CheckResult{
			Name: "wrong tenant rejected", Status: metrics.CheckStatusFail, Error: fmt.Sprintf("mint wrong tenant: %v", err),
		})
	} else {
		client, err = testerws.Connect(ctx, testerws.ConnectConfig{
			GatewayURL: run.Config.GatewayURL,
			Token:      wrongTenantToken,
			Logger:     logger,
		})
		if err != nil {
			checks = append(checks, metrics.CheckResult{
				Name: "wrong tenant rejected", Status: metrics.CheckStatusPass,
			})
		} else {
			_ = client.Close()
			checks = append(checks, metrics.CheckResult{
				Name: "wrong tenant rejected", Status: metrics.CheckStatusFail, Error: "JWT with wrong tenant was accepted",
			})
		}
	}

	// Check 5: Revoked key rejected (Scenario 3 AC-4)
	checks = append(checks, validateRevokedKey(ctx, run, logger)...)

	// Check 6: Missing token rejected
	client, err = testerws.Connect(ctx, testerws.ConnectConfig{
		GatewayURL: run.Config.GatewayURL,
		Token:      "",
		Logger:     logger,
	})
	if err != nil {
		checks = append(checks, metrics.CheckResult{
			Name: "missing token rejected", Status: metrics.CheckStatusPass,
		})
	} else {
		_ = client.Close()
		checks = append(checks, metrics.CheckResult{
			Name: "missing token rejected", Status: metrics.CheckStatusFail, Error: "connection with no token was accepted",
		})
	}

	// Check 7: Provisioning API tenant-scoped JWT (Scenario 2 AC-2)
	checks = append(checks, validateProvisioningJWT(ctx, run, logger)...)

	// Checks 9–10: API key authentication
	// Create a temporary API key for the test tenant; revoke it unconditionally at the end.
	apiKeyBody, apiKeyErr := run.authResult.ProvClient.CreateAPIKey(ctx, run.Config.TenantID, "tester-api-key-"+run.ID)
	var apiKeyValue string
	if apiKeyErr == nil && len(apiKeyBody) > 0 {
		var apiKeyResp struct {
			KeyID string `json:"key_id"`
		}
		if err := json.Unmarshal(apiKeyBody, &apiKeyResp); err == nil {
			apiKeyValue = apiKeyResp.KeyID
		}
	}

	// Check 9: Valid API key accepted (always emitted — fail if create/parse failed)
	if apiKeyErr != nil || apiKeyValue == "" {
		errMsg := "create api key failed"
		if apiKeyErr != nil {
			errMsg = fmt.Sprintf("create api key: %v", apiKeyErr)
		}
		// Check 10 also fails when key creation fails (all 11 slots always emitted)
		checks = append(checks,
			metrics.CheckResult{Name: "api key accepted", Status: metrics.CheckStatusFail, Error: errMsg},
			metrics.CheckResult{Name: "api key REST publish blocked", Status: metrics.CheckStatusFail, Error: "skipped: api key not available"},
		)
	} else {
		// Retry loop: handle key registry cache propagation (same race as JWT check 1)
		var apiKeyClient *testerws.Client
		var apiKeyConnErr error
	apiKeyConnLoop:
		for i, delay := range connectRetryBackoffs {
			if delay > 0 {
				select {
				case <-ctx.Done():
					apiKeyConnErr = fmt.Errorf("context canceled: %w", ctx.Err())
					break apiKeyConnLoop
				case <-time.After(delay):
				}
			}
			apiKeyClient, apiKeyConnErr = testerws.Connect(ctx, testerws.ConnectConfig{
				GatewayURL: run.Config.GatewayURL,
				APIKey:     apiKeyValue,
				Logger:     logger,
			})
			if apiKeyConnErr == nil {
				if i > 0 {
					logger.Info().Int("attempt", i+1).Msg("api key connected after retry")
				}
				break
			}
			logger.Debug().Err(apiKeyConnErr).Int("attempt", i+1).Msg("api key connection attempt failed")
		}
		if apiKeyConnErr != nil {
			checks = append(checks, metrics.CheckResult{
				Name: "api key accepted", Status: metrics.CheckStatusFail,
				Error: fmt.Sprintf("connect: %v", apiKeyConnErr),
			})
		} else {
			checks = append(checks, metrics.CheckResult{
				Name: "api key accepted", Status: metrics.CheckStatusPass,
			})
			_ = apiKeyClient.Close() // best-effort: test cleanup; disconnect errors are not actionable
		}

		// Check 10: API key cannot REST-publish (gateway returns 403 — publish requires JWT)
		if ctx.Err() != nil {
			checks = append(checks, metrics.CheckResult{
				Name: "api key REST publish blocked", Status: metrics.CheckStatusFail,
				Error: fmt.Sprintf("skipped: %v", ctx.Err()),
			})
		} else {
			restStatus, _, restErr := restpublish.NewClient(httpURL(run.Config.GatewayURL)).PublishRaw(
				ctx, validPublishBody, restpublish.AuthConfig{APIKey: apiKeyValue}, "application/json",
			)
			switch {
			case restErr != nil:
				checks = append(checks, metrics.CheckResult{
					Name: "api key REST publish blocked", Status: metrics.CheckStatusFail,
					Error: fmt.Sprintf("publish: %v", restErr),
				})
			case restStatus == http.StatusForbidden:
				checks = append(checks, metrics.CheckResult{
					Name: "api key REST publish blocked", Status: metrics.CheckStatusPass,
				})
			default:
				checks = append(checks, metrics.CheckResult{
					Name: "api key REST publish blocked", Status: metrics.CheckStatusFail,
					Error: fmt.Sprintf("expected 403, got %d", restStatus),
				})
			}
		}

		// Revoke the temporary API key regardless of check outcomes.
		// Use a fresh context — the test run ctx may be canceled at this point.
		// Failure is non-fatal: logged at Error, cleanup continues (§III, §IV).
		revokeCtx, revokeCancel := context.WithTimeout(context.Background(), editionHTTPTimeout)
		if err := run.authResult.ProvClient.RevokeAPIKey(revokeCtx, run.Config.TenantID, apiKeyValue); err != nil { //nolint:contextcheck // teardown: test ctx may be canceled; fresh context required
			logger.Error().Err(err).Str("key_id", apiKeyValue).Msg("validate: failed to revoke temporary api key")
		}
		revokeCancel() // cancel immediately after use, not deferred, to avoid leaking through remainder of validateAuth
	}

	// Check 11: Unauthenticated provisioning request rejected (expect 401)
	// Creates a client with nil provider → no Authorization header is sent.
	unauthClient := auth.NewProvisioningClient(run.Config.ProvisioningURL, nil, logger)
	unauthStatus, unauthErr := unauthClient.GetTenant(ctx, run.Config.TenantID, "")
	switch {
	case unauthErr != nil:
		checks = append(checks, metrics.CheckResult{
			Name: "unauthenticated request rejected", Status: metrics.CheckStatusFail,
			Error: fmt.Sprintf("get tenant: %v", unauthErr),
		})
	case unauthStatus == http.StatusUnauthorized:
		checks = append(checks, metrics.CheckResult{
			Name: "unauthenticated request rejected", Status: metrics.CheckStatusPass,
		})
	default:
		checks = append(checks, metrics.CheckResult{
			Name: "unauthenticated request rejected", Status: metrics.CheckStatusFail,
			Error: fmt.Sprintf("expected 401, got %d", unauthStatus),
		})
	}

	return checks, nil
}

// validateRevokedKey registers a second test key, revokes it, then verifies
// the gateway rejects a JWT minted with that kid.
func validateRevokedKey(ctx context.Context, run *TestRun, logger zerolog.Logger) []metrics.CheckResult {
	provClient := run.authResult.ProvClient
	tenantID := run.Config.TenantID
	revokedKeyID := "tester-revoke-" + run.ID

	// Generate a throwaway keypair for the revoked key test
	kp, err := auth.GenerateKeypair("revoke-" + run.ID)
	if err != nil {
		return []metrics.CheckResult{{
			Name: "revoked key rejected", Status: metrics.CheckStatusFail, Error: fmt.Sprintf("generate revoke keypair: %v", err),
		}}
	}

	// Register the key
	expiresAt := time.Now().Add(1 * time.Hour)
	if err := provClient.RegisterKey(ctx, tenantID, auth.RegisterKeyRequest{
		KeyID:     revokedKeyID,
		Algorithm: "ES256",
		PublicKey: kp.PublicPEM,
		ExpiresAt: &expiresAt,
	}); err != nil {
		return []metrics.CheckResult{{
			Name: "revoked key rejected", Status: metrics.CheckStatusFail, Error: fmt.Sprintf("register revoke key: %v", err),
		}}
	}

	// Revoke it
	if err := provClient.RevokeKey(ctx, tenantID, revokedKeyID); err != nil {
		return []metrics.CheckResult{{
			Name: "revoked key rejected", Status: metrics.CheckStatusFail, Error: fmt.Sprintf("revoke key: %v", err),
		}}
	}

	// Mint JWT with the revoked kid (signed with original test key — doesn't matter,
	// gateway checks revocation status before signature verification)
	revokedToken, err := run.authResult.Minter.MintWithKid(99, revokedKeyID)
	if err != nil {
		return []metrics.CheckResult{{
			Name: "revoked key rejected", Status: metrics.CheckStatusFail, Error: fmt.Sprintf("mint with revoked kid: %v", err),
		}}
	}

	client, err := testerws.Connect(ctx, testerws.ConnectConfig{
		GatewayURL: run.Config.GatewayURL,
		Token:      revokedToken,
		Logger:     logger,
	})
	if err != nil {
		return []metrics.CheckResult{{
			Name: "revoked key rejected", Status: metrics.CheckStatusPass,
		}}
	}
	_ = client.Close()
	return []metrics.CheckResult{{
		Name: "revoked key rejected", Status: metrics.CheckStatusFail, Error: "JWT with revoked kid was accepted",
	}}
}

// validateProvisioningJWT verifies tenant-scoped JWT access on the provisioning API.
func validateProvisioningJWT(ctx context.Context, run *TestRun, logger zerolog.Logger) []metrics.CheckResult {
	var checks []metrics.CheckResult
	provClient := run.authResult.ProvClient
	tenantJWT := run.authResult.TokenFunc(0)

	// GET own tenant with tenant JWT → expect 200
	status, err := provClient.GetTenant(ctx, run.Config.TenantID, tenantJWT)
	switch {
	case err != nil:
		checks = append(checks, metrics.CheckResult{
			Name: "provisioning JWT own tenant", Status: metrics.CheckStatusFail, Error: err.Error(),
		})
	case status == http.StatusOK:
		checks = append(checks, metrics.CheckResult{
			Name: "provisioning JWT own tenant", Status: metrics.CheckStatusPass,
		})
	default:
		checks = append(checks, metrics.CheckResult{
			Name: "provisioning JWT own tenant", Status: metrics.CheckStatusFail,
			Error: fmt.Sprintf("expected 200, got %d", status),
		})
	}

	// Cross-tenant isolation: provision a real second tenant (admin), then GET it
	// with tenant A's JWT → expect 403 (RequireTenant tenant-mismatch). A genuine
	// existing tenant is required: a nonexistent slug would 404 (not found) before
	// the isolation path and would not prove isolation.
	otherTenant := run.Config.TenantID + "-xt"
	if createErr := provClient.CreateTenant(ctx, otherTenant, "cross-tenant isolation probe"); createErr != nil {
		checks = append(checks, metrics.CheckResult{
			Name: "provisioning JWT cross-tenant blocked", Status: metrics.CheckStatusFail,
			Error: fmt.Sprintf("setup second tenant: %v", createErr),
		})
	} else {
		status, err = provClient.GetTenant(ctx, otherTenant, tenantJWT)
		switch {
		case err != nil:
			checks = append(checks, metrics.CheckResult{
				Name: "provisioning JWT cross-tenant blocked", Status: metrics.CheckStatusFail, Error: err.Error(),
			})
		case status == http.StatusForbidden:
			checks = append(checks, metrics.CheckResult{
				Name: "provisioning JWT cross-tenant blocked", Status: metrics.CheckStatusPass,
			})
		default:
			checks = append(checks, metrics.CheckResult{
				Name: "provisioning JWT cross-tenant blocked", Status: metrics.CheckStatusFail,
				Error: fmt.Sprintf("expected 403, got %d", status),
			})
		}

		// Best-effort teardown of the probe tenant. Fresh context — the test run
		// ctx may be canceled; failure is non-fatal, cleanup continues (§III, §IV).
		delCtx, delCancel := context.WithTimeout(context.Background(), 10*time.Second)
		_ = provClient.DeleteTenant(delCtx, otherTenant) //nolint:contextcheck // teardown: test ctx may be canceled; fresh context required
		delCancel()
	}

	logger.Debug().Msg("provisioning JWT validation complete")
	return checks
}

func validateChannels(ctx context.Context, run *TestRun, logger zerolog.Logger) ([]metrics.CheckResult, error) {
	var checks []metrics.CheckResult

	client, err := connectWithRetry(ctx, run.Config.GatewayURL, run.authResult.TokenFunc(0), logger)
	if err != nil {
		return []metrics.CheckResult{{
			Name: "connect", Status: metrics.CheckStatusFail, Error: err.Error(),
		}}, nil
	}
	defer client.Close() //nolint:errcheck // best-effort: test cleanup

	// Test public channel subscribe
	if err := client.Subscribe([]string{tenantChannel(run.authResult.TenantID, validatePublicChannel)}); err != nil {
		checks = append(checks, metrics.CheckResult{
			Name: "public channel subscribe", Status: metrics.CheckStatusFail, Error: err.Error(),
		})
	} else {
		checks = append(checks, metrics.CheckResult{
			Name: "public channel subscribe", Status: metrics.CheckStatusPass,
		})
	}

	return checks, nil
}
