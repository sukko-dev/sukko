package main

import (
	"encoding/json"
	"strings"
	"testing"
)

// renderFixture is the pure output-building step: given the resolved fixture fields and a
// format, produce the exact block written to stdout. Testing it directly avoids needing a
// live stack (run() itself provisions a real tenant).

func TestRenderFixture_JSONIncludesComposeProjectWhenSet(t *testing.T) {
	t.Parallel()
	out, err := renderFixture("json", fixtureFields{
		wsURL:          "ws://localhost:13000/ws",
		token:          "tok",
		apiKey:         "key",
		channel:        "tenant.general.smoke",
		edition:        "pro",
		composeProject: "sukko-sdkjs-grid",
	})
	if err != nil {
		t.Fatalf("renderFixture: %v", err)
	}
	var m map[string]string
	if err := json.Unmarshal([]byte(out), &m); err != nil {
		t.Fatalf("output is not valid JSON: %v\n%s", err, out)
	}
	if got := m["composeProject"]; got != "sukko-sdkjs-grid" {
		t.Errorf("composeProject = %q, want %q", got, "sukko-sdkjs-grid")
	}
	// The other fields must still be present.
	for k, want := range map[string]string{
		"wsUrl": "ws://localhost:13000/ws", "token": "tok", "apiKey": "key",
		"channel": "tenant.general.smoke", "edition": "pro",
	} {
		if m[k] != want {
			t.Errorf("%s = %q, want %q", k, m[k], want)
		}
	}
}

func TestRenderFixture_JSONOmitsComposeProjectWhenEmpty(t *testing.T) {
	t.Parallel()
	out, err := renderFixture("json", fixtureFields{
		wsURL: "ws://x/ws", token: "t", apiKey: "k", channel: "c.general.smoke", edition: "community",
		composeProject: "",
	})
	if err != nil {
		t.Fatalf("renderFixture: %v", err)
	}
	var m map[string]string
	if err := json.Unmarshal([]byte(out), &m); err != nil {
		t.Fatalf("output is not valid JSON: %v\n%s", err, out)
	}
	if _, present := m["composeProject"]; present {
		t.Errorf("composeProject must be OMITTED (not empty) when unset, got output: %s", out)
	}
}

func TestRenderFixture_ShellFormatUnchanged(t *testing.T) {
	t.Parallel()
	// The shell format feeds the go/py suites via SUKKO_IT_* env; it never carries
	// composeProject (sdk-go's reconnect test uses a TCP proxy, not a container bounce), so a
	// set composeProject must not leak into it. Assert the exact byte-for-byte block.
	out, err := renderFixture("shell", fixtureFields{
		wsURL: "ws://localhost:13000/ws", token: "tok", apiKey: "key",
		channel: "tenant.general.smoke", edition: "pro", composeProject: "should-be-ignored",
	})
	if err != nil {
		t.Fatalf("renderFixture: %v", err)
	}
	want := "export SUKKO_IT_WS_URL='ws://localhost:13000/ws'\n" +
		"export SUKKO_IT_TOKEN='tok'\n" +
		"export SUKKO_IT_API_KEY='key'\n" +
		"export SUKKO_IT_CHANNEL='tenant.general.smoke'\n" +
		"export SUKKO_IT_EDITION='pro'\n"
	if out != want {
		t.Errorf("shell block mismatch:\n got: %q\nwant: %q", out, want)
	}
	if strings.Contains(out, "should-be-ignored") || strings.Contains(out, "composeProject") {
		t.Errorf("shell format must not carry composeProject, got: %s", out)
	}
}

func TestRenderFixture_UnknownFormatErrors(t *testing.T) {
	t.Parallel()
	if _, err := renderFixture("yaml", fixtureFields{}); err == nil {
		t.Fatal("expected an error for an unknown format")
	}
}
