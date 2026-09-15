package license

import "testing"

func TestEdition_String(t *testing.T) {
	t.Parallel()
	tests := []struct {
		edition Edition
		want    string
	}{
		{Community, "community"},
		{Pro, "pro"},
		{Enterprise, "enterprise"},
		{"", "community"}, // zero value
	}
	for _, tt := range tests {
		if got := tt.edition.String(); got != tt.want {
			t.Errorf("Edition(%q).String() = %q, want %q", tt.edition, got, tt.want)
		}
	}
}

func TestEdition_IsAtLeast(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name    string
		edition Edition
		other   Edition
		want    bool
	}{
		// Same edition
		{"community >= community", Community, Community, true},
		{"pro >= pro", Pro, Pro, true},
		{"enterprise >= enterprise", Enterprise, Enterprise, true},

		// Higher editions
		{"pro >= community", Pro, Community, true},
		{"enterprise >= community", Enterprise, Community, true},
		{"enterprise >= pro", Enterprise, Pro, true},

		// Lower editions
		{"community >= pro", Community, Pro, false},
		{"community >= enterprise", Community, Enterprise, false},
		{"pro >= enterprise", Pro, Enterprise, false},

		// Zero value treated as community
		{"empty >= community", "", Community, true},
		{"community >= empty", Community, "", true},
		{"empty >= pro", "", Pro, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			if got := tt.edition.IsAtLeast(tt.other); got != tt.want {
				t.Errorf("Edition(%q).IsAtLeast(%q) = %v, want %v", tt.edition, tt.other, got, tt.want)
			}
		})
	}
}

func TestEdition_ZeroValue(t *testing.T) {
	t.Parallel()
	var e Edition
	if e.String() != "community" {
		t.Errorf("zero Edition.String() = %q, want %q", e.String(), "community")
	}
	if !e.IsAtLeast(Community) {
		t.Error("zero Edition.IsAtLeast(Community) should be true")
	}
}
