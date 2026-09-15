package adminui

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

// loginAttempts counts admin UI login attempts by result label.
// Results: "success", "invalid_token", "missing_token".
var loginAttempts = promauto.NewCounterVec(prometheus.CounterOpts{
	Name: "provisioning_admin_ui_login_attempts_total",
	Help: "Total admin UI login attempts (provisioning service).",
}, []string{"result"})
