package admin

import (
	"fmt"
	"strings"
	"time"

	commonv1 "github.com/Olian04/edgerun/gen/edgerun/common/v1"
)

func humanBytes(n int) string {
	switch {
	case n <= 0:
		return "—"
	case n < 1<<10:
		return fmt.Sprintf("%d B", n)
	case n < 1<<20:
		return fmt.Sprintf("%.1f KiB", float64(n)/(1<<10))
	case n < 1<<30:
		return fmt.Sprintf("%.1f MiB", float64(n)/(1<<20))
	}
	return fmt.Sprintf("%.2f GiB", float64(n)/(1<<30))
}

func humanMicros(us int) string {
	switch {
	case us <= 0:
		return "—"
	case us < 1000:
		return fmt.Sprintf("%d µs", us)
	case us < 1_000_000:
		return fmt.Sprintf("%.1f ms", float64(us)/1000)
	}
	return fmt.Sprintf("%.2f s", float64(us)/1e6)
}

func versionLabel(n int) string {
	if n <= 0 {
		return "—"
	}
	return fmt.Sprintf("v%d", n)
}

// limitLabel shows a per-version limit, where zero means "the worker's default".
func limitLabel(n int) string {
	if n <= 0 {
		return "default"
	}
	return humanBytes(n)
}

func when(t time.Time) string { return t.UTC().Format("2006-01-02 15:04:05") }

func shortID(id string) string {
	if len(id) > 8 {
		return id[:8]
	}
	return id
}

func shortDigest(d string) string {
	d = strings.TrimPrefix(d, "sha256:")
	return shortID(d)
}

// reasonLabel turns ERROR_REASON_FUNCTION_TRAPPED into "function trapped".
func reasonLabel(r commonv1.ErrorReason) string {
	if r == commonv1.ErrorReason_ERROR_REASON_UNSPECIFIED {
		return ""
	}
	return strings.ToLower(strings.ReplaceAll(strings.TrimPrefix(r.String(), "ERROR_REASON_"), "_", " "))
}

// resultClass groups an invocation's outcome for the badge: a platform failure
// outranks the status the function may also have sent.
func resultClass(status uint32, reason commonv1.ErrorReason) string {
	switch {
	case reason != commonv1.ErrorReason_ERROR_REASON_UNSPECIFIED:
		return "failed"
	case status == 0:
		return "no response"
	case status < 300:
		return "2xx"
	case status < 400:
		return "3xx"
	case status < 500:
		return "4xx"
	}
	return "5xx"
}
