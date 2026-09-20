package alerts

import "testing"

func TestDestinationURLRejectsStaticSSRFAndUnsafeAuthority(t *testing.T) {
	for _, raw := range []string{"http://example.com", "https://user:pass@example.com", "https://example.com:8443", "https://127.0.0.1/hook", "https://[::1]/hook", "https://metadata.google.internal/hook", "https://example.com/hook#secret"} { // pragma: allowlist secret
		if err := ValidateDestinationURL(raw); err == nil {
			t.Fatalf("accepted %q", raw)
		}
	}
	if err := ValidateDestinationURL("https://example.com/hook?tenant=1"); err != nil {
		t.Fatal(err)
	}
}
