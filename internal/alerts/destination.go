package alerts

import (
	"errors"
	"net"
	"net/url"
	"strings"
)

// ValidateDestinationURL performs the context-free half of destination
// validation. Delivery repeats this after DNS resolution and pins the allowed
// address; accepting configuration never makes a private literal reachable.
func ValidateDestinationURL(raw string) error {
	if len(raw) < 1 || len(raw) > 2048 {
		return errors.New("destination URL size is invalid")
	}
	parsed, err := url.Parse(raw)
	if err != nil || parsed.Scheme != "https" || parsed.Hostname() == "" || parsed.User != nil || parsed.Fragment != "" {
		return errors.New("destination must be an HTTPS URL without credentials or fragment")
	}
	if port := parsed.Port(); port != "" && port != "443" {
		return errors.New("destination port must be 443")
	}
	host := strings.ToLower(strings.TrimSuffix(parsed.Hostname(), "."))
	if host == "localhost" || host == "metadata.google.internal" || strings.HasSuffix(host, ".localhost") {
		return errors.New("private destination host is forbidden")
	}
	if ip := net.ParseIP(host); ip != nil && (!ip.IsGlobalUnicast() || ip.IsPrivate() || ip.IsLoopback() || ip.IsLinkLocalUnicast() || ip.IsLinkLocalMulticast() || ip.IsMulticast() || ip.IsUnspecified()) {
		return errors.New("private destination address is forbidden")
	}
	return nil
}
