package alerts

import (
	"bytes"
	"context"
	"crypto/hmac"
	cryptorand "crypto/rand"
	"crypto/sha256"
	"crypto/tls"
	"encoding/hex"
	"errors"
	"io"
	"math/big"
	"net"
	"net/http"
	"net/netip"
	"net/url"
	"strconv"
	"strings"
	"time"
)

const (
	DeliveryTimeout       = 10 * time.Second
	DeliveryLease         = 60 * time.Second
	DeliveryHeartbeat     = 15 * time.Second
	DeliveryResponseLimit = 64 << 10
)

var errDestinationDNS = errors.New("destination DNS failed")

type DNSResolver interface {
	LookupNetIP(context.Context, string, string) ([]netip.Addr, error)
}

type resolvedDestination struct {
	URL      *url.URL
	Hostname string
	Address  netip.Addr
	Port     string
}

type Sender struct {
	Resolver              DNSResolver
	Dialer                *net.Dialer
	Now                   func() time.Time
	AllowLoopbackForTests bool
}

type SendRequest struct {
	DeliveryID string
	URL        string
	Body       []byte
	Secret     []byte
}

type SendOutcome struct {
	Status     int
	Retry      bool
	RetryAfter time.Duration
	ErrorCode  string
}

func (sender Sender) Send(ctx context.Context, request SendRequest) SendOutcome {
	if request.DeliveryID == "" || len(request.Body) < 2 || len(request.Body) > 64<<10 {
		return SendOutcome{ErrorCode: "invalid_delivery"}
	}
	timeoutContext, cancel := context.WithTimeout(ctx, DeliveryTimeout)
	defer cancel()
	resolved, err := sender.resolve(timeoutContext, request.URL)
	if err != nil {
		return SendOutcome{Retry: errors.Is(err, errDestinationDNS), ErrorCode: "destination_unavailable"}
	}
	now := time.Now().UTC()
	if sender.Now != nil {
		now = sender.Now().UTC()
	}
	timestamp := strconv.FormatInt(now.Unix(), 10)
	httpRequest, err := http.NewRequestWithContext(timeoutContext, http.MethodPost, resolved.URL.String(), bytes.NewReader(request.Body))
	if err != nil {
		return SendOutcome{ErrorCode: "invalid_destination"}
	}
	httpRequest.Header.Set("Content-Type", "application/json")
	httpRequest.Header.Set("X-Eventglass-Delivery", request.DeliveryID)
	httpRequest.Header.Set("X-Eventglass-Timestamp", timestamp)
	if len(request.Secret) > 0 {
		mac := hmac.New(sha256.New, request.Secret)
		_, _ = mac.Write([]byte(timestamp))
		_, _ = mac.Write([]byte("."))
		_, _ = mac.Write(request.Body)
		httpRequest.Header.Set("X-Eventglass-Signature", "v1="+hex.EncodeToString(mac.Sum(nil)))
	}
	dialer := sender.Dialer
	if dialer == nil {
		dialer = &net.Dialer{Timeout: DeliveryTimeout, KeepAlive: -1}
	}
	transport := &http.Transport{
		Proxy:               nil,
		DisableKeepAlives:   true,
		ForceAttemptHTTP2:   false,
		TLSHandshakeTimeout: DeliveryTimeout,
		TLSClientConfig:     &tls.Config{MinVersion: tls.VersionTLS12, ServerName: resolved.Hostname},
		DialContext: func(dialContext context.Context, _, _ string) (net.Conn, error) {
			return dialer.DialContext(dialContext, "tcp", net.JoinHostPort(resolved.Address.String(), resolved.Port))
		},
	}
	defer transport.CloseIdleConnections()
	client := &http.Client{Transport: transport, Timeout: DeliveryTimeout, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}
	response, err := client.Do(httpRequest)
	if err != nil {
		return SendOutcome{Retry: true, ErrorCode: "network_error"}
	}
	defer response.Body.Close()
	read, readErr := io.CopyN(io.Discard, response.Body, DeliveryResponseLimit+1)
	if readErr != nil && !errors.Is(readErr, io.EOF) {
		return SendOutcome{Status: response.StatusCode, Retry: true, ErrorCode: "response_read_failed"}
	}
	if read > DeliveryResponseLimit {
		return SendOutcome{Status: response.StatusCode, ErrorCode: "response_too_large"}
	}
	if response.StatusCode >= 200 && response.StatusCode <= 299 {
		return SendOutcome{Status: response.StatusCode}
	}
	retry := response.StatusCode == http.StatusRequestTimeout || response.StatusCode == http.StatusTooManyRequests || response.StatusCode >= 500
	code := "http_permanent"
	if retry {
		code = "http_retryable"
	}
	return SendOutcome{Status: response.StatusCode, Retry: retry, RetryAfter: parseRetryAfter(response.Header.Get("Retry-After"), now), ErrorCode: code}
}

func (sender Sender) resolve(ctx context.Context, raw string) (resolvedDestination, error) {
	parsed, err := url.Parse(raw)
	if err != nil || parsed.Hostname() == "" || parsed.User != nil || parsed.Fragment != "" {
		return resolvedDestination{}, errors.New("invalid destination")
	}
	hostname := strings.ToLower(strings.TrimSuffix(parsed.Hostname(), "."))
	if metadataHostname(hostname) {
		return resolvedDestination{}, errors.New("metadata destination is forbidden")
	}
	port := parsed.Port()
	if sender.AllowLoopbackForTests {
		if parsed.Scheme != "http" && parsed.Scheme != "https" {
			return resolvedDestination{}, errors.New("invalid test destination scheme")
		}
		if port == "" {
			if parsed.Scheme == "https" {
				port = "443"
			} else {
				port = "80"
			}
		}
	} else {
		if parsed.Scheme != "https" || port != "" && port != "443" {
			return resolvedDestination{}, errors.New("destination must use HTTPS port 443")
		}
		port = "443"
	}
	var addresses []netip.Addr
	if literal, parseErr := netip.ParseAddr(hostname); parseErr == nil {
		addresses = []netip.Addr{literal}
	} else {
		resolver := sender.Resolver
		if resolver == nil {
			resolver = net.DefaultResolver
		}
		addresses, err = resolver.LookupNetIP(ctx, "ip", hostname)
		if err != nil || len(addresses) == 0 {
			return resolvedDestination{}, errDestinationDNS
		}
	}
	for _, address := range addresses {
		address = address.Unmap()
		if !publicDestinationAddress(address) && !(sender.AllowLoopbackForTests && address.IsLoopback()) {
			return resolvedDestination{}, errors.New("destination DNS contains a forbidden address")
		}
		if sender.AllowLoopbackForTests && parsed.Scheme == "http" && !address.IsLoopback() {
			return resolvedDestination{}, errors.New("test HTTP destination must be loopback")
		}
	}
	return resolvedDestination{URL: parsed, Hostname: hostname, Address: addresses[0].Unmap(), Port: port}, nil
}

var forbiddenPublicPrefixes = []netip.Prefix{
	netip.MustParsePrefix("0.0.0.0/8"), netip.MustParsePrefix("100.64.0.0/10"), netip.MustParsePrefix("192.0.0.0/24"),
	netip.MustParsePrefix("192.0.2.0/24"), netip.MustParsePrefix("192.88.99.0/24"), netip.MustParsePrefix("198.18.0.0/15"), netip.MustParsePrefix("198.51.100.0/24"),
	netip.MustParsePrefix("203.0.113.0/24"), netip.MustParsePrefix("224.0.0.0/4"), netip.MustParsePrefix("240.0.0.0/4"),
	netip.MustParsePrefix("64:ff9b:1::/48"), netip.MustParsePrefix("100::/64"), netip.MustParsePrefix("2001::/23"),
	netip.MustParsePrefix("2001:db8::/32"), netip.MustParsePrefix("2002::/16"), netip.MustParsePrefix("3fff::/20"), netip.MustParsePrefix("5f00::/16"),
}

func publicDestinationAddress(address netip.Addr) bool {
	address = address.Unmap()
	if !address.IsValid() || !address.IsGlobalUnicast() || address.IsPrivate() || address.IsLoopback() || address.IsLinkLocalUnicast() || address.IsMulticast() || address.IsUnspecified() || address.Zone() != "" {
		return false
	}
	for _, prefix := range forbiddenPublicPrefixes {
		if prefix.Contains(address) {
			return false
		}
	}
	return true
}

func metadataHostname(host string) bool {
	return host == "metadata.google.internal" || host == "metadata.azure.internal" || host == "instance-data.ec2.internal" || strings.HasSuffix(host, ".localhost") || host == "localhost"
}

func parseRetryAfter(raw string, now time.Time) time.Duration {
	if seconds, err := strconv.ParseInt(strings.TrimSpace(raw), 10, 64); err == nil {
		return min(max(time.Duration(seconds)*time.Second, 0), time.Hour)
	}
	if parsed, err := http.ParseTime(raw); err == nil {
		return min(max(parsed.Sub(now), 0), time.Hour)
	}
	return 0
}

func RetryDelay(attempt int, retryAfter time.Duration) (time.Duration, error) {
	if attempt < 1 || attempt > 12 {
		return 0, errors.New("invalid delivery attempt")
	}
	ceiling := 5 * time.Second * time.Duration(1<<min(attempt-1, 10))
	ceiling = min(ceiling, time.Hour)
	floor := min(max(retryAfter, 0), time.Hour)
	ceiling = max(ceiling, floor)
	value, err := cryptorand.Int(cryptorand.Reader, big.NewInt(int64(ceiling-floor)+1))
	if err != nil {
		return 0, err
	}
	return floor + time.Duration(value.Int64()), nil
}
