package security

import (
	"context"
	"fmt"
	"net"
	"net/http"
	"net/netip"
	"net/url"
	"strconv"
	"strings"
	"time"
)

// ValidateURL validates the parts that can be checked without making a
// network request. IP addresses and local hostnames are rejected by default.
// DNS names are checked again immediately before each connection by the
// transport, which protects against DNS rebinding.
func ValidateURL(raw string, allowPrivate bool) error {
	parsed, err := url.ParseRequestURI(raw)
	if err != nil || parsed.Host == "" || (parsed.Scheme != "http" && parsed.Scheme != "https") {
		return fmt.Errorf("target_url must be a valid http or https URL")
	}
	if parsed.User != nil || parsed.Fragment != "" {
		return fmt.Errorf("target_url must not contain credentials or a fragment")
	}
	host := strings.ToLower(parsed.Hostname())
	if host == "" {
		return fmt.Errorf("target_url must include a hostname")
	}
	if !allowPrivate && (host == "localhost" || strings.HasSuffix(host, ".localhost") || strings.HasSuffix(host, ".local")) {
		return fmt.Errorf("target_url points to a private or local hostname")
	}
	if port := parsed.Port(); port != "" {
		if _, err := strconv.Atoi(port); err != nil {
			return fmt.Errorf("target_url contains an invalid port")
		}
	}
	if address, err := netip.ParseAddr(host); err == nil && !allowPrivate && isPrivate(address) {
		return fmt.Errorf("target_url points to a private or reserved IP address")
	}
	return nil
}

// NewClient disables redirects and resolves every hostname at dial time. It
// dials the validated address directly, so a DNS answer cannot be changed by
// a redirect or silently replaced between validation and connection setup.
func NewClient(timeout time.Duration, allowPrivate bool) *http.Client {
	resolver := net.DefaultResolver
	dialer := &net.Dialer{Timeout: timeout}
	transport := &http.Transport{
		// Do not allow an environment proxy to bypass the IP policy. The
		// destination must be reached directly and checked by this dialer.
		Proxy: nil,
		DialContext: func(ctx context.Context, network, address string) (net.Conn, error) {
			host, port, err := net.SplitHostPort(address)
			if err != nil {
				return nil, err
			}
			ips, err := resolver.LookupNetIP(ctx, "ip", host)
			if err != nil {
				return nil, err
			}
			for _, ip := range ips {
				if !allowPrivate && isPrivate(ip) {
					continue
				}
				return dialer.DialContext(ctx, network, net.JoinHostPort(ip.String(), port))
			}
			return nil, fmt.Errorf("all resolved addresses for %q are private or reserved", host)
		},
	}
	return &http.Client{
		Timeout:   timeout,
		Transport: transport,
		CheckRedirect: func(_ *http.Request, _ []*http.Request) error {
			return http.ErrUseLastResponse
		},
	}
}

func isPrivate(address netip.Addr) bool {
	if !address.IsValid() {
		return true
	}
	if address.IsPrivate() || address.IsLoopback() || address.IsLinkLocalUnicast() || address.IsUnspecified() || address.IsMulticast() {
		return true
	}
	if address.Is4() {
		value := address.As4()
		return value[0] == 100 && value[1] >= 64 && value[1] <= 127 ||
			value[0] == 192 && value[1] == 0 && value[2] == 0 ||
			value[0] == 198 && value[1] >= 18 && value[1] <= 19 ||
			value[0] >= 240
	}
	return false
}
