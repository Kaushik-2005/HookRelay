package security

import (
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

func TestValidateURLRejectsLocalTargetsByDefault(t *testing.T) {
	for _, value := range []string{
		"http://localhost/hook",
		"http://127.0.0.1/hook",
		"http://10.0.0.5/hook",
		"http://[::1]/hook",
		"http://169.254.169.254/latest/meta-data",
	} {
		if err := ValidateURL(value, false); err == nil {
			t.Errorf("ValidateURL(%q) accepted a private target", value)
		}
	}
}

func TestClientDoesNotFollowRedirects(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, "/next", http.StatusFound)
	}))
	defer server.Close()
	response, err := NewClient(time.Second, true).Get(server.URL)
	if err != nil {
		t.Fatalf("request failed: %v", err)
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusFound {
		t.Fatalf("expected redirect response, got %d", response.StatusCode)
	}
}
