package webhooks

import "testing"

func TestValidateURL(t *testing.T) {
	valid := []string{
		"https://example.com/webhooks",
		"http://localhost:8080/hook",
	}
	for _, value := range valid {
		if err := validateURL(value); err != nil {
			t.Errorf("validateURL(%q) returned error: %v", value, err)
		}
	}

	invalid := []string{
		"",
		"ftp://example.com/hook",
		"https:///missing-host",
		"example.com/hook",
	}
	for _, value := range invalid {
		if err := validateURL(value); err == nil {
			t.Errorf("validateURL(%q) accepted invalid URL", value)
		}
	}
}

func TestGenerateSecret(t *testing.T) {
	first, err := generateSecret()
	if err != nil {
		t.Fatalf("generateSecret() error: %v", err)
	}
	second, err := generateSecret()
	if err != nil {
		t.Fatalf("generateSecret() error: %v", err)
	}
	if len(first) == 0 || first == second {
		t.Fatalf("generated secrets should be non-empty and unique")
	}
}
