package events

import (
	"encoding/json"
	"testing"
)

func TestRequestFingerprint(t *testing.T) {
	payload := json.RawMessage(`{"payment_id":"pay_123","amount":999}`)
	first := requestFingerprint("payment.succeeded", payload)
	if first == "" {
		t.Fatal("request fingerprint should not be empty")
	}
	if first != requestFingerprint("payment.succeeded", payload) {
		t.Fatal("same request should produce the same fingerprint")
	}
	if first == requestFingerprint("payment.failed", payload) {
		t.Fatal("different event types should produce different fingerprints")
	}
	if first == requestFingerprint("payment.succeeded", json.RawMessage(`{"payment_id":"pay_456","amount":999}`)) {
		t.Fatal("different payloads should produce different fingerprints")
	}
}
