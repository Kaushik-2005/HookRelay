package main

import (
	"crypto/hmac"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/hex"
	"io"
	"log"
	"net/http"
	"os"
	"strings"
	"time"
)

func main() {
	secret := os.Getenv("HOOKRELAY_SECRET")
	if secret == "" {
		log.Fatal("HOOKRELAY_SECRET is required")
	}
	http.HandleFunc("/webhooks", func(w http.ResponseWriter, r *http.Request) {
		body, err := io.ReadAll(io.LimitReader(r.Body, 5<<20))
		if err != nil || !verify(secret, r.Header.Get("X-HookRelay-Timestamp"), body, r.Header.Get("X-HookRelay-Signature")) {
			http.Error(w, "invalid signature", http.StatusUnauthorized)
			return
		}
		log.Printf("verified event=%s body=%s", r.Header.Get("X-HookRelay-Event-ID"), body)
		w.WriteHeader(http.StatusOK)
	})
	log.Println("receiver listening on :9090")
	log.Fatal(http.ListenAndServe(":9090", nil))
}

func verify(secret, timestamp string, body []byte, header string) bool {
	parsed, err := time.Parse(time.RFC3339, timestamp)
	if err != nil || time.Since(parsed) > 5*time.Minute || time.Since(parsed) < -5*time.Minute {
		return false
	}
	mac := hmac.New(sha256.New, []byte(secret))
	_, _ = mac.Write([]byte(timestamp + "."))
	_, _ = mac.Write(body)
	expected := "sha256=" + hex.EncodeToString(mac.Sum(nil))
	return subtle.ConstantTimeCompare([]byte(expected), []byte(strings.TrimSpace(header))) == 1
}
