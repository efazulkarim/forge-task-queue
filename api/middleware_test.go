package api

import (
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
	"time"
)

func testLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelError}))
}

// --- CORS middleware tests ---

func TestCORS_AllowedOrigin(t *testing.T) {
	origins := []string{"http://localhost:3000", "https://app.example.com"}
	handler := corsMiddleware(origins)(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("ok"))
	}))

	req := httptest.NewRequest(http.MethodGet, "/test", nil)
	req.Header.Set("Origin", "http://localhost:3000")
	rr := httptest.NewRecorder()

	handler.ServeHTTP(rr, req)

	if rr.Header().Get("Access-Control-Allow-Origin") != "http://localhost:3000" {
		t.Errorf("expected CORS origin http://localhost:3000, got %q", rr.Header().Get("Access-Control-Allow-Origin"))
	}
	if rr.Header().Get("Access-Control-Allow-Methods") == "" {
		t.Error("expected Access-Control-Allow-Methods header to be set")
	}
	if rr.Header().Get("Access-Control-Allow-Headers") == "" {
		t.Error("expected Access-Control-Allow-Headers header to be set")
	}
	if rr.Header().Get("Access-Control-Allow-Credentials") != "true" {
		t.Error("expected Access-Control-Allow-Credentials to be true")
	}
	if rr.Code != http.StatusOK {
		t.Errorf("expected status 200, got %d", rr.Code)
	}
}

func TestCORS_BlocksNonMatchingOrigin(t *testing.T) {
	origins := []string{"http://localhost:3000"}
	handler := corsMiddleware(origins)(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))

	req := httptest.NewRequest(http.MethodGet, "/test", nil)
	req.Header.Set("Origin", "https://evil.example.com")
	rr := httptest.NewRecorder()

	handler.ServeHTTP(rr, req)

	if rr.Header().Get("Access-Control-Allow-Origin") != "" {
		t.Errorf("expected no CORS origin header, got %q", rr.Header().Get("Access-Control-Allow-Origin"))
	}
}

func TestCORS_NoOriginHeader(t *testing.T) {
	origins := []string{"http://localhost:3000"}
	handler := corsMiddleware(origins)(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))

	req := httptest.NewRequest(http.MethodGet, "/test", nil)
	// No Origin header
	rr := httptest.NewRecorder()

	handler.ServeHTTP(rr, req)

	if rr.Header().Get("Access-Control-Allow-Origin") != "" {
		t.Errorf("expected no CORS origin header when Origin is empty, got %q", rr.Header().Get("Access-Control-Allow-Origin"))
	}
}

func TestCORS_WildcardOrigin(t *testing.T) {
	origins := []string{"*"}
	handler := corsMiddleware(origins)(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))

	req := httptest.NewRequest(http.MethodGet, "/test", nil)
	req.Header.Set("Origin", "https://any-origin.example.com")
	rr := httptest.NewRecorder()

	handler.ServeHTTP(rr, req)

	if rr.Header().Get("Access-Control-Allow-Origin") != "https://any-origin.example.com" {
		t.Errorf("expected wildcard to allow origin, got %q", rr.Header().Get("Access-Control-Allow-Origin"))
	}
}

func TestCORS_PreflightOPTIONS(t *testing.T) {
	origins := []string{"http://localhost:3000"}
	handler := corsMiddleware(origins)(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		t.Error("next handler should not be called for OPTIONS preflight")
	}))

	req := httptest.NewRequest(http.MethodOptions, "/test", nil)
	req.Header.Set("Origin", "http://localhost:3000")
	rr := httptest.NewRecorder()

	handler.ServeHTTP(rr, req)

	if rr.Code != http.StatusOK {
		t.Errorf("expected 200 for OPTIONS preflight, got %d", rr.Code)
	}
	if rr.Header().Get("Access-Control-Allow-Origin") != "http://localhost:3000" {
		t.Error("expected CORS headers on OPTIONS response")
	}
}

func TestCORS_SecondOriginMatch(t *testing.T) {
	origins := []string{"http://first.com", "http://second.com", "http://third.com"}
	handler := corsMiddleware(origins)(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))

	req := httptest.NewRequest(http.MethodGet, "/test", nil)
	req.Header.Set("Origin", "http://second.com")
	rr := httptest.NewRecorder()

	handler.ServeHTTP(rr, req)

	if rr.Header().Get("Access-Control-Allow-Origin") != "http://second.com" {
		t.Errorf("expected CORS origin http://second.com, got %q", rr.Header().Get("Access-Control-Allow-Origin"))
	}
}

// --- API key middleware tests ---

func TestAPIKey_RejectsMissingKey(t *testing.T) {
	handler := apiKeyMiddleware("my-secret")(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("protected"))
	}))

	req := httptest.NewRequest(http.MethodGet, "/protected", nil)
	rr := httptest.NewRecorder()

	handler.ServeHTTP(rr, req)

	if rr.Code != http.StatusUnauthorized {
		t.Errorf("expected 401, got %d", rr.Code)
	}
	if !strings.Contains(rr.Body.String(), "Invalid or missing API key") {
		t.Errorf("expected error message in body, got %q", rr.Body.String())
	}
}

func TestAPIKey_RejectsWrongKey(t *testing.T) {
	handler := apiKeyMiddleware("my-secret")(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))

	req := httptest.NewRequest(http.MethodGet, "/protected", nil)
	req.Header.Set("Authorization", "Bearer wrong-key")
	rr := httptest.NewRecorder()

	handler.ServeHTTP(rr, req)

	if rr.Code != http.StatusUnauthorized {
		t.Errorf("expected 401, got %d", rr.Code)
	}
}

func TestAPIKey_AcceptsValidKey(t *testing.T) {
	handler := apiKeyMiddleware("my-secret")(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("protected"))
	}))

	req := httptest.NewRequest(http.MethodGet, "/protected", nil)
	req.Header.Set("Authorization", "Bearer my-secret")
	rr := httptest.NewRecorder()

	handler.ServeHTTP(rr, req)

	if rr.Code != http.StatusOK {
		t.Errorf("expected 200, got %d", rr.Code)
	}
	if rr.Body.String() != "protected" {
		t.Errorf("expected body 'protected', got %q", rr.Body.String())
	}
}

func TestAPIKey_NoopWhenKeyIsEmpty(t *testing.T) {
	handler := apiKeyMiddleware("")(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("allowed"))
	}))

	// No Authorization header at all
	req := httptest.NewRequest(http.MethodGet, "/test", nil)
	rr := httptest.NewRecorder()

	handler.ServeHTTP(rr, req)

	if rr.Code != http.StatusOK {
		t.Errorf("expected 200 when API key is empty (auth disabled), got %d", rr.Code)
	}
	if rr.Body.String() != "allowed" {
		t.Errorf("expected body 'allowed', got %q", rr.Body.String())
	}
}

func TestAPIKey_MalformedBearerPrefix(t *testing.T) {
	handler := apiKeyMiddleware("my-secret")(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))

	req := httptest.NewRequest(http.MethodGet, "/protected", nil)
	req.Header.Set("Authorization", "Token my-secret") // Wrong prefix
	rr := httptest.NewRecorder()

	handler.ServeHTTP(rr, req)

	if rr.Code != http.StatusUnauthorized {
		t.Errorf("expected 401 for malformed Bearer prefix, got %d", rr.Code)
	}
}

func TestAPIKey_ContentTypeIsJSON(t *testing.T) {
	handler := apiKeyMiddleware("key")(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))

	req := httptest.NewRequest(http.MethodGet, "/test", nil)
	rr := httptest.NewRecorder()

	handler.ServeHTTP(rr, req)

	if rr.Header().Get("Content-Type") != "application/json" {
		t.Errorf("expected Content-Type application/json, got %q", rr.Header().Get("Content-Type"))
	}
}

// --- Panic recovery middleware tests ---

func TestPanicRecovery_CatchesPanic(t *testing.T) {
	logger := testLogger()
	handler := panicRecoveryMiddleware(logger)(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		panic("something went wrong")
	}))

	req := httptest.NewRequest(http.MethodGet, "/test", nil)
	rr := httptest.NewRecorder()

	// Should not panic
	handler.ServeHTTP(rr, req)

	if rr.Code != http.StatusInternalServerError {
		t.Errorf("expected 500 after panic, got %d", rr.Code)
	}
	if !strings.Contains(rr.Body.String(), "Internal server error") {
		t.Errorf("expected error message in body, got %q", rr.Body.String())
	}
}

func TestPanicRecovery_NoPanicPassesThrough(t *testing.T) {
	logger := testLogger()
	handler := panicRecoveryMiddleware(logger)(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("no panic"))
	}))

	req := httptest.NewRequest(http.MethodGet, "/test", nil)
	rr := httptest.NewRecorder()

	handler.ServeHTTP(rr, req)

	if rr.Code != http.StatusOK {
		t.Errorf("expected 200, got %d", rr.Code)
	}
	if rr.Body.String() != "no panic" {
		t.Errorf("expected 'no panic', got %q", rr.Body.String())
	}
}

func TestPanicRecovery_CatchesIntegerPanic(t *testing.T) {
	logger := testLogger()
	handler := panicRecoveryMiddleware(logger)(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		panic(42)
	}))

	req := httptest.NewRequest(http.MethodGet, "/test", nil)
	rr := httptest.NewRecorder()

	handler.ServeHTTP(rr, req)

	if rr.Code != http.StatusInternalServerError {
		t.Errorf("expected 500 after integer panic, got %d", rr.Code)
	}
}

// --- Rate limiter tests ---

func TestRateLimiter_AllowsFirstRequest(t *testing.T) {
	rl := newRateLimiter(10, 20)

	if !rl.allow("192.168.1.1") {
		t.Error("expected first request to be allowed")
	}
}

func TestRateLimiter_AllowsBurstRequests(t *testing.T) {
	rl := newRateLimiter(10, 5)

	// Should allow up to burst (5) requests
	for i := 0; i < 5; i++ {
		if !rl.allow("10.0.0.1") {
			t.Errorf("request %d should be allowed within burst", i+1)
		}
	}

	// 6th request should be rejected (no time for tokens to refill)
	if rl.allow("10.0.0.1") {
		t.Error("expected request to be rejected after burst exhausted")
	}
}

func TestRateLimiter_DifferentIPsIndependent(t *testing.T) {
	rl := newRateLimiter(10, 2)

	// Exhaust burst for IP A
	rl.allow("10.0.0.1")
	rl.allow("10.0.0.1")

	// IP B should still be allowed
	if !rl.allow("10.0.0.2") {
		t.Error("expected different IP to have independent rate limit")
	}

	// IP A should be blocked
	if rl.allow("10.0.0.1") {
		t.Error("expected IP A to be blocked after burst")
	}
}

func TestRateLimiter_TokensRefillOverTime(t *testing.T) {
	rl := newRateLimiter(1000, 2)

	// Exhaust burst
	rl.allow("10.0.0.1")
	rl.allow("10.0.0.1")

	// Manually set lastSeen to simulate time passing
	rl.mu.Lock()
	rl.visitors["10.0.0.1"].lastSeen = rl.visitors["10.0.0.1"].lastSeen.Add(-2 * time.Second)
	rl.mu.Unlock()

	// Should be allowed now (refilled 2 tokens)
	if !rl.allow("10.0.0.1") {
		t.Error("expected request to be allowed after token refill")
	}
}

func TestRateLimitMiddleware_BlocksAfterBurst(t *testing.T) {
	rl := newRateLimiter(10, 2)
	handler := rateLimitMiddleware(rl)(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))

	// First two requests should pass
	for i := 0; i < 2; i++ {
		req := httptest.NewRequest(http.MethodGet, "/test", nil)
		req.RemoteAddr = "192.168.1.1:12345"
		rr := httptest.NewRecorder()
		handler.ServeHTTP(rr, req)
		if rr.Code != http.StatusOK {
			t.Errorf("request %d: expected 200, got %d", i+1, rr.Code)
		}
	}

	// Third request should be rate limited
	req := httptest.NewRequest(http.MethodGet, "/test", nil)
	req.RemoteAddr = "192.168.1.1:12345"
	rr := httptest.NewRecorder()
	handler.ServeHTTP(rr, req)

	if rr.Code != http.StatusTooManyRequests {
		t.Errorf("expected 429, got %d", rr.Code)
	}
	if !strings.Contains(rr.Body.String(), "Rate limit exceeded") {
		t.Errorf("expected rate limit error message, got %q", rr.Body.String())
	}
}

func TestRateLimitMiddleware_UsesXForwardedFor(t *testing.T) {
	rl := newRateLimiter(10, 1)
	handler := rateLimitMiddleware(rl)(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))

	// Request with X-Forwarded-For header
	req := httptest.NewRequest(http.MethodGet, "/test", nil)
	req.RemoteAddr = "10.0.0.1:12345"
	req.Header.Set("X-Forwarded-For", "203.0.113.50, 70.41.3.18")
	rr := httptest.NewRecorder()

	handler.ServeHTTP(rr, req)

	if rr.Code != http.StatusOK {
		t.Errorf("expected 200, got %d", rr.Code)
	}

	// Second request from same forwarded IP should be blocked
	req2 := httptest.NewRequest(http.MethodGet, "/test", nil)
	req2.RemoteAddr = "10.0.0.2:12345" // different RemoteAddr
	req2.Header.Set("X-Forwarded-For", "203.0.113.50") // same forwarded IP
	rr2 := httptest.NewRecorder()

	handler.ServeHTTP(rr2, req2)

	if rr2.Code != http.StatusTooManyRequests {
		t.Errorf("expected 429 for same forwarded IP, got %d", rr2.Code)
	}
}
