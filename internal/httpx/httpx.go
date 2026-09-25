// Package httpx has tiny helpers for writing JSON HTTP handlers with the standard
// library only (Go 1.22+ ServeMux with method+path patterns). No web framework -
// the routing and error mapping stay explicit.
package httpx

import (
	"encoding/json"
	"log/slog"
	"net/http"
)

// JSON writes v as a JSON response with the given status code.
func JSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	if v != nil {
		if err := json.NewEncoder(w).Encode(v); err != nil {
			slog.Error("encode response", "error", err)
		}
	}
}

// Error writes a JSON error body {"error": msg} with the given status.
func Error(w http.ResponseWriter, status int, msg string) {
	JSON(w, status, map[string]string{"error": msg})
}

// Decode reads and unmarshals a JSON request body into dst, returning false and
// writing a 400 if the body is malformed.
func Decode(w http.ResponseWriter, r *http.Request, dst any) bool {
	if err := json.NewDecoder(r.Body).Decode(dst); err != nil {
		Error(w, http.StatusBadRequest, "invalid JSON body: "+err.Error())
		return false
	}
	return true
}

// Logging wraps a handler to log method, path, and duration - a minimal access log.
func Logging(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		slog.Info("request", "method", r.Method, "path", r.URL.Path)
		next.ServeHTTP(w, r)
	})
}
