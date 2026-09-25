// Package config reads configuration from environment variables with sane
// defaults, so every service is configured the twelve-factor way.
package config

import (
	"os"
	"strconv"
	"strings"
)

// Env returns the value of key, or def if unset/empty.
func Env(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

// EnvInt reads an integer env var, falling back to def if unset or unparseable.
func EnvInt(key string, def int) int {
	if v := os.Getenv(key); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			return n
		}
	}
	return def
}

// EnvList splits a comma-separated env var into a slice (e.g. Kafka brokers).
func EnvList(key, def string) []string {
	raw := Env(key, def)
	parts := strings.Split(raw, ",")
	out := make([]string, 0, len(parts))
	for _, p := range parts {
		if p = strings.TrimSpace(p); p != "" {
			out = append(out, p)
		}
	}
	return out
}
