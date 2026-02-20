package main

import (
	"context"
	"log/slog"
	"os"
	"testing"
)

func TestSetupLogging(t *testing.T) {
	tests := []struct {
		level    string
		expected slog.Level
	}{
		{"debug", slog.LevelDebug},
		{"warn", slog.LevelWarn},
		{"error", slog.LevelError},
		{"info", slog.LevelInfo},
		{"unknown", slog.LevelInfo},
	}

	for _, tt := range tests {
		t.Run(tt.level, func(t *testing.T) {
			t.Setenv("LOG_LEVEL", tt.level)
			setupLogging()

			handler := slog.Default().Handler()
			// Verify the configured level is enabled and the level below it is not
			if !handler.Enabled(context.Background(), tt.expected) {
				t.Errorf("expected level %v to be enabled", tt.expected)
			}
			if tt.expected > slog.LevelDebug {
				if handler.Enabled(context.Background(), tt.expected-1) {
					t.Errorf("expected level %v to be disabled", tt.expected-1)
				}
			}
		})
	}
}

func TestEnvOrDefault(t *testing.T) {
	t.Run("returns env value when set", func(t *testing.T) {
		t.Setenv("TEST_ENV_OR_DEFAULT", "from-env")
		got := envOrDefault("TEST_ENV_OR_DEFAULT", "fallback")
		if got != "from-env" {
			t.Errorf("expected 'from-env', got %q", got)
		}
	})

	t.Run("returns default when unset", func(t *testing.T) {
		os.Unsetenv("TEST_ENV_OR_DEFAULT_UNSET")
		got := envOrDefault("TEST_ENV_OR_DEFAULT_UNSET", "fallback")
		if got != "fallback" {
			t.Errorf("expected 'fallback', got %q", got)
		}
	})

	t.Run("returns default when empty", func(t *testing.T) {
		t.Setenv("TEST_ENV_OR_DEFAULT_EMPTY", "")
		got := envOrDefault("TEST_ENV_OR_DEFAULT_EMPTY", "fallback")
		if got != "fallback" {
			t.Errorf("expected 'fallback', got %q", got)
		}
	})
}
