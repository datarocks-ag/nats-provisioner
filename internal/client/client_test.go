package client

import (
	"errors"
	"strings"
	"testing"
)

func TestRedactURL(t *testing.T) {
	tests := []struct {
		name string
		in   string
		want string
	}{
		{
			name: "no credentials is unchanged",
			in:   "nats://localhost:4222",
			want: "nats://localhost:4222",
		},
		{
			name: "user and password are masked",
			in:   "nats://admin:s3cr3t@nats:4222",
			want: "nats://redacted@nats:4222",
		},
		{
			name: "user only is masked",
			in:   "nats://admin@nats:4222",
			want: "nats://redacted@nats:4222",
		},
		{
			name: "password with url-special characters is masked",
			in:   "nats://admin:p@ss:w0rd@nats:4222",
			want: "nats://redacted@nats:4222",
		},
		{
			name: "comma-separated list redacts each entry",
			in:   "nats://a:b@host1:4222,nats://host2:4222",
			want: "nats://redacted@host1:4222,nats://host2:4222",
		},
		{
			name: "host without scheme and no credentials is unchanged",
			in:   "localhost:4222",
			want: "localhost:4222",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := RedactURL(tt.in)
			if got != tt.want {
				t.Errorf("RedactURL(%q) = %q, want %q", tt.in, got, tt.want)
			}
			if strings.Contains(got, "s3cr3t") || strings.Contains(got, "p@ss:w0rd") {
				t.Errorf("RedactURL(%q) leaked a password: %q", tt.in, got)
			}
		})
	}
}

func TestRedactError(t *testing.T) {
	url := "nats://admin:s3cr3t@nats:4222"
	password := "s3cr3t"

	t.Run("masks embedded url", func(t *testing.T) {
		err := errors.New(`nats: no servers available, last error: dial nats://admin:s3cr3t@nats:4222: refused`)
		got := redactError(err, url, password)
		if strings.Contains(got, "s3cr3t") {
			t.Errorf("redactError leaked password: %q", got)
		}
		if !strings.Contains(got, "nats://redacted@nats:4222") {
			t.Errorf("redactError did not mask embedded url: %q", got)
		}
	})

	t.Run("masks bare password occurrence", func(t *testing.T) {
		err := errors.New(`authorization violation for password s3cr3t`)
		got := redactError(err, url, password)
		if strings.Contains(got, "s3cr3t") {
			t.Errorf("redactError leaked password: %q", got)
		}
	})

	t.Run("leaves error without secrets unchanged", func(t *testing.T) {
		err := errors.New("nats: connection refused")
		got := redactError(err, url, password)
		if got != "nats: connection refused" {
			t.Errorf("redactError altered a clean message: %q", got)
		}
	})

	t.Run("empty url and password is a no-op", func(t *testing.T) {
		err := errors.New("dial tcp 127.0.0.1:4222: connect: connection refused")
		got := redactError(err, "", "")
		if got != err.Error() {
			t.Errorf("redactError with empty creds changed message: %q", got)
		}
	})
}
