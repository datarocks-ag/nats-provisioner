package config

import (
	"os"
	"path/filepath"
	"testing"
	"time"
)

func writeTempConfig(t *testing.T, content string) string {
	t.Helper()
	dir := t.TempDir()
	path := filepath.Join(dir, "config.yaml")
	if err := os.WriteFile(path, []byte(content), 0644); err != nil {
		t.Fatalf("writing temp config: %v", err)
	}
	return path
}

func TestLoadValidConfig(t *testing.T) {
	yaml := `
streams:
  - name: "ORDERS"
    subjects: ["orders.>"]
    retention: "limits"
    storage: "file"
    max_msgs: -1
    max_bytes: 1073741824
    max_age: "7d"
    num_replicas: 1
    consumers:
      - name: "order-processor"
        filter_subject: "orders.created"
        ack_policy: "explicit"
        ack_wait: "30s"
        max_deliver: 5
        deliver_policy: "all"
`
	path := writeTempConfig(t, yaml)
	cfg, err := Load(path)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(cfg.Streams) != 1 {
		t.Fatalf("expected 1 stream, got %d", len(cfg.Streams))
	}
	if cfg.Streams[0].Name != "ORDERS" {
		t.Errorf("expected stream name ORDERS, got %s", cfg.Streams[0].Name)
	}
	if len(cfg.Streams[0].Subjects) != 1 {
		t.Fatalf("expected 1 subject, got %d", len(cfg.Streams[0].Subjects))
	}
	if *cfg.Streams[0].MaxBytes != 1073741824 {
		t.Errorf("expected max_bytes 1073741824, got %d", *cfg.Streams[0].MaxBytes)
	}
	if len(cfg.Streams[0].Consumers) != 1 {
		t.Fatalf("expected 1 consumer, got %d", len(cfg.Streams[0].Consumers))
	}
	if cfg.Streams[0].Consumers[0].Name != "order-processor" {
		t.Errorf("expected consumer name order-processor, got %s", cfg.Streams[0].Consumers[0].Name)
	}
}

func TestEnvVarExpansion(t *testing.T) {
	t.Setenv("TEST_STREAM_NAME", "EVENTS")
	t.Setenv("TEST_SUBJECT", "events.>")

	yaml := `
streams:
  - name: "${TEST_STREAM_NAME}"
    subjects: ["${TEST_SUBJECT}"]
`
	path := writeTempConfig(t, yaml)
	cfg, err := Load(path)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if cfg.Streams[0].Name != "EVENTS" {
		t.Errorf("expected EVENTS, got %s", cfg.Streams[0].Name)
	}
	if cfg.Streams[0].Subjects[0] != "events.>" {
		t.Errorf("expected events.>, got %s", cfg.Streams[0].Subjects[0])
	}
}

func TestUnsetEnvVarPreserved(t *testing.T) {
	os.Unsetenv("TOTALLY_UNSET_VAR")
	result := expandEnvVars("${TOTALLY_UNSET_VAR}")
	if result != "${TOTALLY_UNSET_VAR}" {
		t.Errorf("expected unresolved var to be preserved, got %s", result)
	}
}

func TestEmptyConfig(t *testing.T) {
	yaml := `{}`
	path := writeTempConfig(t, yaml)
	cfg, err := Load(path)
	if err != nil {
		t.Fatalf("unexpected error for empty config: %v", err)
	}
	if len(cfg.Streams) != 0 {
		t.Error("expected empty streams")
	}
}

func TestValidationMissingStreamName(t *testing.T) {
	yaml := `
streams:
  - subjects: ["foo"]
`
	path := writeTempConfig(t, yaml)
	_, err := Load(path)
	if err == nil {
		t.Fatal("expected validation error for missing stream name")
	}
}

func TestValidationInvalidStreamNameChars(t *testing.T) {
	for _, name := range []string{"foo bar", "foo.bar", "foo*bar", "foo>bar", "foo/bar"} {
		t.Run(name, func(t *testing.T) {
			yaml := `
streams:
  - name: "` + name + `"
    subjects: ["test"]
`
			path := writeTempConfig(t, yaml)
			_, err := Load(path)
			if err == nil {
				t.Fatalf("expected validation error for stream name %q", name)
			}
		})
	}
}

func TestValidationDuplicateStreamName(t *testing.T) {
	yaml := `
streams:
  - name: "DUP"
    subjects: ["a"]
  - name: "DUP"
    subjects: ["b"]
`
	path := writeTempConfig(t, yaml)
	_, err := Load(path)
	if err == nil {
		t.Fatal("expected validation error for duplicate stream name")
	}
}

func TestValidationMissingSubjects(t *testing.T) {
	yaml := `
streams:
  - name: "TEST"
`
	path := writeTempConfig(t, yaml)
	_, err := Load(path)
	if err == nil {
		t.Fatal("expected validation error for missing subjects")
	}
}

func TestValidationEmptySubject(t *testing.T) {
	yaml := `
streams:
  - name: "TEST"
    subjects: [""]
`
	path := writeTempConfig(t, yaml)
	_, err := Load(path)
	if err == nil {
		t.Fatal("expected validation error for empty subject")
	}
}

func TestValidationInvalidRetention(t *testing.T) {
	yaml := `
streams:
  - name: "TEST"
    subjects: ["test"]
    retention: "invalid"
`
	path := writeTempConfig(t, yaml)
	_, err := Load(path)
	if err == nil {
		t.Fatal("expected validation error for invalid retention")
	}
}

func TestValidationInvalidStorage(t *testing.T) {
	yaml := `
streams:
  - name: "TEST"
    subjects: ["test"]
    storage: "invalid"
`
	path := writeTempConfig(t, yaml)
	_, err := Load(path)
	if err == nil {
		t.Fatal("expected validation error for invalid storage")
	}
}

func TestValidationInvalidDiscard(t *testing.T) {
	yaml := `
streams:
  - name: "TEST"
    subjects: ["test"]
    discard: "invalid"
`
	path := writeTempConfig(t, yaml)
	_, err := Load(path)
	if err == nil {
		t.Fatal("expected validation error for invalid discard")
	}
}

func TestValidationInvalidAckPolicy(t *testing.T) {
	yaml := `
streams:
  - name: "TEST"
    subjects: ["test"]
    consumers:
      - name: "c1"
        ack_policy: "invalid"
`
	path := writeTempConfig(t, yaml)
	_, err := Load(path)
	if err == nil {
		t.Fatal("expected validation error for invalid ack_policy")
	}
}

func TestValidationInvalidDeliverPolicy(t *testing.T) {
	yaml := `
streams:
  - name: "TEST"
    subjects: ["test"]
    consumers:
      - name: "c1"
        deliver_policy: "invalid"
`
	path := writeTempConfig(t, yaml)
	_, err := Load(path)
	if err == nil {
		t.Fatal("expected validation error for invalid deliver_policy")
	}
}

func TestValidationInvalidReplayPolicy(t *testing.T) {
	yaml := `
streams:
  - name: "TEST"
    subjects: ["test"]
    consumers:
      - name: "c1"
        replay_policy: "invalid"
`
	path := writeTempConfig(t, yaml)
	_, err := Load(path)
	if err == nil {
		t.Fatal("expected validation error for invalid replay_policy")
	}
}

func TestValidationMaxMsgsTooLow(t *testing.T) {
	yaml := `
streams:
  - name: "TEST"
    subjects: ["test"]
    max_msgs: -2
`
	path := writeTempConfig(t, yaml)
	_, err := Load(path)
	if err == nil {
		t.Fatal("expected validation error for max_msgs < -1")
	}
}

func TestValidationNumReplicasTooLow(t *testing.T) {
	yaml := `
streams:
  - name: "TEST"
    subjects: ["test"]
    num_replicas: 0
`
	path := writeTempConfig(t, yaml)
	_, err := Load(path)
	if err == nil {
		t.Fatal("expected validation error for num_replicas < 1")
	}
}

func TestValidationInvalidDuration(t *testing.T) {
	yaml := `
streams:
  - name: "TEST"
    subjects: ["test"]
    max_age: "not-a-duration"
`
	path := writeTempConfig(t, yaml)
	_, err := Load(path)
	if err == nil {
		t.Fatal("expected validation error for invalid max_age duration")
	}
}

func TestValidationByStartSequenceRequiresOptStartSeq(t *testing.T) {
	yaml := `
streams:
  - name: "TEST"
    subjects: ["test"]
    consumers:
      - name: "c1"
        deliver_policy: "by_start_sequence"
`
	path := writeTempConfig(t, yaml)
	_, err := Load(path)
	if err == nil {
		t.Fatal("expected validation error for by_start_sequence without opt_start_seq")
	}
}

func TestValidationByStartTimeRequiresOptStartTime(t *testing.T) {
	yaml := `
streams:
  - name: "TEST"
    subjects: ["test"]
    consumers:
      - name: "c1"
        deliver_policy: "by_start_time"
`
	path := writeTempConfig(t, yaml)
	_, err := Load(path)
	if err == nil {
		t.Fatal("expected validation error for by_start_time without opt_start_time")
	}
}

func TestValidationDuplicateConsumerName(t *testing.T) {
	yaml := `
streams:
  - name: "TEST"
    subjects: ["test"]
    consumers:
      - name: "dup"
      - name: "dup"
`
	path := writeTempConfig(t, yaml)
	_, err := Load(path)
	if err == nil {
		t.Fatal("expected validation error for duplicate consumer name")
	}
}

func TestValidationMissingConsumerName(t *testing.T) {
	yaml := `
streams:
  - name: "TEST"
    subjects: ["test"]
    consumers:
      - description: "no name"
`
	path := writeTempConfig(t, yaml)
	_, err := Load(path)
	if err == nil {
		t.Fatal("expected validation error for missing consumer name")
	}
}

func TestValidationInvalidConsumerNameChars(t *testing.T) {
	yaml := `
streams:
  - name: "TEST"
    subjects: ["test"]
    consumers:
      - name: "bad consumer"
`
	path := writeTempConfig(t, yaml)
	_, err := Load(path)
	if err == nil {
		t.Fatal("expected validation error for invalid consumer name")
	}
}

func TestValidationNullByteInStreamName(t *testing.T) {
	yaml := "streams:\n  - name: \"test\\x00evil\"\n    subjects: [\"test\"]\n"
	path := writeTempConfig(t, yaml)
	_, err := Load(path)
	if err == nil {
		t.Fatal("expected validation error for null byte in stream name")
	}
}

func TestValidationNullByteInSubject(t *testing.T) {
	yaml := "streams:\n  - name: \"TEST\"\n    subjects: [\"test\\x00evil\"]\n"
	path := writeTempConfig(t, yaml)
	_, err := Load(path)
	if err == nil {
		t.Fatal("expected validation error for null byte in subject")
	}
}

func TestValidationNullByteInConsumerName(t *testing.T) {
	yaml := "streams:\n  - name: \"TEST\"\n    subjects: [\"test\"]\n    consumers:\n      - name: \"bad\\x00name\"\n"
	path := writeTempConfig(t, yaml)
	_, err := Load(path)
	if err == nil {
		t.Fatal("expected validation error for null byte in consumer name")
	}
}

func TestValidationNullByteInConsumerDescription(t *testing.T) {
	yaml := "streams:\n  - name: \"TEST\"\n    subjects: [\"test\"]\n    consumers:\n      - name: \"c1\"\n        description: \"bad\\x00desc\"\n"
	path := writeTempConfig(t, yaml)
	_, err := Load(path)
	if err == nil {
		t.Fatal("expected validation error for null byte in consumer description")
	}
}

func TestValidationNullByteInFilterSubject(t *testing.T) {
	yaml := "streams:\n  - name: \"TEST\"\n    subjects: [\"test\"]\n    consumers:\n      - name: \"c1\"\n        filter_subject: \"bad\\x00subj\"\n"
	path := writeTempConfig(t, yaml)
	_, err := Load(path)
	if err == nil {
		t.Fatal("expected validation error for null byte in filter_subject")
	}
}

func TestValidationNullByteInDeliverSubject(t *testing.T) {
	yaml := "streams:\n  - name: \"TEST\"\n    subjects: [\"test\"]\n    consumers:\n      - name: \"c1\"\n        deliver_subject: \"bad\\x00subj\"\n"
	path := writeTempConfig(t, yaml)
	_, err := Load(path)
	if err == nil {
		t.Fatal("expected validation error for null byte in deliver_subject")
	}
}

func TestValidationNullByteInDeliverGroup(t *testing.T) {
	yaml := "streams:\n  - name: \"TEST\"\n    subjects: [\"test\"]\n    consumers:\n      - name: \"c1\"\n        deliver_group: \"bad\\x00group\"\n"
	path := writeTempConfig(t, yaml)
	_, err := Load(path)
	if err == nil {
		t.Fatal("expected validation error for null byte in deliver_group")
	}
}

func TestValidationNullByteInStreamDescription(t *testing.T) {
	yaml := "streams:\n  - name: \"TEST\"\n    subjects: [\"test\"]\n    description: \"bad\\x00desc\"\n"
	path := writeTempConfig(t, yaml)
	_, err := Load(path)
	if err == nil {
		t.Fatal("expected validation error for null byte in stream description")
	}
}

func TestParseDuration(t *testing.T) {
	tests := []struct {
		input string
		want  time.Duration
	}{
		{"", 0},
		{"30s", 30 * time.Second},
		{"5m", 5 * time.Minute},
		{"2h", 2 * time.Hour},
		{"7d", 7 * 24 * time.Hour},
		{"30d", 30 * 24 * time.Hour},
		{"1h30m", 90 * time.Minute},
	}

	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			got, err := ParseDuration(tt.input)
			if err != nil {
				t.Fatalf("ParseDuration(%q) error: %v", tt.input, err)
			}
			if got != tt.want {
				t.Errorf("ParseDuration(%q) = %v, want %v", tt.input, got, tt.want)
			}
		})
	}
}

func TestParseDurationInvalid(t *testing.T) {
	_, err := ParseDuration("not-a-duration")
	if err == nil {
		t.Fatal("expected error for invalid duration")
	}
}

func TestValidationMaxDeliverTooLow(t *testing.T) {
	yaml := `
streams:
  - name: "TEST"
    subjects: ["test"]
    consumers:
      - name: "c1"
        max_deliver: -2
`
	path := writeTempConfig(t, yaml)
	_, err := Load(path)
	if err == nil {
		t.Fatal("expected validation error for max_deliver < -1")
	}
}

func TestValidationMaxAckPendingTooLow(t *testing.T) {
	yaml := `
streams:
  - name: "TEST"
    subjects: ["test"]
    consumers:
      - name: "c1"
        max_ack_pending: -2
`
	path := writeTempConfig(t, yaml)
	_, err := Load(path)
	if err == nil {
		t.Fatal("expected validation error for max_ack_pending < -1")
	}
}

func TestValidationValidByStartSequence(t *testing.T) {
	yaml := `
streams:
  - name: "TEST"
    subjects: ["test"]
    consumers:
      - name: "c1"
        deliver_policy: "by_start_sequence"
        opt_start_seq: 100
`
	path := writeTempConfig(t, yaml)
	_, err := Load(path)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestValidationInvalidGlobalStrategy(t *testing.T) {
	yaml := `
strategy: "invalid"
streams:
  - name: "TEST"
    subjects: ["test"]
`
	path := writeTempConfig(t, yaml)
	_, err := Load(path)
	if err == nil {
		t.Fatal("expected validation error for invalid global strategy")
	}
}

func TestValidationInvalidStreamStrategy(t *testing.T) {
	yaml := `
streams:
  - name: "TEST"
    subjects: ["test"]
    strategy: "invalid"
`
	path := writeTempConfig(t, yaml)
	_, err := Load(path)
	if err == nil {
		t.Fatal("expected validation error for invalid stream strategy")
	}
}

func TestValidStrategies(t *testing.T) {
	for _, strategy := range []string{"create", "update"} {
		t.Run(strategy, func(t *testing.T) {
			yaml := `
strategy: "` + strategy + `"
streams:
  - name: "TEST"
    subjects: ["test"]
    strategy: "` + strategy + `"
`
			path := writeTempConfig(t, yaml)
			_, err := Load(path)
			if err != nil {
				t.Fatalf("unexpected error for strategy %q: %v", strategy, err)
			}
		})
	}
}

func TestEffectiveStrategy(t *testing.T) {
	tests := []struct {
		name       string
		strategies []string
		want       string
	}{
		{"all empty defaults to update", []string{"", ""}, "update"},
		{"first wins", []string{"create", "update"}, "create"},
		{"fallback to second", []string{"", "create"}, "create"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := EffectiveStrategy(tt.strategies...)
			if got != tt.want {
				t.Errorf("EffectiveStrategy(%v) = %q, want %q", tt.strategies, got, tt.want)
			}
		})
	}
}

func TestValidationValidByStartTime(t *testing.T) {
	yaml := `
streams:
  - name: "TEST"
    subjects: ["test"]
    consumers:
      - name: "c1"
        deliver_policy: "by_start_time"
        opt_start_time: "2024-01-01T00:00:00Z"
`
	path := writeTempConfig(t, yaml)
	_, err := Load(path)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}
