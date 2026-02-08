package config

import (
	"fmt"
	"os"
	"regexp"
	"strconv"
	"strings"
	"time"

	"gopkg.in/yaml.v3"
)

// validStrategies is the allowlist of update strategy values.
var validStrategies = map[string]bool{
	"":       true, // inherits from parent/default
	"create": true, // only create if missing, skip if exists
	"update": true, // create or update (default behavior)
}

// EffectiveStrategy returns the first non-empty strategy from the given list,
// defaulting to "update" if all are empty.
func EffectiveStrategy(strategies ...string) string {
	for _, s := range strategies {
		if s != "" {
			return s
		}
	}
	return "update"
}

// Config is the top-level YAML configuration.
type Config struct {
	Strategy string   `yaml:"strategy"`
	Streams  []Stream `yaml:"streams"`
}

// Stream defines a NATS JetStream stream to provision.
type Stream struct {
	Name            string     `yaml:"name"`
	Subjects        []string   `yaml:"subjects"`
	Description     string     `yaml:"description"`
	Retention       string     `yaml:"retention"`
	Storage         string     `yaml:"storage"`
	MaxMsgs         *int64     `yaml:"max_msgs"`
	MaxBytes        *int64     `yaml:"max_bytes"`
	MaxAge          string     `yaml:"max_age"`
	MaxMsgSize      *int32     `yaml:"max_msg_size"`
	NumReplicas     *int       `yaml:"num_replicas"`
	Discard         string     `yaml:"discard"`
	DuplicateWindow string     `yaml:"duplicate_window"`
	AllowRollup     *bool      `yaml:"allow_rollup"`
	DenyDelete      *bool      `yaml:"deny_delete"`
	DenyPurge       *bool      `yaml:"deny_purge"`
	Consumers       []Consumer `yaml:"consumers"`
	Strategy        string     `yaml:"strategy"`
}

// Consumer defines a NATS JetStream consumer to provision.
type Consumer struct {
	Name              string  `yaml:"name"`
	Description       string  `yaml:"description"`
	FilterSubject     string  `yaml:"filter_subject"`
	AckPolicy         string  `yaml:"ack_policy"`
	AckWait           string  `yaml:"ack_wait"`
	MaxDeliver        *int    `yaml:"max_deliver"`
	DeliverPolicy     string  `yaml:"deliver_policy"`
	OptStartSeq       *uint64 `yaml:"opt_start_seq"`
	OptStartTime      string  `yaml:"opt_start_time"`
	ReplayPolicy      string  `yaml:"replay_policy"`
	MaxAckPending     *int    `yaml:"max_ack_pending"`
	InactiveThreshold string  `yaml:"inactive_threshold"`
	DeliverSubject    string  `yaml:"deliver_subject"`
	DeliverGroup      string  `yaml:"deliver_group"`
}

// containsNullByte returns true if s contains a null byte (\x00).
func containsNullByte(s string) bool {
	return strings.ContainsRune(s, '\x00')
}

var envVarPattern = regexp.MustCompile(`\$\{([^}]+)}`)

// expandEnvVars replaces ${VAR} references with their environment variable values.
func expandEnvVars(s string) string {
	return envVarPattern.ReplaceAllStringFunc(s, func(match string) string {
		varName := envVarPattern.FindStringSubmatch(match)[1]
		if val, ok := os.LookupEnv(varName); ok {
			return val
		}
		return match
	})
}

// expandConfig walks the config and expands env vars in string fields.
func expandConfig(cfg *Config) {
	cfg.Strategy = expandEnvVars(cfg.Strategy)
	for i := range cfg.Streams {
		s := &cfg.Streams[i]
		s.Strategy = expandEnvVars(s.Strategy)
		s.Name = expandEnvVars(s.Name)
		s.Description = expandEnvVars(s.Description)
		s.Retention = expandEnvVars(s.Retention)
		s.Storage = expandEnvVars(s.Storage)
		s.MaxAge = expandEnvVars(s.MaxAge)
		s.Discard = expandEnvVars(s.Discard)
		s.DuplicateWindow = expandEnvVars(s.DuplicateWindow)
		for j := range s.Subjects {
			s.Subjects[j] = expandEnvVars(s.Subjects[j])
		}

		for j := range s.Consumers {
			c := &s.Consumers[j]
			c.Name = expandEnvVars(c.Name)
			c.Description = expandEnvVars(c.Description)
			c.FilterSubject = expandEnvVars(c.FilterSubject)
			c.AckPolicy = expandEnvVars(c.AckPolicy)
			c.AckWait = expandEnvVars(c.AckWait)
			c.DeliverPolicy = expandEnvVars(c.DeliverPolicy)
			c.OptStartTime = expandEnvVars(c.OptStartTime)
			c.ReplayPolicy = expandEnvVars(c.ReplayPolicy)
			c.InactiveThreshold = expandEnvVars(c.InactiveThreshold)
			c.DeliverSubject = expandEnvVars(c.DeliverSubject)
			c.DeliverGroup = expandEnvVars(c.DeliverGroup)
		}
	}
}

// Load reads and parses a YAML config file, expanding env vars and validating.
func Load(path string) (*Config, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("reading config file: %w", err)
	}

	var cfg Config
	if err := yaml.Unmarshal(data, &cfg); err != nil {
		return nil, fmt.Errorf("parsing config YAML: %w", err)
	}

	expandConfig(&cfg)

	if err := validate(&cfg); err != nil {
		return nil, fmt.Errorf("validating config: %w", err)
	}

	return &cfg, nil
}

// invalidStreamNameChars contains characters not allowed in stream names.
var invalidStreamNameChars = regexp.MustCompile(`[\s.*>/\\]`)

// invalidConsumerNameChars contains characters not allowed in consumer names.
var invalidConsumerNameChars = regexp.MustCompile(`[\s.*>/\\]`)

var validRetention = map[string]bool{
	"limits":    true,
	"interest":  true,
	"workqueue": true,
	"":          true,
}

var validStorage = map[string]bool{
	"file":   true,
	"memory": true,
	"":       true,
}

var validDiscard = map[string]bool{
	"old": true,
	"new": true,
	"":    true,
}

var validAckPolicy = map[string]bool{
	"none":     true,
	"all":      true,
	"explicit": true,
	"":         true,
}

var validDeliverPolicy = map[string]bool{
	"all":              true,
	"last":             true,
	"new":              true,
	"by_start_sequence": true,
	"by_start_time":    true,
	"":                 true,
}

var validReplayPolicy = map[string]bool{
	"instant":  true,
	"original": true,
	"":         true,
}

// ParseDuration parses a duration string supporting Go durations and "Nd" day notation.
func ParseDuration(s string) (time.Duration, error) {
	if s == "" {
		return 0, nil
	}

	// Check for day notation like "7d" or "30d"
	if strings.HasSuffix(s, "d") {
		prefix := strings.TrimSuffix(s, "d")
		days, err := strconv.Atoi(prefix)
		if err == nil {
			return time.Duration(days) * 24 * time.Hour, nil
		}
	}

	return time.ParseDuration(s)
}

// validateStrategy returns an error if the strategy value is invalid.
func validateStrategy(path, value string) error {
	if !validStrategies[value] {
		return fmt.Errorf("%s: invalid strategy %q (must be \"create\" or \"update\")", path, value)
	}
	return nil
}

func validate(cfg *Config) error {
	if err := validateStrategy("strategy", cfg.Strategy); err != nil {
		return err
	}

	streamNames := make(map[string]bool)

	for i, s := range cfg.Streams {
		if err := validateStrategy(fmt.Sprintf("streams[%d].strategy", i), s.Strategy); err != nil {
			return err
		}
		prefix := fmt.Sprintf("streams[%d]", i)

		if s.Name == "" {
			return fmt.Errorf("%s.name: is required", prefix)
		}
		if containsNullByte(s.Name) {
			return fmt.Errorf("%s.name: contains null byte", prefix)
		}
		if invalidStreamNameChars.MatchString(s.Name) {
			return fmt.Errorf("%s.name: contains invalid characters (whitespace, '.', '*', '>', '/', '\\')", prefix)
		}
		if streamNames[s.Name] {
			return fmt.Errorf("%s.name: duplicate stream name %q", prefix, s.Name)
		}
		streamNames[s.Name] = true

		if containsNullByte(s.Description) {
			return fmt.Errorf("%s.description: contains null byte", prefix)
		}

		if len(s.Subjects) == 0 {
			return fmt.Errorf("%s.subjects: at least one subject is required", prefix)
		}
		for j, subj := range s.Subjects {
			if subj == "" {
				return fmt.Errorf("%s.subjects[%d]: must not be empty", prefix, j)
			}
			if containsNullByte(subj) {
				return fmt.Errorf("%s.subjects[%d]: contains null byte", prefix, j)
			}
		}

		if !validRetention[s.Retention] {
			return fmt.Errorf("%s.retention: invalid value %q (must be limits, interest, or workqueue)", prefix, s.Retention)
		}
		if !validStorage[s.Storage] {
			return fmt.Errorf("%s.storage: invalid value %q (must be file or memory)", prefix, s.Storage)
		}
		if !validDiscard[s.Discard] {
			return fmt.Errorf("%s.discard: invalid value %q (must be old or new)", prefix, s.Discard)
		}

		if s.MaxMsgs != nil && *s.MaxMsgs < -1 {
			return fmt.Errorf("%s.max_msgs: must be >= -1, got %d", prefix, *s.MaxMsgs)
		}
		if s.MaxBytes != nil && *s.MaxBytes < -1 {
			return fmt.Errorf("%s.max_bytes: must be >= -1, got %d", prefix, *s.MaxBytes)
		}
		if s.MaxMsgSize != nil && *s.MaxMsgSize < -1 {
			return fmt.Errorf("%s.max_msg_size: must be >= -1, got %d", prefix, *s.MaxMsgSize)
		}
		if s.NumReplicas != nil && *s.NumReplicas < 1 {
			return fmt.Errorf("%s.num_replicas: must be >= 1, got %d", prefix, *s.NumReplicas)
		}

		if s.MaxAge != "" {
			d, err := ParseDuration(s.MaxAge)
			if err != nil {
				return fmt.Errorf("%s.max_age: invalid duration %q: %w", prefix, s.MaxAge, err)
			}
			if d < 0 {
				return fmt.Errorf("%s.max_age: negative duration %q is not allowed", prefix, s.MaxAge)
			}
		}
		if s.DuplicateWindow != "" {
			d, err := ParseDuration(s.DuplicateWindow)
			if err != nil {
				return fmt.Errorf("%s.duplicate_window: invalid duration %q: %w", prefix, s.DuplicateWindow, err)
			}
			if d < 0 {
				return fmt.Errorf("%s.duplicate_window: negative duration %q is not allowed", prefix, s.DuplicateWindow)
			}
		}

		if err := validateConsumers(prefix, s.Consumers); err != nil {
			return err
		}
	}

	return nil
}

func validateConsumers(streamPrefix string, consumers []Consumer) error {
	names := make(map[string]bool)

	for j, c := range consumers {
		prefix := fmt.Sprintf("%s.consumers[%d]", streamPrefix, j)

		if c.Name == "" {
			return fmt.Errorf("%s.name: is required", prefix)
		}
		if containsNullByte(c.Name) {
			return fmt.Errorf("%s.name: contains null byte", prefix)
		}
		if invalidConsumerNameChars.MatchString(c.Name) {
			return fmt.Errorf("%s.name: contains invalid characters (whitespace, '.', '*', '>', '/', '\\')", prefix)
		}
		if names[c.Name] {
			return fmt.Errorf("%s.name: duplicate consumer name %q", prefix, c.Name)
		}
		names[c.Name] = true

		if containsNullByte(c.Description) {
			return fmt.Errorf("%s.description: contains null byte", prefix)
		}
		if containsNullByte(c.FilterSubject) {
			return fmt.Errorf("%s.filter_subject: contains null byte", prefix)
		}
		if containsNullByte(c.DeliverSubject) {
			return fmt.Errorf("%s.deliver_subject: contains null byte", prefix)
		}
		if containsNullByte(c.DeliverGroup) {
			return fmt.Errorf("%s.deliver_group: contains null byte", prefix)
		}

		if !validAckPolicy[c.AckPolicy] {
			return fmt.Errorf("%s.ack_policy: invalid value %q (must be none, all, or explicit)", prefix, c.AckPolicy)
		}
		if !validDeliverPolicy[c.DeliverPolicy] {
			return fmt.Errorf("%s.deliver_policy: invalid value %q (must be all, last, new, by_start_sequence, or by_start_time)", prefix, c.DeliverPolicy)
		}
		if !validReplayPolicy[c.ReplayPolicy] {
			return fmt.Errorf("%s.replay_policy: invalid value %q (must be instant or original)", prefix, c.ReplayPolicy)
		}

		if c.MaxDeliver != nil && *c.MaxDeliver < -1 {
			return fmt.Errorf("%s.max_deliver: must be >= -1, got %d", prefix, *c.MaxDeliver)
		}
		if c.MaxAckPending != nil && *c.MaxAckPending < -1 {
			return fmt.Errorf("%s.max_ack_pending: must be >= -1, got %d", prefix, *c.MaxAckPending)
		}

		if c.AckWait != "" {
			d, err := ParseDuration(c.AckWait)
			if err != nil {
				return fmt.Errorf("%s.ack_wait: invalid duration %q: %w", prefix, c.AckWait, err)
			}
			if d < 0 {
				return fmt.Errorf("%s.ack_wait: negative duration %q is not allowed", prefix, c.AckWait)
			}
		}
		if c.InactiveThreshold != "" {
			d, err := ParseDuration(c.InactiveThreshold)
			if err != nil {
				return fmt.Errorf("%s.inactive_threshold: invalid duration %q: %w", prefix, c.InactiveThreshold, err)
			}
			if d < 0 {
				return fmt.Errorf("%s.inactive_threshold: negative duration %q is not allowed", prefix, c.InactiveThreshold)
			}
		}

		if c.DeliverPolicy == "by_start_sequence" && c.OptStartSeq == nil {
			return fmt.Errorf("%s: deliver_policy 'by_start_sequence' requires opt_start_seq", prefix)
		}
		if c.DeliverPolicy == "by_start_time" {
			if c.OptStartTime == "" {
				return fmt.Errorf("%s: deliver_policy 'by_start_time' requires opt_start_time", prefix)
			}
			if _, err := time.Parse(time.RFC3339, c.OptStartTime); err != nil {
				return fmt.Errorf("%s.opt_start_time: invalid RFC3339 format %q: %w", prefix, c.OptStartTime, err)
			}
		}
	}

	return nil
}
