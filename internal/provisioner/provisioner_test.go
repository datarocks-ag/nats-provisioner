package provisioner

import (
	"context"
	"errors"
	"sync"
	"testing"

	"github.com/nats-io/nats.go/jetstream"

	"nats-provisioner/internal/config"
)

// mockStream implements jetstream.Stream for testing.
type mockStream struct {
	jetstream.Stream
	info jetstream.StreamInfo
}

func (m *mockStream) CachedInfo() *jetstream.StreamInfo {
	return &m.info
}

// mockJetStream implements JetStreamManager for testing.
type mockJetStream struct {
	mu        sync.Mutex
	streams   map[string]jetstream.StreamConfig
	consumers map[string]map[string]jetstream.ConsumerConfig

	createStreamCalls           int
	updateStreamCalls           int
	createOrUpdateConsumerCalls int

	createStreamErr           error
	updateStreamErr           error
	streamErr                 error
	createOrUpdateConsumerErr error
}

func newMockJetStream() *mockJetStream {
	return &mockJetStream{
		streams:   make(map[string]jetstream.StreamConfig),
		consumers: make(map[string]map[string]jetstream.ConsumerConfig),
	}
}

func (m *mockJetStream) CreateStream(_ context.Context, cfg jetstream.StreamConfig) (jetstream.Stream, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.createStreamCalls++
	if m.createStreamErr != nil {
		return nil, m.createStreamErr
	}
	m.streams[cfg.Name] = cfg
	return &mockStream{info: jetstream.StreamInfo{Config: cfg}}, nil
}

func (m *mockJetStream) UpdateStream(_ context.Context, cfg jetstream.StreamConfig) (jetstream.Stream, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.updateStreamCalls++
	if m.updateStreamErr != nil {
		return nil, m.updateStreamErr
	}
	m.streams[cfg.Name] = cfg
	return &mockStream{info: jetstream.StreamInfo{Config: cfg}}, nil
}

func (m *mockJetStream) Stream(_ context.Context, name string) (jetstream.Stream, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.streamErr != nil {
		return nil, m.streamErr
	}
	cfg, ok := m.streams[name]
	if !ok {
		return nil, jetstream.ErrStreamNotFound
	}
	return &mockStream{info: jetstream.StreamInfo{Config: cfg}}, nil
}

func (m *mockJetStream) CreateOrUpdateConsumer(_ context.Context, stream string, cfg jetstream.ConsumerConfig) (jetstream.Consumer, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.createOrUpdateConsumerCalls++
	if m.createOrUpdateConsumerErr != nil {
		return nil, m.createOrUpdateConsumerErr
	}
	if m.consumers[stream] == nil {
		m.consumers[stream] = make(map[string]jetstream.ConsumerConfig)
	}
	m.consumers[stream][cfg.Name] = cfg
	return nil, nil
}

func TestEnsureStreamCreatesWhenMissing(t *testing.T) {
	mock := newMockJetStream()

	maxMsgs := int64(-1)
	stream := config.Stream{
		Name:      "ORDERS",
		Subjects:  []string{"orders.>"},
		Retention: "limits",
		Storage:   "file",
		MaxMsgs:   &maxMsgs,
	}

	p := New(mock, &config.Config{})
	if err := p.ensureStream(context.Background(), stream, "update"); err != nil {
		t.Fatalf("ensureStream: %v", err)
	}

	if mock.createStreamCalls != 1 {
		t.Errorf("expected 1 CreateStream call, got %d", mock.createStreamCalls)
	}
	cfg := mock.streams["ORDERS"]
	if cfg.Name != "ORDERS" {
		t.Errorf("expected stream name ORDERS, got %s", cfg.Name)
	}
	if cfg.MaxMsgs != -1 {
		t.Errorf("expected max_msgs -1, got %d", cfg.MaxMsgs)
	}
}

func TestEnsureStreamUpdate(t *testing.T) {
	mock := newMockJetStream()
	// Pre-populate existing stream
	mock.streams["ORDERS"] = jetstream.StreamConfig{
		Name:      "ORDERS",
		Subjects:  []string{"orders.>"},
		Storage:   jetstream.FileStorage,
		Retention: jetstream.LimitsPolicy,
	}

	stream := config.Stream{
		Name:     "ORDERS",
		Subjects: []string{"orders.>", "orders-v2.>"},
		Storage:  "file",
	}

	p := New(mock, &config.Config{})
	if err := p.ensureStream(context.Background(), stream, "update"); err != nil {
		t.Fatalf("ensureStream: %v", err)
	}

	if mock.createStreamCalls != 0 {
		t.Errorf("expected 0 CreateStream calls, got %d", mock.createStreamCalls)
	}
	if mock.updateStreamCalls != 1 {
		t.Errorf("expected 1 UpdateStream call, got %d", mock.updateStreamCalls)
	}
}

func TestEnsureStreamCreateStrategySkipsExisting(t *testing.T) {
	mock := newMockJetStream()
	// Pre-populate existing stream
	mock.streams["ORDERS"] = jetstream.StreamConfig{
		Name:      "ORDERS",
		Subjects:  []string{"orders.>"},
		Storage:   jetstream.FileStorage,
		Retention: jetstream.LimitsPolicy,
	}

	stream := config.Stream{
		Name:     "ORDERS",
		Subjects: []string{"orders.>", "orders-v2.>"},
		Storage:  "file",
	}

	p := New(mock, &config.Config{})
	if err := p.ensureStream(context.Background(), stream, "create"); err != nil {
		t.Fatalf("ensureStream: %v", err)
	}

	// Should not have called UpdateStream or CreateStream
	if mock.createStreamCalls != 0 {
		t.Errorf("expected 0 CreateStream calls, got %d", mock.createStreamCalls)
	}
	if mock.updateStreamCalls != 0 {
		t.Errorf("expected 0 UpdateStream calls, got %d", mock.updateStreamCalls)
	}
}

func TestEnsureConsumer(t *testing.T) {
	mock := newMockJetStream()
	mock.streams["ORDERS"] = jetstream.StreamConfig{Name: "ORDERS"}

	consumer := config.Consumer{
		Name:          "order-processor",
		FilterSubject: "orders.created",
		AckPolicy:     "explicit",
		AckWait:       "30s",
	}

	p := New(mock, &config.Config{})
	if err := p.ensureConsumer(context.Background(), "ORDERS", consumer); err != nil {
		t.Fatalf("ensureConsumer: %v", err)
	}

	if mock.createOrUpdateConsumerCalls != 1 {
		t.Errorf("expected 1 CreateOrUpdateConsumer call, got %d", mock.createOrUpdateConsumerCalls)
	}
	cfg := mock.consumers["ORDERS"]["order-processor"]
	if cfg.Name != "order-processor" {
		t.Errorf("expected consumer name order-processor, got %s", cfg.Name)
	}
	if cfg.FilterSubject != "orders.created" {
		t.Errorf("expected filter_subject orders.created, got %s", cfg.FilterSubject)
	}
}

func TestRunFullProvisioning(t *testing.T) {
	mock := newMockJetStream()

	maxMsgs := int64(-1)
	replicas := 1
	cfg := &config.Config{
		Streams: []config.Stream{
			{
				Name:        "ORDERS",
				Subjects:    []string{"orders.>"},
				Retention:   "limits",
				Storage:     "file",
				MaxMsgs:     &maxMsgs,
				NumReplicas: &replicas,
				Consumers: []config.Consumer{
					{
						Name:      "order-processor",
						AckPolicy: "explicit",
					},
					{
						Name:      "order-logger",
						AckPolicy: "none",
					},
				},
			},
		},
	}

	p := New(mock, cfg)
	if err := p.Run(context.Background()); err != nil {
		t.Fatalf("Run: %v", err)
	}

	if mock.createStreamCalls != 1 {
		t.Errorf("expected 1 CreateStream call, got %d", mock.createStreamCalls)
	}
	if mock.createOrUpdateConsumerCalls != 2 {
		t.Errorf("expected 2 CreateOrUpdateConsumer calls, got %d", mock.createOrUpdateConsumerCalls)
	}
}

func TestStreamCreateError(t *testing.T) {
	mock := newMockJetStream()
	mock.createStreamErr = errors.New("create failed")

	cfg := &config.Config{
		Streams: []config.Stream{
			{Name: "FAIL", Subjects: []string{"fail.>"}},
		},
	}

	p := New(mock, cfg)
	err := p.Run(context.Background())
	if err == nil {
		t.Fatal("expected error from Run")
	}
}

func TestConsumerCreateError(t *testing.T) {
	mock := newMockJetStream()
	mock.createOrUpdateConsumerErr = errors.New("consumer failed")

	cfg := &config.Config{
		Streams: []config.Stream{
			{
				Name:     "TEST",
				Subjects: []string{"test.>"},
				Consumers: []config.Consumer{
					{Name: "fail-consumer"},
				},
			},
		},
	}

	p := New(mock, cfg)
	err := p.Run(context.Background())
	if err == nil {
		t.Fatal("expected error from Run")
	}
}

func TestBuildStreamConfigDefaults(t *testing.T) {
	s := config.Stream{
		Name:     "TEST",
		Subjects: []string{"test.>"},
	}

	cfg, err := buildStreamConfig(s)
	if err != nil {
		t.Fatalf("buildStreamConfig: %v", err)
	}

	if cfg.Storage != jetstream.FileStorage {
		t.Errorf("expected default storage FileStorage, got %v", cfg.Storage)
	}
	if cfg.Retention != jetstream.LimitsPolicy {
		t.Errorf("expected default retention LimitsPolicy, got %v", cfg.Retention)
	}
	if cfg.Discard != jetstream.DiscardOld {
		t.Errorf("expected default discard DiscardOld, got %v", cfg.Discard)
	}
}

func TestBuildStreamConfigDurations(t *testing.T) {
	s := config.Stream{
		Name:            "TEST",
		Subjects:        []string{"test.>"},
		MaxAge:          "7d",
		DuplicateWindow: "2m",
	}

	cfg, err := buildStreamConfig(s)
	if err != nil {
		t.Fatalf("buildStreamConfig: %v", err)
	}

	if cfg.MaxAge != 7*24*60*60*1e9 { // 7 days in nanoseconds
		t.Errorf("expected MaxAge 7d, got %v", cfg.MaxAge)
	}
	if cfg.Duplicates != 2*60*1e9 { // 2 minutes in nanoseconds
		t.Errorf("expected Duplicates 2m, got %v", cfg.Duplicates)
	}
}

func TestBuildConsumerConfigAckPolicies(t *testing.T) {
	tests := []struct {
		input string
		want  jetstream.AckPolicy
	}{
		{"none", jetstream.AckNonePolicy},
		{"all", jetstream.AckAllPolicy},
		{"explicit", jetstream.AckExplicitPolicy},
		{"", jetstream.AckExplicitPolicy},
	}

	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			c := config.Consumer{Name: "test", AckPolicy: tt.input}
			cfg, err := buildConsumerConfig(c)
			if err != nil {
				t.Fatalf("buildConsumerConfig: %v", err)
			}
			if cfg.AckPolicy != tt.want {
				t.Errorf("expected AckPolicy %v, got %v", tt.want, cfg.AckPolicy)
			}
		})
	}
}

func TestBuildConsumerConfigDeliverPolicies(t *testing.T) {
	tests := []struct {
		input string
		want  jetstream.DeliverPolicy
	}{
		{"all", jetstream.DeliverAllPolicy},
		{"last", jetstream.DeliverLastPolicy},
		{"new", jetstream.DeliverNewPolicy},
		{"", jetstream.DeliverAllPolicy},
	}

	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			c := config.Consumer{Name: "test", DeliverPolicy: tt.input}
			cfg, err := buildConsumerConfig(c)
			if err != nil {
				t.Fatalf("buildConsumerConfig: %v", err)
			}
			if cfg.DeliverPolicy != tt.want {
				t.Errorf("expected DeliverPolicy %v, got %v", tt.want, cfg.DeliverPolicy)
			}
		})
	}
}

// --- buildStreamConfig branch coverage ---

func TestBuildStreamConfigRetentionPolicies(t *testing.T) {
	tests := []struct {
		input string
		want  jetstream.RetentionPolicy
	}{
		{"limits", jetstream.LimitsPolicy},
		{"interest", jetstream.InterestPolicy},
		{"workqueue", jetstream.WorkQueuePolicy},
		{"", jetstream.LimitsPolicy},
	}
	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			s := config.Stream{Name: "T", Subjects: []string{"t"}, Retention: tt.input}
			cfg, err := buildStreamConfig(s)
			if err != nil {
				t.Fatalf("buildStreamConfig: %v", err)
			}
			if cfg.Retention != tt.want {
				t.Errorf("expected Retention %v, got %v", tt.want, cfg.Retention)
			}
		})
	}
}

func TestBuildStreamConfigUnknownRetention(t *testing.T) {
	s := config.Stream{Name: "T", Subjects: []string{"t"}, Retention: "bad"}
	_, err := buildStreamConfig(s)
	if err == nil {
		t.Fatal("expected error for unknown retention")
	}
}

func TestBuildStreamConfigStorageTypes(t *testing.T) {
	tests := []struct {
		input string
		want  jetstream.StorageType
	}{
		{"file", jetstream.FileStorage},
		{"memory", jetstream.MemoryStorage},
		{"", jetstream.FileStorage},
	}
	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			s := config.Stream{Name: "T", Subjects: []string{"t"}, Storage: tt.input}
			cfg, err := buildStreamConfig(s)
			if err != nil {
				t.Fatalf("buildStreamConfig: %v", err)
			}
			if cfg.Storage != tt.want {
				t.Errorf("expected Storage %v, got %v", tt.want, cfg.Storage)
			}
		})
	}
}

func TestBuildStreamConfigUnknownStorage(t *testing.T) {
	s := config.Stream{Name: "T", Subjects: []string{"t"}, Storage: "bad"}
	_, err := buildStreamConfig(s)
	if err == nil {
		t.Fatal("expected error for unknown storage")
	}
}

func TestBuildStreamConfigDiscardPolicies(t *testing.T) {
	tests := []struct {
		input string
		want  jetstream.DiscardPolicy
	}{
		{"old", jetstream.DiscardOld},
		{"new", jetstream.DiscardNew},
		{"", jetstream.DiscardOld},
	}
	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			s := config.Stream{Name: "T", Subjects: []string{"t"}, Discard: tt.input}
			cfg, err := buildStreamConfig(s)
			if err != nil {
				t.Fatalf("buildStreamConfig: %v", err)
			}
			if cfg.Discard != tt.want {
				t.Errorf("expected Discard %v, got %v", tt.want, cfg.Discard)
			}
		})
	}
}

func TestBuildStreamConfigUnknownDiscard(t *testing.T) {
	s := config.Stream{Name: "T", Subjects: []string{"t"}, Discard: "bad"}
	_, err := buildStreamConfig(s)
	if err == nil {
		t.Fatal("expected error for unknown discard")
	}
}

func TestBuildStreamConfigPointerFields(t *testing.T) {
	maxBytes := int64(1024)
	maxMsgSize := int32(512)
	numReplicas := 3
	allowRollup := true
	denyDelete := true
	denyPurge := true

	s := config.Stream{
		Name:        "T",
		Subjects:    []string{"t"},
		MaxBytes:    &maxBytes,
		MaxMsgSize:  &maxMsgSize,
		NumReplicas: &numReplicas,
		AllowRollup: &allowRollup,
		DenyDelete:  &denyDelete,
		DenyPurge:   &denyPurge,
	}

	cfg, err := buildStreamConfig(s)
	if err != nil {
		t.Fatalf("buildStreamConfig: %v", err)
	}
	if cfg.MaxBytes != 1024 {
		t.Errorf("expected MaxBytes 1024, got %d", cfg.MaxBytes)
	}
	if cfg.MaxMsgSize != 512 {
		t.Errorf("expected MaxMsgSize 512, got %d", cfg.MaxMsgSize)
	}
	if cfg.Replicas != 3 {
		t.Errorf("expected Replicas 3, got %d", cfg.Replicas)
	}
	if !cfg.AllowRollup {
		t.Error("expected AllowRollup true")
	}
	if !cfg.DenyDelete {
		t.Error("expected DenyDelete true")
	}
	if !cfg.DenyPurge {
		t.Error("expected DenyPurge true")
	}
}

func TestBuildStreamConfigDescription(t *testing.T) {
	s := config.Stream{
		Name:        "T",
		Subjects:    []string{"t"},
		Description: "my stream",
	}
	cfg, err := buildStreamConfig(s)
	if err != nil {
		t.Fatalf("buildStreamConfig: %v", err)
	}
	if cfg.Description != "my stream" {
		t.Errorf("expected description 'my stream', got %q", cfg.Description)
	}
}

func TestBuildStreamConfigInvalidMaxAge(t *testing.T) {
	s := config.Stream{Name: "T", Subjects: []string{"t"}, MaxAge: "bad"}
	_, err := buildStreamConfig(s)
	if err == nil {
		t.Fatal("expected error for invalid max_age")
	}
}

func TestBuildStreamConfigInvalidDuplicateWindow(t *testing.T) {
	s := config.Stream{Name: "T", Subjects: []string{"t"}, DuplicateWindow: "bad"}
	_, err := buildStreamConfig(s)
	if err == nil {
		t.Fatal("expected error for invalid duplicate_window")
	}
}

// --- buildConsumerConfig branch coverage ---

func TestBuildConsumerConfigDeliverSubjectError(t *testing.T) {
	c := config.Consumer{Name: "c1", DeliverSubject: "some.subject"}
	_, err := buildConsumerConfig(c)
	if err == nil {
		t.Fatal("expected error for deliver_subject set")
	}
}

func TestBuildConsumerConfigDeliverGroupError(t *testing.T) {
	c := config.Consumer{Name: "c1", DeliverGroup: "some-group"}
	_, err := buildConsumerConfig(c)
	if err == nil {
		t.Fatal("expected error for deliver_group set")
	}
}

func TestBuildConsumerConfigUnknownAckPolicy(t *testing.T) {
	c := config.Consumer{Name: "c1", AckPolicy: "bad"}
	_, err := buildConsumerConfig(c)
	if err == nil {
		t.Fatal("expected error for unknown ack policy")
	}
}

func TestBuildConsumerConfigByStartSequence(t *testing.T) {
	seq := uint64(42)
	c := config.Consumer{Name: "c1", DeliverPolicy: "by_start_sequence", OptStartSeq: &seq}
	cfg, err := buildConsumerConfig(c)
	if err != nil {
		t.Fatalf("buildConsumerConfig: %v", err)
	}
	if cfg.DeliverPolicy != jetstream.DeliverByStartSequencePolicy {
		t.Errorf("expected DeliverByStartSequencePolicy, got %v", cfg.DeliverPolicy)
	}
	if cfg.OptStartSeq != 42 {
		t.Errorf("expected OptStartSeq 42, got %d", cfg.OptStartSeq)
	}
}

func TestBuildConsumerConfigByStartTimeValid(t *testing.T) {
	c := config.Consumer{
		Name:          "c1",
		DeliverPolicy: "by_start_time",
		OptStartTime:  "2024-06-15T10:00:00Z",
	}
	cfg, err := buildConsumerConfig(c)
	if err != nil {
		t.Fatalf("buildConsumerConfig: %v", err)
	}
	if cfg.DeliverPolicy != jetstream.DeliverByStartTimePolicy {
		t.Errorf("expected DeliverByStartTimePolicy, got %v", cfg.DeliverPolicy)
	}
	if cfg.OptStartTime == nil {
		t.Fatal("expected OptStartTime to be set")
	}
}

func TestBuildConsumerConfigByStartTimeInvalid(t *testing.T) {
	c := config.Consumer{
		Name:          "c1",
		DeliverPolicy: "by_start_time",
		OptStartTime:  "not-rfc3339",
	}
	_, err := buildConsumerConfig(c)
	if err == nil {
		t.Fatal("expected error for invalid opt_start_time")
	}
}

func TestBuildConsumerConfigUnknownDeliverPolicy(t *testing.T) {
	c := config.Consumer{Name: "c1", DeliverPolicy: "bad"}
	_, err := buildConsumerConfig(c)
	if err == nil {
		t.Fatal("expected error for unknown deliver policy")
	}
}

func TestBuildConsumerConfigReplayPolicies(t *testing.T) {
	tests := []struct {
		input string
		want  jetstream.ReplayPolicy
	}{
		{"instant", jetstream.ReplayInstantPolicy},
		{"original", jetstream.ReplayOriginalPolicy},
		{"", jetstream.ReplayInstantPolicy},
	}
	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			c := config.Consumer{Name: "c1", ReplayPolicy: tt.input}
			cfg, err := buildConsumerConfig(c)
			if err != nil {
				t.Fatalf("buildConsumerConfig: %v", err)
			}
			if cfg.ReplayPolicy != tt.want {
				t.Errorf("expected ReplayPolicy %v, got %v", tt.want, cfg.ReplayPolicy)
			}
		})
	}
}

func TestBuildConsumerConfigUnknownReplayPolicy(t *testing.T) {
	c := config.Consumer{Name: "c1", ReplayPolicy: "bad"}
	_, err := buildConsumerConfig(c)
	if err == nil {
		t.Fatal("expected error for unknown replay policy")
	}
}

func TestBuildConsumerConfigAckWaitParseError(t *testing.T) {
	c := config.Consumer{Name: "c1", AckWait: "bad"}
	_, err := buildConsumerConfig(c)
	if err == nil {
		t.Fatal("expected error for invalid ack_wait")
	}
}

func TestBuildConsumerConfigPointerFields(t *testing.T) {
	maxDeliver := 5
	maxAckPending := 100
	c := config.Consumer{
		Name:          "c1",
		MaxDeliver:    &maxDeliver,
		MaxAckPending: &maxAckPending,
	}
	cfg, err := buildConsumerConfig(c)
	if err != nil {
		t.Fatalf("buildConsumerConfig: %v", err)
	}
	if cfg.MaxDeliver != 5 {
		t.Errorf("expected MaxDeliver 5, got %d", cfg.MaxDeliver)
	}
	if cfg.MaxAckPending != 100 {
		t.Errorf("expected MaxAckPending 100, got %d", cfg.MaxAckPending)
	}
}

func TestBuildConsumerConfigInactiveThresholdValid(t *testing.T) {
	c := config.Consumer{Name: "c1", InactiveThreshold: "5m"}
	cfg, err := buildConsumerConfig(c)
	if err != nil {
		t.Fatalf("buildConsumerConfig: %v", err)
	}
	if cfg.InactiveThreshold != 5*60*1e9 {
		t.Errorf("expected InactiveThreshold 5m, got %v", cfg.InactiveThreshold)
	}
}

func TestBuildConsumerConfigInactiveThresholdParseError(t *testing.T) {
	c := config.Consumer{Name: "c1", InactiveThreshold: "bad"}
	_, err := buildConsumerConfig(c)
	if err == nil {
		t.Fatal("expected error for invalid inactive_threshold")
	}
}

func TestBuildConsumerConfigDescription(t *testing.T) {
	c := config.Consumer{Name: "c1", Description: "my consumer"}
	cfg, err := buildConsumerConfig(c)
	if err != nil {
		t.Fatalf("buildConsumerConfig: %v", err)
	}
	if cfg.Description != "my consumer" {
		t.Errorf("expected description 'my consumer', got %q", cfg.Description)
	}
}

// --- ensureStream error path coverage ---

func TestEnsureStreamBuildConfigError(t *testing.T) {
	mock := newMockJetStream()
	stream := config.Stream{
		Name:      "T",
		Subjects:  []string{"t"},
		Retention: "invalid",
	}
	p := New(mock, &config.Config{})
	err := p.ensureStream(context.Background(), stream, "update")
	if err == nil {
		t.Fatal("expected error from buildStreamConfig")
	}
}

func TestEnsureStreamNonNotFoundError(t *testing.T) {
	mock := newMockJetStream()
	mock.streamErr = errors.New("connection lost")

	stream := config.Stream{
		Name:     "T",
		Subjects: []string{"t"},
	}
	p := New(mock, &config.Config{})
	err := p.ensureStream(context.Background(), stream, "update")
	if err == nil {
		t.Fatal("expected error from Stream()")
	}
	if err.Error() != "connection lost" {
		t.Errorf("expected 'connection lost', got %q", err.Error())
	}
}

func TestEnsureStreamStorageMismatchWarning(t *testing.T) {
	mock := newMockJetStream()
	mock.streams["T"] = jetstream.StreamConfig{
		Name:      "T",
		Subjects:  []string{"t"},
		Storage:   jetstream.FileStorage,
		Retention: jetstream.LimitsPolicy,
	}

	stream := config.Stream{
		Name:     "T",
		Subjects: []string{"t"},
		Storage:  "memory",
	}

	p := New(mock, &config.Config{})
	if err := p.ensureStream(context.Background(), stream, "update"); err != nil {
		t.Fatalf("ensureStream: %v", err)
	}

	// Verify the update carried forward the existing storage
	updated := mock.streams["T"]
	if updated.Storage != jetstream.FileStorage {
		t.Errorf("expected storage to remain FileStorage, got %v", updated.Storage)
	}
}

func TestEnsureStreamRetentionMismatchWarning(t *testing.T) {
	mock := newMockJetStream()
	mock.streams["T"] = jetstream.StreamConfig{
		Name:      "T",
		Subjects:  []string{"t"},
		Storage:   jetstream.FileStorage,
		Retention: jetstream.LimitsPolicy,
	}

	stream := config.Stream{
		Name:      "T",
		Subjects:  []string{"t"},
		Retention: "interest",
	}

	p := New(mock, &config.Config{})
	if err := p.ensureStream(context.Background(), stream, "update"); err != nil {
		t.Fatalf("ensureStream: %v", err)
	}

	updated := mock.streams["T"]
	if updated.Retention != jetstream.LimitsPolicy {
		t.Errorf("expected retention to remain LimitsPolicy, got %v", updated.Retention)
	}
}

func TestEnsureStreamDenyDeleteMismatchWarning(t *testing.T) {
	mock := newMockJetStream()
	mock.streams["T"] = jetstream.StreamConfig{
		Name:       "T",
		Subjects:   []string{"t"},
		Storage:    jetstream.FileStorage,
		Retention:  jetstream.LimitsPolicy,
		DenyDelete: false,
	}

	denyDelete := true
	stream := config.Stream{
		Name:       "T",
		Subjects:   []string{"t"},
		DenyDelete: &denyDelete,
	}

	p := New(mock, &config.Config{})
	if err := p.ensureStream(context.Background(), stream, "update"); err != nil {
		t.Fatalf("ensureStream: %v", err)
	}

	updated := mock.streams["T"]
	if updated.DenyDelete != false {
		t.Error("expected DenyDelete to remain false (carried forward from existing)")
	}
}

func TestEnsureStreamDenyPurgeMismatchWarning(t *testing.T) {
	mock := newMockJetStream()
	mock.streams["T"] = jetstream.StreamConfig{
		Name:      "T",
		Subjects:  []string{"t"},
		Storage:   jetstream.FileStorage,
		Retention: jetstream.LimitsPolicy,
		DenyPurge: false,
	}

	denyPurge := true
	stream := config.Stream{
		Name:      "T",
		Subjects:  []string{"t"},
		DenyPurge: &denyPurge,
	}

	p := New(mock, &config.Config{})
	if err := p.ensureStream(context.Background(), stream, "update"); err != nil {
		t.Fatalf("ensureStream: %v", err)
	}

	updated := mock.streams["T"]
	if updated.DenyPurge != false {
		t.Error("expected DenyPurge to remain false (carried forward from existing)")
	}
}

func TestEnsureStreamUpdateError(t *testing.T) {
	mock := newMockJetStream()
	mock.streams["T"] = jetstream.StreamConfig{
		Name:     "T",
		Subjects: []string{"t"},
		Storage:  jetstream.FileStorage,
	}
	mock.updateStreamErr = errors.New("update denied")

	stream := config.Stream{
		Name:     "T",
		Subjects: []string{"t"},
	}

	p := New(mock, &config.Config{})
	err := p.ensureStream(context.Background(), stream, "update")
	if err == nil {
		t.Fatal("expected error from UpdateStream")
	}
	if err.Error() != "update denied" {
		t.Errorf("expected 'update denied', got %q", err.Error())
	}
}

// --- ensureConsumer error path coverage ---

func TestEnsureStreamUpdatePreservesUnsetMutableFields(t *testing.T) {
	mock := newMockJetStream()
	// Existing stream has non-default mutable fields the user is about to omit
	mock.streams["KEEP"] = jetstream.StreamConfig{
		Name:      "KEEP",
		Subjects:  []string{"keep.>"},
		Storage:   jetstream.FileStorage,
		Retention: jetstream.LimitsPolicy,
		Discard:   jetstream.DiscardNew,
		MaxMsgs:   1000,
		MaxBytes:  2048,
		Replicas:  3,
	}

	// User submits a config that omits all those fields — we must not silently
	// reset them to their Go zero values on the update path.
	stream := config.Stream{
		Name:     "KEEP",
		Subjects: []string{"keep.>"},
	}

	p := New(mock, &config.Config{})
	if err := p.ensureStream(context.Background(), stream, "update"); err != nil {
		t.Fatalf("ensureStream: %v", err)
	}

	updated := mock.streams["KEEP"]
	if updated.Discard != jetstream.DiscardNew {
		t.Errorf("expected Discard preserved as DiscardNew, got %v", updated.Discard)
	}
	if updated.MaxMsgs != 1000 {
		t.Errorf("expected MaxMsgs preserved as 1000, got %d", updated.MaxMsgs)
	}
	if updated.MaxBytes != 2048 {
		t.Errorf("expected MaxBytes preserved as 2048, got %d", updated.MaxBytes)
	}
	if updated.Replicas != 3 {
		t.Errorf("expected Replicas preserved as 3, got %d", updated.Replicas)
	}
}

func TestEnsureConsumerBuildConfigError(t *testing.T) {
	mock := newMockJetStream()

	consumer := config.Consumer{
		Name:           "c1",
		DeliverSubject: "push-not-supported",
	}

	p := New(mock, &config.Config{})
	err := p.ensureConsumer(context.Background(), "T", consumer)
	if err == nil {
		t.Fatal("expected error from buildConsumerConfig")
	}
}
