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

	createStreamCalls         int
	updateStreamCalls         int
	createOrUpdateConsumerCalls int

	createStreamErr         error
	updateStreamErr         error
	streamErr               error
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

func TestEnsureStreamCreate(t *testing.T) {
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
	if err := p.ensureStream(context.Background(), stream); err != nil {
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
	if err := p.ensureStream(context.Background(), stream); err != nil {
		t.Fatalf("ensureStream: %v", err)
	}

	if mock.createStreamCalls != 0 {
		t.Errorf("expected 0 CreateStream calls, got %d", mock.createStreamCalls)
	}
	if mock.updateStreamCalls != 1 {
		t.Errorf("expected 1 UpdateStream call, got %d", mock.updateStreamCalls)
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
