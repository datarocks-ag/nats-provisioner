//go:build integration

package provisioner_test

import (
	"context"
	"log"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/testcontainers/testcontainers-go"
	natsmodule "github.com/testcontainers/testcontainers-go/modules/nats"

	"nats-provisioner/internal/config"
	"nats-provisioner/internal/provisioner"
)

func setupNATS(t *testing.T) (jetstream.JetStream, *nats.Conn, func()) {
	t.Helper()
	ctx := context.Background()

	natsContainer, err := natsmodule.Run(ctx,
		"nats:2.12-alpine",
	)
	if err != nil {
		t.Fatalf("failed to start nats container: %v", err)
	}

	connStr, err := natsContainer.ConnectionString(ctx)
	if err != nil {
		t.Fatalf("failed to get connection string: %v", err)
	}

	nc, err := nats.Connect(connStr)
	if err != nil {
		t.Fatalf("failed to connect to nats: %v", err)
	}

	js, err := jetstream.New(nc)
	if err != nil {
		nc.Close()
		t.Fatalf("failed to create jetstream context: %v", err)
	}

	cleanup := func() {
		nc.Close()
		if err := testcontainers.TerminateContainer(natsContainer); err != nil {
			log.Printf("failed to terminate container: %v", err)
		}
	}

	return js, nc, cleanup
}

func writeTestConfig(t *testing.T, yaml string) string {
	t.Helper()
	dir := t.TempDir()
	path := filepath.Join(dir, "config.yaml")
	if err := os.WriteFile(path, []byte(yaml), 0644); err != nil {
		t.Fatal(err)
	}
	return path
}

func TestIntegrationFullProvisioning(t *testing.T) {
	js, _, cleanup := setupNATS(t)
	defer cleanup()

	configYAML := `
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
	cfgPath := writeTestConfig(t, configYAML)
	cfg, err := config.Load(cfgPath)
	if err != nil {
		t.Fatalf("failed to load config: %v", err)
	}

	ctx := context.Background()

	// First run
	p := provisioner.New(js, cfg)
	if err := p.Run(ctx); err != nil {
		t.Fatalf("first provisioning run failed: %v", err)
	}

	// Verify stream exists
	stream, err := js.Stream(ctx, "ORDERS")
	if err != nil {
		t.Fatalf("stream not found: %v", err)
	}
	info := stream.CachedInfo()
	if info.Config.Name != "ORDERS" {
		t.Errorf("expected stream name ORDERS, got %s", info.Config.Name)
	}

	// Verify consumer exists
	consumer, err := stream.Consumer(ctx, "order-processor")
	if err != nil {
		t.Fatalf("consumer not found: %v", err)
	}
	cInfo := consumer.CachedInfo()
	if cInfo.Config.Name != "order-processor" {
		t.Errorf("expected consumer name order-processor, got %s", cInfo.Config.Name)
	}
}

func TestIntegrationIdempotency(t *testing.T) {
	js, _, cleanup := setupNATS(t)
	defer cleanup()

	configYAML := `
streams:
  - name: "EVENTS"
    subjects: ["events.>"]
    consumers:
      - name: "event-handler"
        ack_policy: "explicit"
`
	cfgPath := writeTestConfig(t, configYAML)
	cfg, err := config.Load(cfgPath)
	if err != nil {
		t.Fatal(err)
	}

	ctx := context.Background()

	// First run
	p := provisioner.New(js, cfg)
	if err := p.Run(ctx); err != nil {
		t.Fatalf("first run: %v", err)
	}

	// Second run (idempotent)
	p2 := provisioner.New(js, cfg)
	if err := p2.Run(ctx); err != nil {
		t.Fatalf("second (idempotent) run: %v", err)
	}
}

func TestIntegrationPublishConsume(t *testing.T) {
	js, nc, cleanup := setupNATS(t)
	defer cleanup()

	configYAML := `
streams:
  - name: "MESSAGES"
    subjects: ["messages.>"]
    consumers:
      - name: "msg-reader"
        filter_subject: "messages.new"
        ack_policy: "explicit"
`
	cfgPath := writeTestConfig(t, configYAML)
	cfg, err := config.Load(cfgPath)
	if err != nil {
		t.Fatal(err)
	}

	ctx := context.Background()
	p := provisioner.New(js, cfg)
	if err := p.Run(ctx); err != nil {
		t.Fatal(err)
	}

	// Publish a message
	if err := nc.Publish("messages.new", []byte("hello")); err != nil {
		t.Fatalf("publish: %v", err)
	}
	nc.Flush()

	// Consume via JetStream consumer
	stream, err := js.Stream(ctx, "MESSAGES")
	if err != nil {
		t.Fatal(err)
	}
	consumer, err := stream.Consumer(ctx, "msg-reader")
	if err != nil {
		t.Fatal(err)
	}

	msg, err := consumer.Next(jetstream.FetchMaxWait(5 * time.Second))
	if err != nil {
		t.Fatalf("next: %v", err)
	}
	if string(msg.Data()) != "hello" {
		t.Errorf("expected 'hello', got %q", string(msg.Data()))
	}
	msg.Ack()
}

func TestIntegrationEmptyConfig(t *testing.T) {
	js, _, cleanup := setupNATS(t)
	defer cleanup()

	cfgPath := writeTestConfig(t, "{}")
	cfg, err := config.Load(cfgPath)
	if err != nil {
		t.Fatal(err)
	}

	p := provisioner.New(js, cfg)
	if err := p.Run(context.Background()); err != nil {
		t.Fatalf("empty config should succeed: %v", err)
	}
}
