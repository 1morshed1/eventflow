// Package kafka wraps segmentio/kafka-go with just enough for this project: a
// Publisher used by the relay and a Consumer used by projectors. Messages are the
// JSON-encoded eventstore.Event; the message key is the aggregate id so that all
// events for one aggregate land in the same partition and stay ordered.
package kafka

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"strconv"
	"time"

	"github.com/1morshed1/eventflow/internal/eventstore"
	kgo "github.com/segmentio/kafka-go"
)

// EnsureTopics creates the given topics (1 partition, replication 1) if they do
// not already exist.
//
// Why this matters: a consumer group that joins BEFORE its topic exists is
// assigned zero partitions and, without partition-change watching, never picks the
// topic up when it later appears - it sits idle forever. Creating topics up front
// (and enabling WatchPartitionChanges on the reader) removes that startup race.
func EnsureTopics(ctx context.Context, brokers []string, topics ...string) error {
	conn, err := kgo.DialContext(ctx, "tcp", brokers[0])
	if err != nil {
		return fmt.Errorf("dial kafka: %w", err)
	}
	defer conn.Close()

	controller, err := conn.Controller()
	if err != nil {
		return fmt.Errorf("get controller: %w", err)
	}
	ctrlConn, err := kgo.DialContext(ctx, "tcp", net.JoinHostPort(controller.Host, strconv.Itoa(controller.Port)))
	if err != nil {
		return fmt.Errorf("dial controller: %w", err)
	}
	defer ctrlConn.Close()

	configs := make([]kgo.TopicConfig, 0, len(topics))
	for _, t := range topics {
		configs = append(configs, kgo.TopicConfig{Topic: t, NumPartitions: 1, ReplicationFactor: 1})
	}
	if err := ctrlConn.CreateTopics(configs...); err != nil && !errors.Is(err, kgo.TopicAlreadyExists) {
		return fmt.Errorf("create topics: %w", err)
	}
	return nil
}

// TopicForAggregate maps an aggregate type to its Kafka topic. Splitting by
// aggregate type lets consumers subscribe only to what they care about.
func TopicForAggregate(aggregateType string) string {
	switch aggregateType {
	case "Order":
		return "order-events"
	case "Payment":
		return "payment-events"
	case "Inventory":
		return "inventory-events"
	default:
		return "misc-events"
	}
}

// Publisher writes events to Kafka. It is safe for concurrent use.
type Publisher struct {
	writer *kgo.Writer
}

// NewPublisher creates a Publisher against the given brokers. We use Hash
// balancing on the message key (aggregate id) to guarantee per-aggregate ordering.
func NewPublisher(brokers []string) *Publisher {
	return &Publisher{
		writer: &kgo.Writer{
			Addr:                   kgo.TCP(brokers...),
			Balancer:               &kgo.Hash{},
			AllowAutoTopicCreation: true,
			RequiredAcks:           kgo.RequireAll, // durability: wait for all replicas
			BatchTimeout:           10 * time.Millisecond,
		},
	}
}

// Publish sends a batch of events to their topics. Because the relay is
// at-least-once, a consumer may see the same event twice after a crash; projectors
// must therefore be idempotent (they are - see the checkpoint logic).
func (p *Publisher) Publish(ctx context.Context, events []eventstore.Event) error {
	if len(events) == 0 {
		return nil
	}
	msgs := make([]kgo.Message, 0, len(events))
	for _, e := range events {
		payload, err := json.Marshal(e)
		if err != nil {
			return fmt.Errorf("marshal event: %w", err)
		}
		msgs = append(msgs, kgo.Message{
			Topic: TopicForAggregate(e.AggregateType),
			Key:   []byte(e.AggregateID.String()),
			Value: payload,
		})
	}
	if err := p.writer.WriteMessages(ctx, msgs...); err != nil {
		return fmt.Errorf("write messages: %w", err)
	}
	return nil
}

// Close flushes and closes the underlying writer.
func (p *Publisher) Close() error { return p.writer.Close() }

// Consumer reads events from one topic as part of a consumer group. Kafka tracks
// the group's committed offset so each projector resumes where it left off.
type Consumer struct {
	reader *kgo.Reader
}

// NewConsumer subscribes group `groupID` to `topic`. StartOffset FirstOffset means
// a brand-new group starts from the beginning of the log - which is exactly how you
// rebuild a read model from scratch: use a fresh group id.
func NewConsumer(brokers []string, topic, groupID string) *Consumer {
	return &Consumer{
		reader: kgo.NewReader(kgo.ReaderConfig{
			Brokers:     brokers,
			Topic:       topic,
			GroupID:     groupID,
			StartOffset: kgo.FirstOffset,
			MinBytes:    1,
			MaxBytes:    10e6,
			MaxWait:     500 * time.Millisecond,
			// Re-trigger a group rebalance when the topic's partitions change (e.g.
			// the topic is created after the consumer starts), so we never sit idle
			// with zero partitions assigned.
			WatchPartitionChanges: true,
			PartitionWatchInterval: 2 * time.Second,
		}),
	}
}

// Handler processes one event. Returning an error stops consumption (the offset is
// not committed), so the event is redelivered - keep handlers idempotent.
type Handler func(ctx context.Context, e eventstore.Event) error

// Run consumes messages until ctx is cancelled, decoding each into an event and
// passing it to h. It commits the offset only after h succeeds, giving
// at-least-once delivery.
func (c *Consumer) Run(ctx context.Context, h Handler) error {
	for {
		msg, err := c.reader.FetchMessage(ctx)
		if err != nil {
			if ctx.Err() != nil {
				return nil // clean shutdown
			}
			return fmt.Errorf("fetch message: %w", err)
		}

		var e eventstore.Event
		if err := json.Unmarshal(msg.Value, &e); err != nil {
			// A poison message we can never decode: log-and-skip by committing, so
			// it does not block the partition forever.
			if err := c.reader.CommitMessages(ctx, msg); err != nil {
				return fmt.Errorf("commit poison message: %w", err)
			}
			continue
		}

		if err := h(ctx, e); err != nil {
			return fmt.Errorf("handle event %d: %w", e.ID, err)
		}
		if err := c.reader.CommitMessages(ctx, msg); err != nil {
			return fmt.Errorf("commit message: %w", err)
		}
	}
}

// Close closes the reader.
func (c *Consumer) Close() error { return c.reader.Close() }
