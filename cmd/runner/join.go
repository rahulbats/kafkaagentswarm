package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"

	"github.com/IBM/sarama"
)

// joinState is the durable record of a fan-in node's progress joining one
// correlation ID's contributions: each dependency's data, keyed by the
// upstream node name that produced it.
type joinState struct {
	Received map[string]map[string]any `json:"received"`
}

// mergeJoinedData flattens a completed join into one data map: each
// source's payload is kept under its own name (so nothing collides) and
// also flattened to the top level as a convenience for simple argument
// matching against tool schemas.
func mergeJoinedData(st *joinState) map[string]any {
	merged := map[string]any{}
	for source, data := range st.Received {
		merged[source] = data
		for k, v := range data {
			if _, exists := merged[k]; !exists {
				merged[k] = v
			}
		}
	}
	return merged
}

// writeJoinState durably upserts a correlation ID's join progress to
// JOIN_STATE_TOPIC - a Kafka-compacted "changelog" topic, the same pattern
// Kafka Streams uses to back a groupBy/aggregate state store - before the
// caller is allowed to mark the triggering input message as consumed.
func (h *agentHandler) writeJoinState(correlationID string, st *joinState) error {
	payload, err := json.Marshal(st)
	if err != nil {
		return fmt.Errorf("marshaling join state: %w", err)
	}
	_, _, err = h.producer.SendMessage(&sarama.ProducerMessage{
		Topic: h.cfg.joinStateTopic,
		Key:   sarama.StringEncoder(correlationID),
		Value: sarama.ByteEncoder(payload),
	})
	return err
}

// tombstoneJoinState deletes a completed correlation ID's entry once it's
// been fully processed and published, so the compacted topic doesn't grow
// forever. It's cleanup, not a correctness requirement: if it fails, the
// worst case is that a future restart re-processes (and re-publishes) an
// already-completed join - the standard at-least-once caveat that applies
// everywhere else in this pipeline too.
func (h *agentHandler) tombstoneJoinState(correlationID string) error {
	_, _, err := h.producer.SendMessage(&sarama.ProducerMessage{
		Topic: h.cfg.joinStateTopic,
		Key:   sarama.StringEncoder(correlationID),
		Value: nil, // a nil value is a Kafka tombstone: compaction drops the key entirely
	})
	return err
}

// hydrateJoinState rebuilds a fan-in node's in-memory join buffer from its
// compacted changelog topic at startup - the same recovery Kafka Streams
// does when it rebuilds a state store from its changelog. Returns an empty,
// ready-to-use map for a node that isn't a fan-in join (topic == "").
func hydrateJoinState(ctx context.Context, brokers []string, topic string) (map[string]*joinState, error) {
	joins := map[string]*joinState{}
	if topic == "" {
		return joins, nil
	}

	saramaCfg := sarama.NewConfig()
	saramaCfg.Version = sarama.V3_0_0_0

	saramaClient, err := sarama.NewClient(brokers, saramaCfg)
	if err != nil {
		return nil, fmt.Errorf("connecting to kafka: %w", err)
	}
	defer func() { _ = saramaClient.Close() }()

	consumer, err := sarama.NewConsumerFromClient(saramaClient)
	if err != nil {
		return nil, fmt.Errorf("creating hydration consumer: %w", err)
	}
	defer func() { _ = consumer.Close() }()

	partitions, err := saramaClient.Partitions(topic)
	if err != nil {
		return nil, fmt.Errorf("listing partitions for %s: %w", topic, err)
	}

	for _, partition := range partitions {
		if err := hydratePartition(ctx, saramaClient, consumer, topic, partition, joins); err != nil {
			return nil, err
		}
	}

	log.Printf("Hydrated %d in-progress join(s) from %s", len(joins), topic)
	return joins, nil
}

func hydratePartition(ctx context.Context, saramaClient sarama.Client, consumer sarama.Consumer,
	topic string, partition int32, joins map[string]*joinState) error {
	newest, err := saramaClient.GetOffset(topic, partition, sarama.OffsetNewest)
	if err != nil {
		return fmt.Errorf("getting newest offset for %s/%d: %w", topic, partition, err)
	}
	if newest == 0 {
		return nil // empty partition, nothing to replay
	}

	pc, err := consumer.ConsumePartition(topic, partition, sarama.OffsetOldest)
	if err != nil {
		return fmt.Errorf("consuming %s/%d: %w", topic, partition, err)
	}
	defer func() { _ = pc.Close() }()

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case msg := <-pc.Messages():
			applyHydratedRecord(joins, msg)
			if msg.Offset+1 >= newest {
				return nil
			}
		}
	}
}

func applyHydratedRecord(joins map[string]*joinState, msg *sarama.ConsumerMessage) {
	key := string(msg.Key)
	if len(msg.Value) == 0 {
		delete(joins, key) // tombstone
		return
	}
	var st joinState
	if err := json.Unmarshal(msg.Value, &st); err != nil {
		log.Printf("Skipping unreadable join-state record for %q: %v", key, err)
		return
	}
	joins[key] = &st
}
