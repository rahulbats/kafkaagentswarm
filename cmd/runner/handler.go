package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"sync"

	"github.com/IBM/sarama"
)

type agentHandler struct {
	cfg      config
	producer sarama.SyncProducer
	tools    map[string]toolBinding
	llm      *llmClient

	mu    sync.Mutex
	joins map[string]*joinState
}

func (h *agentHandler) Setup(sarama.ConsumerGroupSession) error   { return nil }
func (h *agentHandler) Cleanup(sarama.ConsumerGroupSession) error { return nil }

func (h *agentHandler) ConsumeClaim(sess sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	for msg := range claim.Messages() {
		env := parseOrWrapEnvelope(msg.Value)
		log.Printf("[%s] received correlation=%s source=%q topic=%s",
			h.cfg.nodeName, env.CorrelationID, env.Source, msg.Topic)

		merged, ready, err := h.join(env)
		if err != nil {
			// The contribution isn't durably recorded anywhere yet - exit
			// without marking the message so Kubernetes restarts this pod
			// and Kafka redelivers it.
			log.Fatalf("[%s] failed to durably record contribution for correlation=%s: %v",
				h.cfg.nodeName, env.CorrelationID, err)
		}

		// The contribution is now safe - either it's durably recorded in
		// JOIN_STATE_TOPIC, or (for a non-join node) there was nothing to
		// record beyond the message itself. Either way it's fine to commit
		// this offset even though the actual task (agent loop + publish)
		// hasn't run yet: if that fails, completeCorrelation exits the
		// process, and on restart a fan-in join is recovered from
		// JOIN_STATE_TOPIC (see hydrateJoinState in main), while a
		// straight-through node's task is small enough to just be
		// re-triggered by whatever originally produced this message.
		sess.MarkMessage(msg, "")

		if ready {
			h.completeCorrelation(env.CorrelationID, merged)
		}
	}
	return nil
}

// join durably records an incoming envelope's contribution to a fan-in join
// (a node with more than one DEPENDS_ON entry) and reports the merged data
// once every dependency has reported in for that correlation ID. A node
// with 0 or 1 dependency needs no join at all - env.Data is already
// everything there is.
//
// Each contribution is written to JOIN_STATE_TOPIC - a Kafka-compacted
// changelog topic, the same pattern Kafka Streams uses to back a
// groupBy/aggregate state store - before this returns successfully. That
// has to happen promptly per contribution, not deferred until the whole
// join completes: a partition's consumer offset is one monotonic cursor, so
// an early, still-incomplete contribution can't be left uncommitted while a
// later, unrelated correlation ID's contribution on the same partition gets
// committed - that would either stall the partition indefinitely or
// silently skip past (and lose) the earlier one.
func (h *agentHandler) join(env envelope) (map[string]any, bool, error) {
	if len(h.cfg.dependsOn) <= 1 {
		return env.Data, true, nil
	}

	h.mu.Lock()
	defer h.mu.Unlock()

	st, ok := h.joins[env.CorrelationID]
	if !ok {
		st = &joinState{Received: map[string]map[string]any{}}
		h.joins[env.CorrelationID] = st
	}
	st.Received[env.Source] = env.Data

	if err := h.writeJoinState(env.CorrelationID, st); err != nil {
		return nil, false, fmt.Errorf("persisting join state: %w", err)
	}

	if len(st.Received) < len(h.cfg.dependsOn) {
		log.Printf("[%s] fan-in waiting on correlation=%s: have %d/%d",
			h.cfg.nodeName, env.CorrelationID, len(st.Received), len(h.cfg.dependsOn))
		return nil, false, nil
	}

	return mergeJoinedData(st), true, nil
}

// completeCorrelation runs the agent loop and publishes its result for a
// correlation ID whose join (if any) is complete. Called either right after
// the triggering message arrives, or during startup recovery (main.go) for
// a join that completed before an earlier crash but was never published.
func (h *agentHandler) completeCorrelation(correlationID string, data map[string]any) {
	out, ok := h.agentLoop(context.Background(), data)
	if !ok {
		log.Fatalf("[%s] agent loop did not complete for correlation=%s - exiting so Kubernetes restarts "+
			"this pod; any join state is already durably recorded and will be retried",
			h.cfg.nodeName, correlationID)
	}

	if err := h.publish(correlationID, out); err != nil {
		log.Fatalf("[%s] failed to publish result for correlation=%s: %v", h.cfg.nodeName, correlationID, err)
	}

	if h.cfg.joinStateTopic == "" {
		return
	}
	h.mu.Lock()
	delete(h.joins, correlationID)
	h.mu.Unlock()
	if err := h.tombstoneJoinState(correlationID); err != nil {
		log.Printf("[%s] failed to tombstone join state for correlation=%s (non-fatal): %v",
			h.cfg.nodeName, correlationID, err)
	}
}

// publish sends the node's output to every downstream input topic. A node
// with no OUTPUT_TOPICS is a terminal sink (e.g. the last Worker before a
// HumanInTheLoop gate still publishes into the gate's own input topic - a
// true dead end just logs the final result).
func (h *agentHandler) publish(correlationID string, data map[string]any) error {
	if len(h.cfg.outputTopics) == 0 {
		log.Printf("[%s] reached a terminal node for correlation=%s: %v", h.cfg.nodeName, correlationID, data)
		return nil
	}

	payload, err := json.Marshal(envelope{CorrelationID: correlationID, Source: h.cfg.nodeName, Data: data})
	if err != nil {
		return fmt.Errorf("marshaling outgoing envelope: %w", err)
	}

	for _, topic := range h.cfg.outputTopics {
		if _, _, err := h.producer.SendMessage(&sarama.ProducerMessage{
			Topic: topic,
			Key:   sarama.StringEncoder(correlationID),
			Value: sarama.ByteEncoder(payload),
		}); err != nil {
			return fmt.Errorf("publishing to %s: %w", topic, err)
		}
	}
	return nil
}
