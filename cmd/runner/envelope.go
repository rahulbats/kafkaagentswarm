package main

import (
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"strings"
)

// envelope is the message format agents pass between Kafka topics. It
// carries a correlation ID so a fan-in node can match up messages that
// originated from the same upstream event, and the name of the node that
// produced it so a fan-in node knows which dependency each message
// satisfies.
type envelope struct {
	CorrelationID string         `json:"correlation_id"`
	Source        string         `json:"source,omitempty"`
	Data          map[string]any `json:"data"`
}

// parseOrWrapEnvelope accepts either an envelope already produced by an
// upstream node, or a raw JSON payload - e.g. the first message a DAG root
// node receives from outside the swarm - which it wraps into a fresh
// envelope with a newly minted correlation ID.
func parseOrWrapEnvelope(raw []byte) envelope {
	var env envelope
	if err := json.Unmarshal(raw, &env); err == nil && env.CorrelationID != "" && env.Data != nil {
		return env
	}

	var data map[string]any
	if err := json.Unmarshal(raw, &data); err != nil {
		data = map[string]any{"raw": string(raw)}
	}
	return envelope{CorrelationID: newCorrelationID(data), Data: data}
}

// newCorrelationID picks a stable ID out of a fresh payload if it looks like
// it has one (an explicit correlation_id/id field, or anything named
// *_id), falling back to a random one.
func newCorrelationID(data map[string]any) string {
	for _, key := range []string{"correlation_id", "id"} {
		if s, ok := data[key].(string); ok && s != "" {
			return s
		}
	}
	for k, v := range data {
		if strings.HasSuffix(k, "_id") {
			if s, ok := v.(string); ok && s != "" {
				return s
			}
		}
	}
	buf := make([]byte, 8)
	_, _ = rand.Read(buf)
	return hex.EncodeToString(buf)
}
