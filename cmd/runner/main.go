// Command runner is the generic agent worker image (Dockerfile.runner). The
// controller deploys one of these per Worker node in an AgentSwarm's DAG.
// Each instance is an agent: it takes a task off its node's input topic,
// runs it through a recursive LLM+tool-use loop (instructions + whatever
// MCP tools the node is allowed to call) until the LLM produces a final
// answer, and publishes that to every downstream node's input topic. A node
// with more than one dependsOn entry waits for a contribution from each
// dependency (see join.go) before running the loop.
package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"os/signal"
	"strings"
	"syscall"

	"github.com/IBM/sarama"
)

// config is everything the controller wires into the container's
// environment (see internal/controller/agentswarm_controller.go).
type config struct {
	brokers        []string
	nodeName       string
	inputTopic     string
	outputTopics   []string
	dependsOn      []string
	joinStateTopic string // set only when len(dependsOn) > 1
	allowedTools   []string
	instructions   string
	mcpEndpoints   map[string]string // name -> base URL
	llmBaseURL     string
	llmModel       string
	llmAPIKey      string
}

func loadConfig() config {
	mcpEndpoints := map[string]string{}
	for _, pair := range splitCSV(os.Getenv("MCP_ENDPOINTS")) {
		name, url, ok := strings.Cut(pair, "=")
		if ok {
			mcpEndpoints[name] = url
		}
	}

	return config{
		brokers:        splitCSV(os.Getenv("KAFKA_BROKERS")),
		nodeName:       os.Getenv("NODE_NAME"),
		inputTopic:     os.Getenv("INPUT_TOPIC"),
		outputTopics:   splitCSV(os.Getenv("OUTPUT_TOPICS")),
		dependsOn:      splitCSV(os.Getenv("DEPENDS_ON")),
		joinStateTopic: os.Getenv("JOIN_STATE_TOPIC"),
		allowedTools:   splitCSV(os.Getenv("ALLOWED_TOOLS")),
		instructions:   os.Getenv("INSTRUCTIONS"),
		mcpEndpoints:   mcpEndpoints,
		llmBaseURL:     os.Getenv("LLM_BASE_URL"),
		llmModel:       os.Getenv("LLM_MODEL"),
		llmAPIKey:      os.Getenv("LLM_API_KEY"),
	}
}

func splitCSV(s string) []string {
	if s == "" {
		return nil
	}
	parts := strings.Split(s, ",")
	out := make([]string, 0, len(parts))
	for _, p := range parts {
		if p = strings.TrimSpace(p); p != "" {
			out = append(out, p)
		}
	}
	return out
}

func main() {
	cfg := loadConfig()
	fmt.Printf("Agent booted.\n  Node: %s\n  Input: %s\n  Outputs: %s\n  DependsOn: %s\n"+
		"  Tools: %s\n  LLM: %s (%s)\n  Instructions: %s\n",
		cfg.nodeName, cfg.inputTopic, strings.Join(cfg.outputTopics, ","),
		strings.Join(cfg.dependsOn, ","), strings.Join(cfg.allowedTools, ","),
		cfg.llmBaseURL, cfg.llmModel, cfg.instructions)

	saramaCfg := sarama.NewConfig()
	saramaCfg.Version = sarama.V3_0_0_0
	saramaCfg.Consumer.Offsets.Initial = sarama.OffsetOldest
	saramaCfg.Producer.Return.Successes = true

	producer, err := sarama.NewSyncProducer(cfg.brokers, saramaCfg)
	if err != nil {
		log.Fatalf("Failed to create Kafka producer: %v", err)
	}
	defer func() {
		if cerr := producer.Close(); cerr != nil {
			log.Printf("Failed to close Kafka producer: %v", cerr)
		}
	}()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	tools, closeTools := connectMCPTools(ctx, cfg)
	defer closeTools()

	// Fan-in nodes rebuild their partial-join state from the durable
	// changelog topic before touching the input topic at all, so a join
	// that completed just before an earlier crash - but was never
	// processed/published - gets picked up here instead of being stuck
	// waiting on a rebalance that (from Kafka's point of view) already
	// happened.
	joins, err := hydrateJoinState(ctx, cfg.brokers, cfg.joinStateTopic)
	if err != nil {
		log.Fatalf("Failed to hydrate join state from %s: %v", cfg.joinStateTopic, err)
	}

	handler := &agentHandler{
		cfg:      cfg,
		producer: producer,
		tools:    tools,
		joins:    joins,
	}
	if cfg.llmBaseURL != "" {
		handler.llm = newLLMClient(cfg)
	}

	for correlationID, st := range joins {
		if len(st.Received) >= len(cfg.dependsOn) {
			log.Printf("[%s] recovering a join that completed before an earlier restart: correlation=%s",
				cfg.nodeName, correlationID)
			handler.completeCorrelation(correlationID, mergeJoinedData(st))
		}
	}

	group, err := sarama.NewConsumerGroup(cfg.brokers, cfg.nodeName+"-group", saramaCfg)
	if err != nil {
		log.Fatalf("Failed to create Kafka consumer group: %v", err)
	}

	go func() {
		for {
			if err := group.Consume(ctx, []string{cfg.inputTopic}, handler); err != nil {
				log.Printf("Error consuming: %v", err)
			}
			if ctx.Err() != nil {
				return
			}
		}
	}()

	sig := make(chan os.Signal, 1)
	signal.Notify(sig, syscall.SIGINT, syscall.SIGTERM)
	<-sig
	cancel()
	if err := group.Close(); err != nil {
		log.Printf("Failed to close consumer group: %v", err)
	}
}
