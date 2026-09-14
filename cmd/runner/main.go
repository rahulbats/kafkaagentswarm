package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"strings"
	"syscall"

	"github.com/IBM/sarama"
)

func main() {
	brokers := strings.Split(os.Getenv("KAFKA_BROKERS"), ",")
	inputTopic := os.Getenv("INPUT_TOPIC")
	outputTopic := os.Getenv("OUTPUT_TOPIC")
	instructions := os.Getenv("INSTRUCTIONS")
	allowedTools := os.Getenv("ALLOWED_TOOLS")

	fmt.Printf("Worker booted.\n  Input: %s\n  Output: %s\n  Tools: %s\n  Instructions: %s\n",
		inputTopic, outputTopic, allowedTools, instructions)

	config := sarama.NewConfig()
	config.Version = sarama.V3_0_0_0
	config.Consumer.Offsets.Initial = sarama.OffsetOldest

	group, err := sarama.NewConsumerGroup(brokers, os.Getenv("NODE_NAME")+"-group", config)
	if err != nil {
		panic(err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	handler := &ConsumerHandler{outputTopic: outputTopic}

	go func() {
		for {
			if err := group.Consume(ctx, []string{inputTopic}, handler); err != nil {
				fmt.Printf("Error consuming: %v\n", err)
			}
		}
	}()

	sig := make(chan os.Signal, 1)
	signal.Notify(sig, syscall.SIGINT, syscall.SIGTERM)
	<-sig
	cancel()
}

type ConsumerHandler struct {
	outputTopic string
}

func (h *ConsumerHandler) Setup(sarama.ConsumerGroupSession) error   { return nil }
func (h *ConsumerHandler) Cleanup(sarama.ConsumerGroupSession) error { return nil }
func (h *ConsumerHandler) ConsumeClaim(sess sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	for msg := range claim.Messages() {
		fmt.Printf("Received payload on %s: %s\n", msg.Topic, string(msg.Value))
		// 1. Fetch tool definitions from MCP Server via SSE/HTTP
		// 2. Filter using ALLOWED_TOOLS
		// 3. Process LLM step & call MCP tool
		// 4. Emit output to h.outputTopic
		sess.MarkMessage(msg, "")
	}
	return nil
}
