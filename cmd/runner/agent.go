package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"maps"
	"net/http"
	"strings"
	"time"

	"github.com/mark3labs/mcp-go/mcp"
)

// maxAgentTurns caps how many LLM<->tool round trips a single task gets
// before the agent loop gives up. Without a cap a misbehaving model that
// never stops requesting tool calls would loop forever.
const maxAgentTurns = 8

// chatMessage, toolCall and toolSpec mirror the OpenAI chat-completions
// wire format (the de facto standard also implemented by Ollama, LM Studio,
// MLX-based servers, vLLM, and most hosted OpenAI-compatible providers), so
// this runner needs no provider SDK - just an HTTP POST.
type chatMessage struct {
	Role       string     `json:"role"`
	Content    string     `json:"content,omitempty"`
	ToolCalls  []toolCall `json:"tool_calls,omitempty"`
	ToolCallID string     `json:"tool_call_id,omitempty"`
}

type toolCall struct {
	ID       string           `json:"id"`
	Type     string           `json:"type"`
	Function toolCallFunction `json:"function"`
}

type toolCallFunction struct {
	Name      string `json:"name"`
	Arguments string `json:"arguments"` // a JSON object, encoded as a string
}

type toolSpec struct {
	Type     string       `json:"type"`
	Function toolFunction `json:"function"`
}

type toolFunction struct {
	Name        string         `json:"name"`
	Description string         `json:"description,omitempty"`
	Parameters  map[string]any `json:"parameters"`
}

type chatRequest struct {
	Model    string        `json:"model"`
	Messages []chatMessage `json:"messages"`
	Tools    []toolSpec    `json:"tools,omitempty"`
}

type chatResponse struct {
	Choices []struct {
		Message chatMessage `json:"message"`
	} `json:"choices"`
	Error *struct {
		Message string `json:"message"`
	} `json:"error,omitempty"`
}

// llmClient is a minimal OpenAI-compatible chat-completions client.
type llmClient struct {
	httpClient *http.Client
	baseURL    string
	model      string
	apiKey     string
}

func newLLMClient(cfg config) *llmClient {
	return &llmClient{
		httpClient: &http.Client{Timeout: 120 * time.Second},
		baseURL:    strings.TrimRight(cfg.llmBaseURL, "/"),
		model:      cfg.llmModel,
		apiKey:     cfg.llmAPIKey,
	}
}

func (c *llmClient) chat(ctx context.Context, messages []chatMessage, tools []toolSpec) (chatMessage, error) {
	body, err := json.Marshal(chatRequest{Model: c.model, Messages: messages, Tools: tools})
	if err != nil {
		return chatMessage{}, fmt.Errorf("marshaling chat request: %w", err)
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.baseURL+"/chat/completions", bytes.NewReader(body))
	if err != nil {
		return chatMessage{}, err
	}
	req.Header.Set("Content-Type", "application/json")
	if c.apiKey != "" {
		req.Header.Set("Authorization", "Bearer "+c.apiKey)
	}

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return chatMessage{}, err
	}
	defer func() { _ = resp.Body.Close() }()

	respBody, err := io.ReadAll(resp.Body)
	if err != nil {
		return chatMessage{}, fmt.Errorf("reading llm response: %w", err)
	}
	if resp.StatusCode != http.StatusOK {
		return chatMessage{}, fmt.Errorf("llm endpoint returned %s: %s", resp.Status, string(respBody))
	}

	var parsed chatResponse
	if err := json.Unmarshal(respBody, &parsed); err != nil {
		return chatMessage{}, fmt.Errorf("decoding llm response: %w", err)
	}
	if parsed.Error != nil {
		return chatMessage{}, fmt.Errorf("llm endpoint error: %s", parsed.Error.Message)
	}
	if len(parsed.Choices) == 0 {
		return chatMessage{}, fmt.Errorf("llm endpoint returned no choices")
	}
	return parsed.Choices[0].Message, nil
}

// agentLoop drives this node's LLM through a recursive instructions+tool-use
// cycle - like a Foundry/Assistants-style agent: the LLM decides which of
// this node's allowed tools to call and with what arguments, sees each
// result, and repeats - until it produces a final answer, or the turn cap
// is hit. It reports false if the loop never reached a final answer (an LLM
// call failed, or the turn cap was hit), telling the caller not to publish
// or commit anything for this task; the caller (see handler.go) treats that
// as fatal and exits so Kubernetes restarts the pod and Kafka redelivers
// the uncommitted input.
func (h *agentHandler) agentLoop(ctx context.Context, data map[string]any) (map[string]any, bool) {
	if h.llm == nil {
		// A Worker node with no LLM to call isn't a lesser agent, it's not
		// an agent - it can never do anything with instructions or tools
		// even when they're configured (and calls the LLM either way, even
		// with none set, see below). This should never happen in practice
		// since the CRD defaults llm.baseURL; treat it as the
		// misconfiguration it is (a bad APIKeySecretRef, an old CR applied
		// before this field existed, baseURL explicitly zeroed out) rather
		// than silently forwarding unprocessed data downstream.
		log.Printf("[%s] has no LLM endpoint configured (LLM_BASE_URL is empty)", h.cfg.nodeName)
		return nil, false
	}

	taskJSON, err := json.Marshal(data)
	if err != nil {
		log.Printf("[%s] failed to marshal task for the agent loop: %v", h.cfg.nodeName, err)
		return nil, false
	}

	messages := []chatMessage{
		{Role: "system", Content: h.systemPrompt()},
		{Role: "user", Content: string(taskJSON)},
	}
	tools := h.toolSpecs()

	for turn := range maxAgentTurns {
		reply, err := h.llm.chat(ctx, messages, tools)
		if err != nil {
			log.Printf("[%s] LLM call failed (turn %d): %v", h.cfg.nodeName, turn, err)
			return nil, false
		}

		if len(reply.ToolCalls) == 0 {
			log.Printf("[%s] agent finished after %d turn(s): %s", h.cfg.nodeName, turn+1, reply.Content)
			return mergeFinalAnswer(data, reply.Content), true
		}

		messages = append(messages, reply)
		for _, tc := range reply.ToolCalls {
			messages = append(messages, chatMessage{
				Role:       "tool",
				ToolCallID: tc.ID,
				Content:    h.callToolForAgent(ctx, tc),
			})
		}
	}

	log.Printf("[%s] agent loop hit the %d-turn safety cap without a final answer", h.cfg.nodeName, maxAgentTurns)
	return nil, false
}

// systemPrompt tells the LLM what step of the pipeline it is and hands it
// this node's instructions verbatim.
func (h *agentHandler) systemPrompt() string {
	return fmt.Sprintf(
		"You are the %q step in a Kafka-coordinated multi-agent pipeline. %s\n\n"+
			"You will be given the task so far as a JSON object. Use your available tools as needed to "+
			"complete your part of the task, but never call the same tool with the same arguments more "+
			"than once - you already have its result. As soon as you have everything you need, stop "+
			"calling tools and reply with your final answer: a single JSON object, and nothing else - no "+
			"markdown code fences, no commentary before or after it. That final object is merged into the "+
			"data handed to the next step(s) in the pipeline, so include every field you want them to see.",
		h.cfg.nodeName, h.cfg.instructions,
	)
}

// toolSpecs builds the OpenAI-format tool list for this node's allowed
// tools, from the schemas already fetched over MCP.
func (h *agentHandler) toolSpecs() []toolSpec {
	specs := make([]toolSpec, 0, len(h.cfg.allowedTools))
	for _, name := range h.cfg.allowedTools {
		binding, ok := h.tools[name]
		if !ok {
			log.Printf("[%s] tool %q is not available from any configured MCP endpoint", h.cfg.nodeName, name)
			continue
		}
		specs = append(specs, toolSpec{
			Type: "function",
			Function: toolFunction{
				Name:        binding.tool.Name,
				Description: binding.tool.Description,
				Parameters: map[string]any{
					"type":       "object",
					"properties": binding.tool.InputSchema.Properties,
					"required":   binding.tool.InputSchema.Required,
				},
			},
		})
	}
	return specs
}

// callToolForAgent executes one LLM-requested tool call over MCP and
// returns its result serialized for a "tool" role message. A tool error is
// handed back to the LLM as the tool's result (so it can see the failure
// and adapt) rather than treated as an agent-loop failure.
func (h *agentHandler) callToolForAgent(ctx context.Context, tc toolCall) string {
	binding, ok := h.tools[tc.Function.Name]
	if !ok {
		return fmt.Sprintf(`{"error": "tool %q is not available"}`, tc.Function.Name)
	}

	var args map[string]any
	if err := json.Unmarshal([]byte(tc.Function.Arguments), &args); err != nil {
		return fmt.Sprintf(`{"error": "invalid arguments: %s"}`, err.Error())
	}

	result, err := binding.client.CallTool(ctx, mcp.CallToolRequest{
		Params: mcp.CallToolParams{Name: tc.Function.Name, Arguments: args},
	})
	if err != nil {
		log.Printf("[%s] tool %q call failed: %v", h.cfg.nodeName, tc.Function.Name, err)
		return fmt.Sprintf(`{"error": %q}`, err.Error())
	}

	parsed := parseToolResult(result)
	out, err := json.Marshal(parsed)
	if err != nil {
		return "{}"
	}
	log.Printf("[%s] called tool %q with %s -> %s", h.cfg.nodeName, tc.Function.Name, tc.Function.Arguments, out)
	return string(out)
}

// mergeFinalAnswer folds the agent's final answer into the outgoing data.
// If the answer contains a JSON object its fields are merged in (the
// expected case, per the instruction in systemPrompt); otherwise it's kept
// verbatim under a "result" key rather than discarded.
func mergeFinalAnswer(data map[string]any, content string) map[string]any {
	out := make(map[string]any, len(data)+1)
	maps.Copy(out, data)

	if parsed, ok := extractJSONObject(content); ok {
		maps.Copy(out, parsed)
	} else if content != "" {
		out["result"] = content
	}
	return out
}

// extractJSONObject finds a JSON object in the agent's final answer. Models
// - especially smaller/local ones - don't reliably follow "reply with only
// JSON": a real answer seen in testing was wrapped in a markdown code fence
// with prose both before and after it. This tries, in order: the content
// as-is, the contents of a ```-fenced block (with or without a "json" tag),
// and finally the substring from the first '{' to the last '}' in the
// content - broad, but this runner only ever expects one JSON object per
// answer, so a stray brace elsewhere in the prose is an acceptable risk
// against silently losing the whole answer.
func extractJSONObject(content string) (map[string]any, bool) {
	if parsed, ok := tryUnmarshalObject(content); ok {
		return parsed, true
	}

	if _, rest, ok := strings.Cut(content, "```"); ok {
		rest = strings.TrimPrefix(rest, "json")
		rest = strings.TrimPrefix(rest, "\n")
		if fenced, _, ok := strings.Cut(rest, "```"); ok {
			if parsed, ok := tryUnmarshalObject(fenced); ok {
				return parsed, true
			}
		}
	}

	if start := strings.Index(content, "{"); start != -1 {
		if end := strings.LastIndex(content, "}"); end > start {
			if parsed, ok := tryUnmarshalObject(content[start : end+1]); ok {
				return parsed, true
			}
		}
	}

	return nil, false
}

func tryUnmarshalObject(s string) (map[string]any, bool) {
	var parsed map[string]any
	if err := json.Unmarshal([]byte(strings.TrimSpace(s)), &parsed); err != nil {
		return nil, false
	}
	return parsed, true
}
