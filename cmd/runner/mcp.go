package main

import (
	"context"
	"encoding/json"
	"log"

	"github.com/mark3labs/mcp-go/client"
	"github.com/mark3labs/mcp-go/mcp"
)

// toolBinding is where a tool named in ALLOWED_TOOLS was actually found:
// which MCP client serves it, and its schema (handed to the LLM so it knows
// how to call it).
type toolBinding struct {
	client *client.Client
	tool   mcp.Tool
}

// connectMCPTools connects to every configured MCP endpoint, lists its
// tools, and returns a lookup from tool name to the client/schema that
// serves it, plus a cleanup func that closes every connection.
func connectMCPTools(ctx context.Context, cfg config) (map[string]toolBinding, func()) {
	tools := map[string]toolBinding{}
	var clients []*client.Client

	for name, url := range cfg.mcpEndpoints {
		c, err := client.NewSSEMCPClient(url)
		if err != nil {
			log.Printf("Failed to create MCP client for %s (%s): %v", name, url, err)
			continue
		}
		if err := c.Start(ctx); err != nil {
			log.Printf("Failed to start MCP client for %s (%s): %v", name, url, err)
			continue
		}

		initReq := mcp.InitializeRequest{}
		initReq.Params.ProtocolVersion = mcp.LATEST_PROTOCOL_VERSION
		initReq.Params.ClientInfo = mcp.Implementation{Name: cfg.nodeName, Version: "1.0.0"}
		if _, err := c.Initialize(ctx, initReq); err != nil {
			log.Printf("Failed to initialize MCP client for %s (%s): %v", name, url, err)
			_ = c.Close()
			continue
		}

		listResult, err := c.ListTools(ctx, mcp.ListToolsRequest{})
		if err != nil {
			log.Printf("Failed to list tools from %s (%s): %v", name, url, err)
			_ = c.Close()
			continue
		}

		for _, t := range listResult.Tools {
			tools[t.Name] = toolBinding{client: c, tool: t}
		}
		clients = append(clients, c)
		log.Printf("Connected to MCP endpoint %s (%s): %d tool(s)", name, url, len(listResult.Tools))
	}

	return tools, func() {
		for _, c := range clients {
			_ = c.Close()
		}
	}
}

// parseToolResult reads the first JSON-object text content out of a tool
// result. MCP tools can return non-JSON or multi-part content; this runner
// only understands the single-JSON-object convention the example MCP server
// uses.
func parseToolResult(result *mcp.CallToolResult) map[string]any {
	for _, c := range result.Content {
		text, ok := c.(mcp.TextContent)
		if !ok {
			continue
		}
		var parsed map[string]any
		if err := json.Unmarshal([]byte(text.Text), &parsed); err == nil {
			return parsed
		}
	}
	return map[string]any{}
}
