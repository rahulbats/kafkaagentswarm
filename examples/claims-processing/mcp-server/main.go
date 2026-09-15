package main

import (
	"context"
	"fmt"
	"log"
	"net/http"

	"github.com/mark3labs/mcp-go/mcp"
	"github.com/mark3labs/mcp-go/server"
)

func main() {
	s := server.NewMCPServer(
		"ClaimsToolServer",
		"1.0.0",
		server.WithToolCapabilities(true),
	)

	// 1. Ingestion Tool
	// raw_json is intentionally optional: by the time a worker calls a tool,
	// the swarm runner has already parsed the Kafka message into structured
	// fields (see cmd/runner), so there's no separate raw string blob to
	// hand this tool - it just marks that ingestion happened.
	s.AddTool(
		mcp.NewTool("parse_claim_json",
			mcp.WithDescription("Parse raw claim JSON payload"),
			mcp.WithString("raw_json", mcp.Description("Raw JSON input, if available")),
		),
		func(ctx context.Context, request mcp.CallToolRequest) (*mcp.CallToolResult, error) {
			return mcp.NewToolResultText(`{"status": "parsed", "claim_id": "CLM-9901", "policy_id": "POL-441"}`), nil
		},
	)

	// 2. Fraud Analysis Tools
	s.AddTool(
		mcp.NewTool("query_fraud_database",
			mcp.WithDescription("Check fraud DB for policy history"),
			mcp.WithString("policy_id", mcp.Required(), mcp.Description("Target Policy ID")),
		),
		func(ctx context.Context, request mcp.CallToolRequest) (*mcp.CallToolResult, error) {
			return mcp.NewToolResultText(`{"previous_claims": 0, "flagged": false}`), nil
		},
	)

	s.AddTool(
		mcp.NewTool("calculate_risk_score",
			mcp.WithDescription("Calculate risk score based on claim amount"),
			// Named "amount" to match the field the claim payload already
			// carries (see swarm.yaml's Trigger Pipeline Execution step),
			// rather than requiring a separate rename step.
			mcp.WithNumber("amount", mcp.Required(), mcp.Description("Claim dollar amount")),
		),
		func(ctx context.Context, request mcp.CallToolRequest) (*mcp.CallToolResult, error) {
			// fraud_status is what claim-decision-agent's synthesize_claim_summary
			// tool expects downstream - carried alongside the raw score/rating.
			return mcp.NewToolResultText(`{"risk_score": 0.12, "rating": "LOW", "fraud_status": "LOW"}`), nil
		},
	)

	// 3. Coverage Verification Tools
	s.AddTool(
		mcp.NewTool("get_policy_coverage",
			mcp.WithDescription("Check policy limit and endorsements"),
			mcp.WithString("policy_id", mcp.Required(), mcp.Description("Target Policy ID")),
		),
		func(ctx context.Context, request mcp.CallToolRequest) (*mcp.CallToolResult, error) {
			// coverage_status is what claim-decision-agent's synthesize_claim_summary
			// tool expects downstream - carried alongside the raw coverage details.
			return mcp.NewToolResultText(`{"active": true, "max_coverage": 50000, "coverage_status": "ACTIVE"}`), nil
		},
	)

	s.AddTool(
		mcp.NewTool("get_deductible_balance",
			mcp.WithDescription("Fetch remaining deductible amount"),
			mcp.WithString("policy_id", mcp.Required(), mcp.Description("Target Policy ID")),
		),
		func(ctx context.Context, request mcp.CallToolRequest) (*mcp.CallToolResult, error) {
			return mcp.NewToolResultText(`{"deductible_remaining": 500}`), nil
		},
	)

	// 4. Aggregation Tool
	s.AddTool(
		mcp.NewTool("synthesize_claim_summary",
			mcp.WithDescription("Synthesize fraud and coverage results"),
			mcp.WithString("fraud_status", mcp.Required(), mcp.Description("Status from fraud check")),
			mcp.WithString("coverage_status", mcp.Required(), mcp.Description("Status from coverage check")),
		),
		func(ctx context.Context, request mcp.CallToolRequest) (*mcp.CallToolResult, error) {
			args := request.GetArguments()
			res := fmt.Sprintf(`{"decision": "APPROVED", "fraud_status": "%v", "coverage_status": "%v"}`,
				args["fraud_status"], args["coverage_status"])
			return mcp.NewToolResultText(res), nil
		},
	)

	// The base URL has to be the address clients actually connect through
	// (the k8s Service DNS name from k8s/mcp-deployment.yaml - see
	// swarm.yaml's mcpServices), not the bind-all listen address: the
	// server announces this as its message endpoint on connect, and MCP
	// clients validate that the announced origin matches the one they
	// connected to, rejecting "http://0.0.0.0:8000" as a mismatch.
	sseServer := server.NewSSEServer(s, server.WithBaseURL("http://mcp-claims-server:8000"))
	log.Println("MCP Server listening on :8000...")
	if err := http.ListenAndServe(":8000", sseServer); err != nil {
		log.Fatalf("Server error: %v", err)
	}
}
