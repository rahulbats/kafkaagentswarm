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
	s.AddTool(
		mcp.NewTool("parse_claim_json",
			mcp.WithDescription("Parse raw claim JSON payload"),
			mcp.WithString("raw_json", mcp.Required(), mcp.Description("Raw JSON input")),
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
			mcp.WithNumber("claim_amount", mcp.Required(), mcp.Description("Claim dollar amount")),
		),
		func(ctx context.Context, request mcp.CallToolRequest) (*mcp.CallToolResult, error) {
			return mcp.NewToolResultText(`{"risk_score": 0.12, "rating": "LOW"}`), nil
		},
	)

	// 3. Coverage Verification Tools
	s.AddTool(
		mcp.NewTool("get_policy_coverage",
			mcp.WithDescription("Check policy limit and endorsements"),
			mcp.WithString("policy_id", mcp.Required(), mcp.Description("Target Policy ID")),
		),
		func(ctx context.Context, request mcp.CallToolRequest) (*mcp.CallToolResult, error) {
			return mcp.NewToolResultText(`{"active": true, "max_coverage": 50000}`), nil
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

	sseServer := server.NewSSEServer(s, server.WithBaseURL("http://0.0.0.0:8000"))
	log.Println("MCP Server listening on :8000...")
	if err := http.ListenAndServe(":8000", sseServer); err != nil {
		log.Fatalf("Server error: %v", err)
	}
}
