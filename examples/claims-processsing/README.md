# Claims Processing Pipeline Example

Demonstrates an end-to-end insurance claims processing pipeline orchestrated by **FlinkSwarm**. This example showcases dynamic DAG topology reconciliation, parallel fan-out execution (`fraud-detection` and `coverage-verifier`), static fan-in aggregation (`claim-decision`), tool execution via an external **MCP Server**, and a **Human-in-the-Loop** gate.

---

## Architecture Overview
                  ┌──────────────────────┐
                  │  claims-ingest-agent │
                  └──────────┬───────────┘
                             │ (Fan-Out)
          ┌──────────────────┴──────────────────┐
          ▼                                     ▼
┌─────────────────────────┐           ┌─────────────────────────┐
│ fraud-detection-agent   │           │ coverage-verifier-agent │
└────────────┬────────────┘           └────────────┬────────────┘
             │                                     │
             └──────────────────┬──────────────────┘
                                │ (Fan-In Aggregation)
                                ▼
                    ┌──────────────────────┐
                    │ claim-decision-agent │
                    └──────────┬───────────┘
                               │
                               ▼
                    ┌──────────────────────┐
                    │ hitl-payout-approval │ (HumanInTheLoop)
                    └──────────────────────┘


---

## Step-by-Step Guide
### 1. Start the FlinkSwarm Controller Operator

Ensure your local `k3d` cluster (e.g., `controller`) is running and start the operator reconciler:

```bash
# Terminal 1: Run the Go controller
make run



### 2. Build Container Images

# Terminal 2: Navigate to repo root
cd FlinkSwarm

# Build the MCP Claims Server image
docker build -t mcp-claims-server:v1 ./examples/claims-processing/mcp-server

# Build the generic Go Agent Runner image
docker build -t agent-runner:v1 -f Dockerfile.runner .


### 3. Load Images into k3d Cluster

k3d image load mcp-claims-server:v1 -c controller
k3d image load agent-runner:v1 -c controller



### 4. Deploy Pipeline Infrastructure & Agent Swarm
# 1. Deploy the MCP Tool Server Deployment & Service
kubectl apply -f examples/claims-processing/k8s/mcp-deployment.yaml

# 2. Deploy the AgentSwarm Custom Resource
kubectl apply -f examples/claims-processing/swarm.yaml


### Verify Cluster Workloads
# Verify deployments
kubectl get deployments

# Verify active worker pods
kubectl get pods

Expected Pod Statuses:
⚬	mcp-claims-server (1/1 Running)
⚬	claims-processing-swarm-claims-ingest-agent (1/1 Running)
⚬	claims-processing-swarm-fraud-detection-agent (2/2 Running)
⚬	claims-processing-swarm-coverage-verifier-agent (2/2 Running)
⚬	claims-processing-swarm-claim-decision-agent (1/1 Running)
⚬	(Note: hitl-payout-approval generates no pods because it is a HumanInTheLoop gate).



### 5: Inspect Injected Environment
POD_NAME=$(kubectl get pods -l app=agentswarm --no-headers | grep ingest-agent | awk '{print $1}')
kubectl exec $POD_NAME -- env



### 6: Trigger Pipeline Execution
echo '{"claim_id": "CLM-9901", "policy_id": "POL-441", "amount": 1200.00}' | \
  kafka-console-producer --broker-list localhost:9092 --topic claims-processing-swarm-claims-ingest-agent-events

Stream live worker logs to observe message consumption, tool invocation, and downstream event passing across the DAG:

### 7: Teardown & Cleanup
kubectl delete -f examples/claims-processing/swarm.yaml
kubectl delete -f examples/claims-processing/k8s/mcp-deployment.yaml
