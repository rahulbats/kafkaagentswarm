# Claims Processing Pipeline Example

Demonstrates an end-to-end insurance claims processing pipeline orchestrated by **kafkaagentswarm**. This example showcases dynamic DAG topology reconciliation, parallel fan-out execution (`fraud-detection` and `coverage-verifier`), fan-in aggregation (`claim-decision`), tool execution via an external **MCP Server**, and a **Human-in-the-Loop** gate.

---

## Architecture Overview
```
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
```

Topics are auto-named by the controller, one input topic per node (`<swarm>-<node>-input`); there's no separately-named "output" topic — a node's output is simply whatever topic(s) its downstream dependents read from. `claim-decision-agent` declares two `dependsOn` entries, so the runner waits until it has seen a message from both `fraud-detection-agent` and `coverage-verifier-agent` for the same claim before proceeding (a fan-in join, keyed by a correlation ID carried on every message).

---

## Step-by-Step Guide

### 1. Start the kafkaagentswarm Controller Operator

Ensure your local `k3d` cluster (e.g., `controller`) is running and start the operator reconciler:

```bash
# Terminal 1: Run the Go controller
make run
```

### 2. Build Container Images

```bash
# Terminal 2: Navigate to repo root
cd kafkaagentswarm

# Build the MCP Claims Server image
docker build -t mcp-claims-server:v1 ./examples/claims-processing/mcp-server

# Build the generic Go Agent Runner image
docker build -t agent-runner:v1 -f Dockerfile.runner .
```

### 3. Load Images into k3d Cluster

```bash
k3d image load mcp-claims-server:v1 -c controller
k3d image load agent-runner:v1 -c controller
```

### 4. Deploy Pipeline Infrastructure & Agent Swarm

`swarm.yaml` references your LLM's API key by name (`llm.apiKeySecretRef`), not by value, so create the Secret yourself first - the key itself never needs to go into a file this repo tracks:

```bash
kubectl create secret generic llm-credentials --from-literal=api-key='<your-api-key>'
```

(If your local server doesn't require auth - Ollama, most local setups - `apiKeySecretRef` is optional; you can drop it from `swarm.yaml` and skip this step.)

```bash
# 1. Deploy the MCP Tool Server Deployment & Service
kubectl apply -f examples/claims-processing/k8s/mcp-deployment.yaml

# 2. Deploy the AgentSwarm Custom Resource
kubectl apply -f examples/claims-processing/swarm.yaml
```

### Verify Cluster Workloads

```bash
# Verify deployments
kubectl get deployments

# Verify active worker pods
kubectl get pods
```

Expected Pod Statuses:
- `mcp-claims-server` (1/1 Running)
- `claims-processing-swarm-claims-ingest-agent` (1/1 Running)
- `claims-processing-swarm-fraud-detection-agent` (2/2 Running)
- `claims-processing-swarm-coverage-verifier-agent` (2/2 Running)
- `claims-processing-swarm-claim-decision-agent` (1/1 Running)
- (Note: `hitl-payout-approval` generates no pods because it is a HumanInTheLoop gate — it still gets a Kafka input topic for `claim-decision-agent` to publish the final decision into, for a human/downstream system to consume.)

### 5. Inspect Injected Environment

```bash
POD_NAME=$(kubectl get pods -l app=agentswarm --no-headers | grep ingest-agent | awk '{print $1}')
kubectl exec "$POD_NAME" -- env
```

You should see `NODE_NAME`, `INPUT_TOPIC`, `OUTPUT_TOPICS`, `DEPENDS_ON`, `ALLOWED_TOOLS`, `MCP_ENDPOINTS`, `INSTRUCTIONS`, `LLM_BASE_URL`, `LLM_MODEL`, and `KAFKA_BROKERS` — everything `cmd/runner` needs to wire itself into the DAG without a coordinator telling it what to do.

> **NOTE:** Every Worker node needs a reachable LLM: `swarm.yaml` points `llm.baseURL` at `http://host.k3d.internal:8001/v1`, k3d's built-in DNS alias for the host machine, assuming a local OpenAI-compatible server (e.g. an MLX-based server) running on the host at port 8001. Set `llm.model` to whatever model name your server expects (check its `/v1/models` endpoint), and see step 4 for pointing `llm.apiKeySecretRef` at your server's API key. A missing or unreachable LLM endpoint is fatal for a Worker pod by design (see `cmd/runner/agent.go`) — it'll crash-loop rather than silently forward unprocessed data. Running something else (Ollama, LM Studio, a hosted provider) instead? Just adjust `llm.baseURL`/`llm.model` to match.

### 6. Trigger Pipeline Execution

`swarm.yaml` points `kafka.brokers` at `host.k3d.internal:9092` (see the note below), which only resolves *inside* the k3d cluster — so trigger the pipeline from a pod rather than a Kafka CLI on your host:

```bash
kubectl run kafka-trigger --rm -i --restart=Never --image=edenhill/kcat:1.7.1 -- \
  sh -c 'echo "{\"claim_id\": \"CLM-9901\", \"policy_id\": \"POL-441\", \"amount\": 1200.00}" | \
    kcat -b host.k3d.internal:9092 -t claims-processing-swarm-claims-ingest-agent-input -P'
```

> **NOTE:** If your broker runs locally the same way (e.g. `apache/kafka` in Docker on your Mac), it needs to *advertise* `host.k3d.internal:<port>` too, not just be reachable there — Kafka clients reconnect to whatever address the broker's metadata response tells them to use, not necessarily the one they bootstrapped through. A broker advertising `localhost:9092` (a common local-dev default) will let a pod's initial connection succeed and then fail on the actual produce/consume call, since the pod would be told to reconnect to its own loopback. Check with `docker inspect <container> --format '{{json .Config.Env}}'` and set `KAFKA_ADVERTISED_LISTENERS=PLAINTEXT://host.k3d.internal:9092` (this can't be changed on a running container - recreate it, which for a local dev broker with `KAFKA_LOG_DIRS` on ephemeral storage also resets its data). If you need the broker reachable from your host machine too (e.g. for your own CLI tools) as well as from pods, you'll need a second listener with its own advertised address and port, since a single listener can only advertise one address to every client.

Stream live worker logs to observe message consumption, MCP tool invocation, and downstream event passing across the DAG:

```bash
kubectl logs -l node=claims-ingest-agent -f
kubectl logs -l node=fraud-detection-agent -f
kubectl logs -l node=coverage-verifier-agent -f
kubectl logs -l node=claim-decision-agent -f
```

### 7. Teardown & Cleanup

```bash
kubectl delete -f examples/claims-processing/swarm.yaml
kubectl delete -f examples/claims-processing/k8s/mcp-deployment.yaml
```
