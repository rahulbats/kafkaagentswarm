/*
Copyright 2026.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package controller

import (
	"context"
	"fmt"
	"strings"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"

	"github.com/IBM/sarama"

	swarmv1alpha1 "github.com/rahulbats/kafkaagentswarm/api/v1alpha1"
)

// kafkaAdmin is the subset of sarama.ClusterAdmin this controller actually
// uses. Narrowed down (Sarama's real interface has 30+ methods) so tests can
// substitute a fake here instead of needing a real Kafka broker.
type kafkaAdmin interface {
	ListTopics() (map[string]sarama.TopicDetail, error)
	CreateTopic(topic string, detail *sarama.TopicDetail, validateOnly bool) error
	Close() error
}

// AgentSwarmReconciler reconciles a AgentSwarm object
type AgentSwarmReconciler struct {
	client.Client
	Scheme *runtime.Scheme

	// NewKafkaAdmin constructs the Kafka admin client used to provision
	// topics. Defaults to a real Sarama cluster admin (see
	// newSaramaClusterAdmin); tests set this to a fake instead of needing a
	// real broker reachable from envtest.
	NewKafkaAdmin func(brokers []string) (kafkaAdmin, error)
}

// +kubebuilder:rbac:groups=swarm.kafkaagentswarm.io,resources=agentswarms,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=swarm.kafkaagentswarm.io,resources=agentswarms/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=swarm.kafkaagentswarm.io,resources=agentswarms/finalizers,verbs=update
// +kubebuilder:rbac:groups=apps,resources=deployments,verbs=get;list;watch;create;update;patch;delete

// Reconcile is part of the main kubernetes reconciliation loop which aims to
// move the current state of the cluster closer to the desired state.
//
// For more details, check Reconcile and its Result here:
// - https://pkg.go.dev/sigs.k8s.io/controller-runtime@v0.25.0/pkg/reconcile
func (r *AgentSwarmReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	logger := log.FromContext(ctx)

	// 1. Fetch the AgentSwarm instance
	var swarm swarmv1alpha1.AgentSwarm
	if err := r.Get(ctx, req.NamespacedName, &swarm); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}

	logger.Info("Reconciling AgentSwarm", "name", swarm.Name)

	// 2. Initialize Kafka Admin Client
	newAdmin := r.NewKafkaAdmin
	if newAdmin == nil {
		newAdmin = newSaramaClusterAdmin
	}
	admin, err := newAdmin(swarm.Spec.Kafka.Brokers)
	if err != nil {
		logger.Error(err, "Failed to connect to Kafka brokers", "brokers", swarm.Spec.Kafka.Brokers)
		return ctrl.Result{}, err
	}
	defer func() {
		if cerr := admin.Close(); cerr != nil {
			logger.Error(cerr, "Failed to close Kafka admin client")
		}
	}()

	// 3. Compute the DAG wiring. Every node gets exactly one input topic,
	// auto-named from the swarm/node name. A node's outputs aren't a
	// separately named topic: they're simply the input topics of whatever
	// nodes declare it in dependsOn, so a sequential edge is just "write to
	// the next node's input topic" and a fan-out edge is "write to each of
	// them". A node with more than one dependsOn entry is a fan-in join:
	// the worker (see cmd/runner) is responsible for waiting until it has
	// seen a message from every declared dependency for a given
	// correlation ID before treating the join as complete.
	inputTopics := make(map[string]string, len(swarm.Spec.Nodes))
	for _, node := range swarm.Spec.Nodes {
		inputTopics[node.Name] = fmt.Sprintf("%s-%s-input", swarm.Name, node.Name)
	}

	downstreamOf := make(map[string][]string, len(swarm.Spec.Nodes))
	for _, node := range swarm.Spec.Nodes {
		for _, dep := range node.DependsOn {
			downstreamOf[dep] = append(downstreamOf[dep], node.Name)
		}
	}

	// 4. Process DAG Nodes: Provision Topics & Reconcile Worker Deployments
	for _, node := range swarm.Spec.Nodes {
		inputTopic := inputTopics[node.Name]

		// Every node - including HumanInTheLoop gates - gets an input topic,
		// since upstream nodes need somewhere to publish into even when
		// nothing consumes it automatically.
		if err := r.ensureKafkaTopic(admin, inputTopic); err != nil {
			logger.Error(err, "Failed to reconcile Kafka topic", "topic", inputTopic)
			return ctrl.Result{}, err
		}

		// Skip Pod creation for Human-in-the-Loop nodes
		if node.Type == swarmv1alpha1.NodeTypeHITL {
			logger.Info("Configured HITL node event gate", "node", node.Name, "topic", inputTopic)
			continue
		}

		outputTopics := make([]string, 0, len(downstreamOf[node.Name]))
		for _, downstreamNode := range downstreamOf[node.Name] {
			outputTopics = append(outputTopics, inputTopics[downstreamNode])
		}

		// A node with more than one dependsOn entry is a fan-in join. Its
		// partial join progress needs to survive a pod restart independent
		// of when the source (input) offset gets committed - see
		// cmd/runner's join state handling for why deferring the input
		// commit isn't safe here. It gets a compacted "changelog" topic to
		// persist that state in, mirroring how Kafka Streams backs a
		// groupBy/aggregate state store with a changelog topic.
		var joinStateTopic string
		if len(node.DependsOn) > 1 {
			joinStateTopic = fmt.Sprintf("%s-%s-joinstate", swarm.Name, node.Name)
			if err := r.ensureCompactedTopic(admin, joinStateTopic); err != nil {
				logger.Error(err, "Failed to reconcile join-state topic", "topic", joinStateTopic)
				return ctrl.Result{}, err
			}
		}

		// Reconcile Kubernetes Deployment for standard worker nodes
		if err := r.reconcileDeployment(ctx, &swarm, node, inputTopic, outputTopics, joinStateTopic); err != nil {
			logger.Error(err, "Failed to reconcile Deployment", "node", node.Name)
			return ctrl.Result{}, err
		}
	}

	return ctrl.Result{}, nil
}

// ensureKafkaTopic checks and creates the required topic using Sarama ClusterAdmin
func (r *AgentSwarmReconciler) ensureKafkaTopic(admin kafkaAdmin, topicName string) error {
	return r.ensureTopic(admin, topicName, nil)
}

// ensureCompactedTopic creates a log-compacted topic, used as durable
// key-value storage (Kafka's "changelog topic" pattern) rather than an
// ordered event stream - only the latest value per key is retained.
func (r *AgentSwarmReconciler) ensureCompactedTopic(admin kafkaAdmin, topicName string) error {
	compact := "compact"
	return r.ensureTopic(admin, topicName, map[string]*string{"cleanup.policy": &compact})
}

func (r *AgentSwarmReconciler) ensureTopic(admin kafkaAdmin, topicName string, configEntries map[string]*string) error {
	topics, err := admin.ListTopics()
	if err != nil {
		return err
	}

	if _, exists := topics[topicName]; !exists {
		detail := &sarama.TopicDetail{
			NumPartitions:     3,
			ReplicationFactor: 1,
			ConfigEntries:     configEntries,
		}
		return admin.CreateTopic(topicName, detail, false)
	}
	return nil
}

// reconcileDeployment creates or updates the pod workload for a DAG node
func (r *AgentSwarmReconciler) reconcileDeployment(ctx context.Context, swarm *swarmv1alpha1.AgentSwarm, node swarmv1alpha1.AgentNode, inputTopic string, outputTopics []string, joinStateTopic string) error {
	deploymentName := fmt.Sprintf("%s-%s", swarm.Name, node.Name)
	desiredDep := r.buildDeployment(swarm, node, deploymentName, inputTopic, outputTopics, joinStateTopic)
	found := &appsv1.Deployment{}

	err := r.Get(ctx, types.NamespacedName{Name: deploymentName, Namespace: swarm.Namespace}, found)
	if err != nil && errors.IsNotFound(err) {
		return r.Create(ctx, desiredDep)
	} else if err != nil {
		return err
	}

	// Update template spec (command, image, env) and replica counts on existing deployments
	found.Spec.Replicas = desiredDep.Spec.Replicas
	found.Spec.Template = desiredDep.Spec.Template
	return r.Update(ctx, found)
}

// buildDeployment structures the Kubernetes Deployment spec for an agent node
func (r *AgentSwarmReconciler) buildDeployment(swarm *swarmv1alpha1.AgentSwarm, node swarmv1alpha1.AgentNode, name string, inputTopic string, outputTopics []string, joinStateTopic string) *appsv1.Deployment {
	labels := map[string]string{
		"app":   "agentswarm",
		"node":  node.Name,
		"swarm": swarm.Name,
	}

	replicas := node.Replicas
	if replicas == 0 {
		replicas = 1
	}

	env := []corev1.EnvVar{
		{Name: "KAFKA_BROKERS", Value: strings.Join(swarm.Spec.Kafka.Brokers, ",")},
		{Name: "NODE_NAME", Value: node.Name},
		{Name: "INPUT_TOPIC", Value: inputTopic},
		{Name: "OUTPUT_TOPICS", Value: strings.Join(outputTopics, ",")},
		{Name: "DEPENDS_ON", Value: strings.Join(node.DependsOn, ",")},
		{Name: "JOIN_STATE_TOPIC", Value: joinStateTopic},
		{Name: "ALLOWED_TOOLS", Value: strings.Join(node.AllowedTools, ",")},
		{Name: "MCP_ENDPOINTS", Value: encodeMCPEndpoints(node.MCPServices)},
		{Name: "INSTRUCTIONS", Value: node.Instructions},
		{Name: "LLM_BASE_URL", Value: node.LLM.BaseURL},
		{Name: "LLM_MODEL", Value: node.LLM.Model},
	}
	if node.LLM.APIKeySecretRef != nil {
		env = append(env, corev1.EnvVar{
			Name: "LLM_API_KEY",
			ValueFrom: &corev1.EnvVarSource{
				SecretKeyRef: node.LLM.APIKeySecretRef,
			},
		})
	}

	dep := &appsv1.Deployment{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: swarm.Namespace,
		},
		Spec: appsv1.DeploymentSpec{
			Replicas: &replicas,
			Selector: &metav1.LabelSelector{
				MatchLabels: labels,
			},
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{
					Labels: labels,
				},
				Spec: corev1.PodSpec{
					Containers: []corev1.Container{{
						Name:  "agent-runner",
						Image: node.Image,
						// No Command override: the image's own ENTRYPOINT
						// (cmd/runner) is what actually consumes/produces
						// Kafka messages and calls MCP tools.
						Env: env,
					}},
				},
			},
		},
	}

	// Set AgentSwarm instance as the owner and controller of this Deployment
	_ = ctrl.SetControllerReference(swarm, dep, r.Scheme)
	return dep
}

// encodeMCPEndpoints serializes a node's MCP service list into the
// "name=url,name=url" form the runner parses back out of MCP_ENDPOINTS.
func encodeMCPEndpoints(endpoints []swarmv1alpha1.MCPEndpoint) string {
	pairs := make([]string, 0, len(endpoints))
	for _, ep := range endpoints {
		pairs = append(pairs, fmt.Sprintf("%s=%s", ep.Name, ep.URL))
	}
	return strings.Join(pairs, ",")
}

// newSaramaClusterAdmin is the production NewKafkaAdmin implementation - a
// real Sarama cluster admin dialing the given brokers.
func newSaramaClusterAdmin(brokers []string) (kafkaAdmin, error) {
	config := sarama.NewConfig()
	config.Version = sarama.V3_0_0_0
	return sarama.NewClusterAdmin(brokers, config)
}

// SetupWithManager sets up the controller with the Manager.
func (r *AgentSwarmReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&swarmv1alpha1.AgentSwarm{}).
		Named("agentswarm").
		Complete(r)
}
