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

// AgentSwarmReconciler reconciles a AgentSwarm object
type AgentSwarmReconciler struct {
	client.Client
	Scheme *runtime.Scheme
}

// +kubebuilder:rbac:groups=swarm.kafkaagentswarm.io,resources=agentswarms,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=swarm.kafkaagentswarm.io,resources=agentswarms/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=swarm.kafkaagentswarm.io,resources=agentswarms/finalizers,verbs=update

// Reconcile is part of the main kubernetes reconciliation loop which aims to
// move the current state of the cluster closer to the desired state.
// TODO(user): Modify the Reconcile function to compare the state specified by
// the AgentSwarm object against the actual cluster state, and then
// perform operations to make the cluster state reflect the state specified by
// the user.
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
	admin, err := r.newKafkaAdmin(swarm.Spec.Kafka.Brokers)
	if err != nil {
		logger.Error(err, "Failed to connect to Kafka brokers", "brokers", swarm.Spec.Kafka.Brokers)
		return ctrl.Result{}, err
	}
	defer func() {
		if cerr := admin.Close(); cerr != nil {
			logger.Error(cerr, "Failed to close Kafka admin client")
		}
	}()

	// 3. Process DAG Nodes: Provision Topics & Reconcile Worker Deployments
	for _, node := range swarm.Spec.Nodes {
		// Topic naming convention: <swarm-name>-<node-name>-events
		topicName := fmt.Sprintf("%s-%s-events", swarm.Name, node.Name)

		// Create topic on external Kafka cluster if missing
		if err := r.ensureKafkaTopic(admin, topicName); err != nil {
			logger.Error(err, "Failed to reconcile Kafka topic", "topic", topicName)
			return ctrl.Result{}, err
		}

		// Skip Pod creation for Human-in-the-Loop nodes
		if node.Type == swarmv1alpha1.NodeTypeHITL {
			logger.Info("Configured HITL node event gates", "node", node.Name)
			continue
		}

		// Reconcile Kubernetes Deployment for standard worker nodes
		if err := r.reconcileDeployment(ctx, &swarm, node, topicName); err != nil {
			logger.Error(err, "Failed to reconcile Deployment", "node", node.Name)
			return ctrl.Result{}, err
		}
	}

	return ctrl.Result{}, nil
}

// ensureKafkaTopic checks and creates the required topic using Sarama ClusterAdmin
func (r *AgentSwarmReconciler) ensureKafkaTopic(admin sarama.ClusterAdmin, topicName string) error {
	topics, err := admin.ListTopics()
	if err != nil {
		return err
	}

	if _, exists := topics[topicName]; !exists {
		detail := &sarama.TopicDetail{
			NumPartitions:     3,
			ReplicationFactor: 1,
		}
		return admin.CreateTopic(topicName, detail, false)
	}
	return nil
}

// reconcileDeployment creates or updates the pod workload for a DAG node
func (r *AgentSwarmReconciler) reconcileDeployment(ctx context.Context, swarm *swarmv1alpha1.AgentSwarm, node swarmv1alpha1.AgentNode, outputTopic string) error {
	deploymentName := fmt.Sprintf("%s-%s", swarm.Name, node.Name)
	desiredDep := r.buildDeployment(swarm, node, deploymentName, outputTopic)
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
func (r *AgentSwarmReconciler) buildDeployment(swarm *swarmv1alpha1.AgentSwarm, node swarmv1alpha1.AgentNode, name string, outputTopic string) *appsv1.Deployment {
	labels := map[string]string{
		"app":   "agentswarm",
		"node":  node.Name,
		"swarm": swarm.Name,
	}

	replicas := node.Replicas
	if replicas == 0 {
		replicas = 1
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
						Name:    "agent-runner",
						Image:   node.Image,
						Command: []string{"sh", "-c", "echo Agent running... && sleep 3600"},
						Env: []corev1.EnvVar{
							{Name: "KAFKA_BROKERS", Value: fmt.Sprintf("%v", swarm.Spec.Kafka.Brokers)},
							{Name: "OUTPUT_TOPIC", Value: outputTopic},
							{Name: "INSTRUCTIONS", Value: node.Instructions},
						},
					}},
				},
			},
		},
	}

	// Set AgentSwarm instance as the owner and controller of this Deployment
	_ = ctrl.SetControllerReference(swarm, dep, r.Scheme)
	return dep
}

func (r *AgentSwarmReconciler) newKafkaAdmin(brokers []string) (sarama.ClusterAdmin, error) {
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
