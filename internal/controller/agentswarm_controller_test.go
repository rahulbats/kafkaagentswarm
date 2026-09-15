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

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	appsv1 "k8s.io/api/apps/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/IBM/sarama"

	swarmv1alpha1 "github.com/rahulbats/kafkaagentswarm/api/v1alpha1"
)

// fakeKafkaAdmin is a no-op kafkaAdmin: envtest has no real Kafka broker for
// the reconciler to dial, so tests substitute this in place of a real
// Sarama cluster admin (see AgentSwarmReconciler.NewKafkaAdmin).
type fakeKafkaAdmin struct {
	topics map[string]sarama.TopicDetail
}

func newFakeKafkaAdmin() *fakeKafkaAdmin {
	return &fakeKafkaAdmin{topics: map[string]sarama.TopicDetail{}}
}

func (f *fakeKafkaAdmin) ListTopics() (map[string]sarama.TopicDetail, error) {
	return f.topics, nil
}

func (f *fakeKafkaAdmin) CreateTopic(topic string, detail *sarama.TopicDetail, _ bool) error {
	f.topics[topic] = *detail
	return nil
}

func (f *fakeKafkaAdmin) Close() error { return nil }

var _ = Describe("AgentSwarm Controller", func() {
	Context("When reconciling a resource", func() {
		const (
			resourceName      = "test-resource"
			resourceNamespace = "default"
		)

		ctx := context.Background()

		typeNamespacedName := types.NamespacedName{
			Name:      resourceName,
			Namespace: resourceNamespace,
		}
		agentswarm := &swarmv1alpha1.AgentSwarm{}

		BeforeEach(func() {
			By("creating the custom resource for the Kind AgentSwarm")
			err := k8sClient.Get(ctx, typeNamespacedName, agentswarm)
			if err != nil && errors.IsNotFound(err) {
				resource := &swarmv1alpha1.AgentSwarm{
					ObjectMeta: metav1.ObjectMeta{
						Name:      resourceName,
						Namespace: resourceNamespace,
					},
					Spec: swarmv1alpha1.AgentSwarmSpec{
						Kafka: swarmv1alpha1.KafkaConfig{
							Brokers: []string{"localhost:9092"},
						},
						Nodes: []swarmv1alpha1.AgentNode{
							{
								Name:  "ingest-agent",
								Type:  swarmv1alpha1.NodeTypeWorker,
								Image: "agent-runner:v1",
							},
						},
					},
				}
				Expect(k8sClient.Create(ctx, resource)).To(Succeed())
			}
		})

		AfterEach(func() {
			resource := &swarmv1alpha1.AgentSwarm{}
			err := k8sClient.Get(ctx, typeNamespacedName, resource)
			Expect(err).NotTo(HaveOccurred())

			By("Cleanup the specific resource instance AgentSwarm")
			Expect(k8sClient.Delete(ctx, resource)).To(Succeed())
		})
		It("should successfully reconcile the resource", func() {
			By("Reconciling the created resource")
			controllerReconciler := &AgentSwarmReconciler{
				Client: k8sClient,
				Scheme: k8sClient.Scheme(),
				// envtest has no real Kafka broker to dial; substitute a
				// fake so the reconciler's topic-provisioning logic still
				// runs (and can be asserted on) without one.
				NewKafkaAdmin: func(brokers []string) (kafkaAdmin, error) {
					return newFakeKafkaAdmin(), nil
				},
			}

			_, err := controllerReconciler.Reconcile(ctx, reconcile.Request{
				NamespacedName: typeNamespacedName,
			})
			Expect(err).NotTo(HaveOccurred())

			By("creating a Deployment for the Worker node")
			deployment := &appsv1.Deployment{}
			Expect(k8sClient.Get(ctx, types.NamespacedName{
				Name:      resourceName + "-ingest-agent",
				Namespace: resourceNamespace,
			}, deployment)).To(Succeed())
		})
	})
})
