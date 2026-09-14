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

package v1alpha1

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
)

// NodeType identifies the execution pattern of a swarm node
type NodeType string

const (
	NodeTypeWorker NodeType = "Worker"
	NodeTypeHITL   NodeType = "HumanInTheLoop"
)

// MCPEndpoint defines shared MCP server service connections
type MCPEndpoint struct {
	Name string `json:"name"`
	URL  string `json:"url"`
}

// AgentNode defines a single DAG node execution unit
type AgentNode struct {
	Name         string        `json:"name"`
	Type         NodeType      `json:"type,omitempty"`
	Image        string        `json:"image,omitempty"`
	Instructions string        `json:"instructions,omitempty"`
	DependsOn    []string      `json:"dependsOn,omitempty"`
	Replicas     int32         `json:"replicas,omitempty"`
	MCPServices  []MCPEndpoint `json:"mcpServices,omitempty"`
	AllowedTools []string      `json:"allowedTools,omitempty"`
}

// KafkaConfig defines access configuration for the external Kafka cluster
type KafkaConfig struct {
	Brokers []string `json:"brokers"`
}

// EDIT THIS FILE!  THIS IS SCAFFOLDING FOR YOU TO OWN!
// NOTE: json tags are required.  Any new fields you add must have json tags for the fields to be serialized.

// AgentSwarmSpec defines the desired state of AgentSwarm
type AgentSwarmSpec struct {
	// INSERT ADDITIONAL SPEC FIELDS - desired state of cluster
	// Important: Run "make" to regenerate code after modifying this file
	// The following markers will use OpenAPI v3 schema to validate the value
	// More info: https://book.kubebuilder.io/reference/markers/crd-validation.html

	// foo is an example field of AgentSwarm. Edit agentswarm_types.go to remove/update
	// +optional
	Kafka KafkaConfig `json:"kafka"`
	Nodes []AgentNode `json:"nodes"`
}

// AgentSwarmStatus defines the observed state of AgentSwarm.
type AgentSwarmStatus struct {
	// INSERT ADDITIONAL STATUS FIELD - define observed state of cluster
	// Important: Run "make" to regenerate code after modifying this file

	// For Kubernetes API conventions, see:
	// https://github.com/kubernetes/community/blob/master/contributors/devel/sig-architecture/api-conventions.md#typical-status-properties

	// conditions represent the current state of the AgentSwarm resource.
	// Each condition has a unique type and reflects the status of a specific aspect of the resource.
	//
	// Standard condition types include:
	// - "Available": the resource is fully functional
	// - "Progressing": the resource is being created or updated
	// - "Degraded": the resource failed to reach or maintain its desired state
	//
	// The status of each condition is one of True, False, or Unknown.
	// +listType=map
	// +listMapKey=type
	// +optional
	Conditions  []metav1.Condition `json:"conditions,omitempty"`
	ActiveNodes int32              `json:"activeNodes,omitempty"`
}

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status

// AgentSwarm is the Schema for the agentswarms API
type AgentSwarm struct {
	metav1.TypeMeta `json:",inline"`

	// metadata is a standard object metadata
	// +optional
	metav1.ObjectMeta `json:"metadata,omitzero"`

	// spec defines the desired state of AgentSwarm
	// +required
	Spec AgentSwarmSpec `json:"spec"`

	// status defines the observed state of AgentSwarm
	// +optional
	Status AgentSwarmStatus `json:"status,omitzero"`
}

// +kubebuilder:object:root=true

// AgentSwarmList contains a list of AgentSwarm
type AgentSwarmList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitzero"`
	Items           []AgentSwarm `json:"items"`
}

func init() {
	SchemeBuilder.Register(func(s *runtime.Scheme) error {
		s.AddKnownTypes(SchemeGroupVersion, &AgentSwarm{}, &AgentSwarmList{})
		return nil
	})
}
