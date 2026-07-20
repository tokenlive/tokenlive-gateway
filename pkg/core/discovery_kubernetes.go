package core

import (
	"context"
	"fmt"
	"strconv"
	"sync"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/watch"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
)

// KubernetesDiscoveryConfig holds Kubernetes service discovery configuration.
type KubernetesDiscoveryConfig struct {
	Namespace     string `json:"namespace" yaml:"namespace"`           // K8s namespace
	LabelSelector string `json:"label_selector" yaml:"label_selector"` // label selector
	FieldSelector string `json:"field_selector" yaml:"field_selector"` // field selector
	Port          int    `json:"port" yaml:"port"`                     // service port
	Scheme        string `json:"scheme" yaml:"scheme"`                 // http or https
	KubeConfig    string `json:"kube_config" yaml:"kube_config"`       // kubeconfig path (optional)
}

// KubernetesDiscovery implements Kubernetes service discovery.
type KubernetesDiscovery struct {
	client    *kubernetes.Clientset
	config    *KubernetesDiscoveryConfig
	endpoints map[string][]*Endpoint        // model -> endpoints
	watchers  map[string]context.CancelFunc // model -> cancel function
	mu        sync.RWMutex
}

// NewKubernetesDiscovery creates Kubernetes service discovery.
func NewKubernetesDiscovery(config *KubernetesDiscoveryConfig) (*KubernetesDiscovery, error) {
	var restConfig *rest.Config
	var err error

	if config.KubeConfig != "" {
		restConfig, err = clientcmd.BuildConfigFromFlags("", config.KubeConfig)
	} else {
		// Use in-cluster config
		restConfig, err = rest.InClusterConfig()
	}

	if err != nil {
		return nil, fmt.Errorf("failed to create k8s config: %w", err)
	}

	clientset, err := kubernetes.NewForConfig(restConfig)
	if err != nil {
		return nil, fmt.Errorf("failed to create k8s client: %w", err)
	}

	return &KubernetesDiscovery{
		client:    clientset,
		config:    config,
		endpoints: make(map[string][]*Endpoint),
		watchers:  make(map[string]context.CancelFunc),
	}, nil
}

// List implements core.Discovery.
func (kd *KubernetesDiscovery) List(ctx context.Context, model string) ([]*Endpoint, error) {
	listOptions := metav1.ListOptions{}
	if kd.config.LabelSelector != "" {
		listOptions.LabelSelector = kd.config.LabelSelector
	}
	if kd.config.FieldSelector != "" {
		listOptions.FieldSelector = kd.config.FieldSelector
	}

	pods, err := kd.client.CoreV1().Pods(kd.config.Namespace).List(ctx, listOptions)
	if err != nil {
		return nil, fmt.Errorf("failed to list pods: %w", err)
	}

	endpoints := make([]*Endpoint, 0)
	for _, pod := range pods.Items {
		// Only return running pods
		if pod.Status.Phase != "Running" {
			continue
		}

		// Skip pods without an IP
		if pod.Status.PodIP == "" {
			continue
		}

		scheme := kd.config.Scheme
		if scheme == "" {
			scheme = "http"
		}
		var rawURL string
		if kd.config.Port <= 0 {
			rawURL = fmt.Sprintf("%s://%s", scheme, pod.Status.PodIP)
		} else {
			rawURL = fmt.Sprintf("%s://%s:%d", scheme, pod.Status.PodIP, kd.config.Port)
		}

		weight := 1
		if weightStr, ok := pod.Annotations["load-balancer.weight"]; ok {
			if w, err := strconv.Atoi(weightStr); err == nil && w > 0 {
				weight = w
			}
		}

		provider := pod.Annotations["github.com/tokenlive/tokenlive-gateway.provider"]
		providerProtocol := pod.Annotations["github.com/tokenlive/tokenlive-gateway.provider-protocol"]
		apiKey := pod.Annotations["github.com/tokenlive/tokenlive-gateway.api-key"]
		upstreamModel := pod.Annotations["github.com/tokenlive/tokenlive-gateway.upstream-model"]

		metadata := map[string]string{
			"pod_name":      pod.Name,
			"namespace":     pod.Namespace,
			"node_name":     pod.Spec.NodeName,
			"creation_time": pod.CreationTimestamp.String(),
		}
		// Add pod labels to metadata
		for k, v := range pod.Labels {
			metadata["label."+k] = v
		}

		apis := []RequestType{
			RequestTypeChatCompletion,
			RequestTypeEmbedding,
		}

		ep := &Endpoint{
			ID:               string(pod.UID),
			URL:              rawURL,
			Provider:         provider,
			ProviderProtocol: providerProtocol,
			APIKey:           apiKey,
			Model:            model,
			UpstreamModel:    upstreamModel,
			Metadata:         metadata,
			Weight:           weight,
			RequestTypes:     apis,
			Healthy:          true, // running pods are healthy by default
		}

		endpoints = append(endpoints, ep)
	}

	// Update cache
	kd.mu.Lock()
	kd.endpoints[model] = endpoints
	kd.mu.Unlock()

	return endpoints, nil
}

// Watch implements core.Discovery.
func (kd *KubernetesDiscovery) Watch(ctx context.Context, model string) (<-chan []*Endpoint, error) {
	endpointsChan := make(chan []*Endpoint, 10)

	// Push current instance list first
	endpoints, err := kd.List(ctx, model)
	if err != nil {
		close(endpointsChan)
		return nil, err
	}

	select {
	case endpointsChan <- endpoints:
	case <-ctx.Done():
		close(endpointsChan)
		return nil, ctx.Err()
	}

	// Start watching
	go func() {
		defer close(endpointsChan)

		for {
			select {
			case <-ctx.Done():
				return
			default:
				if err := kd.watchPods(ctx, model, endpointsChan); err != nil {
					// Watch failed; wait and retry
					time.Sleep(5 * time.Second)
				}
			}
		}
	}()

	return endpointsChan, nil
}

// watchPods watches pod changes.
func (kd *KubernetesDiscovery) watchPods(ctx context.Context, model string, endpointsChan chan<- []*Endpoint) error {
	listOptions := metav1.ListOptions{}
	if kd.config.LabelSelector != "" {
		listOptions.LabelSelector = kd.config.LabelSelector
	}
	if kd.config.FieldSelector != "" {
		listOptions.FieldSelector = kd.config.FieldSelector
	}

	watcher, err := kd.client.CoreV1().Pods(kd.config.Namespace).Watch(ctx, listOptions)
	if err != nil {
		return fmt.Errorf("failed to watch pods: %w", err)
	}
	defer watcher.Stop()

	for {
		select {
		case event, ok := <-watcher.ResultChan():
			if !ok {
				return fmt.Errorf("watch channel closed")
			}

			// Pod changed; re-fetch instance list
			switch event.Type {
			case watch.Added, watch.Modified, watch.Deleted:
				endpoints, err := kd.List(ctx, model)
				if err != nil {
					continue
				}

				select {
				case endpointsChan <- endpoints:
				case <-ctx.Done():
					return ctx.Err()
				}
			}

		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

// Close implements core.Discovery.
func (kd *KubernetesDiscovery) Close() error {
	kd.mu.Lock()
	defer kd.mu.Unlock()

	// Cancel all watchers
	for _, cancel := range kd.watchers {
		cancel()
	}
	kd.watchers = make(map[string]context.CancelFunc)

	return nil
}
