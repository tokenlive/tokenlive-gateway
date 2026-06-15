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

// KubernetesDiscoveryConfig Kubernetes 服务发现配置
type KubernetesDiscoveryConfig struct {
	Namespace     string `json:"namespace" yaml:"namespace"`           // K8s 命名空间
	LabelSelector string `json:"label_selector" yaml:"label_selector"` // 标签选择器
	FieldSelector string `json:"field_selector" yaml:"field_selector"` // 字段选择器
	Port          int    `json:"port" yaml:"port"`                     // 服务端口
	Scheme        string `json:"scheme" yaml:"scheme"`                 // http 或 https
	KubeConfig    string `json:"kube_config" yaml:"kube_config"`       // kubeconfig 路径（可选）
}

// KubernetesDiscovery Kubernetes 服务发现实现
type KubernetesDiscovery struct {
	client    *kubernetes.Clientset
	config    *KubernetesDiscoveryConfig
	endpoints map[string][]*Endpoint        // model -> endpoints
	watchers  map[string]context.CancelFunc // model -> cancel function
	mu        sync.RWMutex
}

// NewKubernetesDiscovery 创建 Kubernetes 服务发现
func NewKubernetesDiscovery(config *KubernetesDiscoveryConfig) (*KubernetesDiscovery, error) {
	var restConfig *rest.Config
	var err error

	// 如果指定了 kubeconfig 路径，使用文件配置
	if config.KubeConfig != "" {
		restConfig, err = clientcmd.BuildConfigFromFlags("", config.KubeConfig)
	} else {
		// 否则使用集群内配置
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

// List 实现 core.Discovery 接口
func (kd *KubernetesDiscovery) List(ctx context.Context, model string) ([]*Endpoint, error) {
	// 查询 Pods
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
		// 只返回运行中的 Pod
		if pod.Status.Phase != "Running" {
			continue
		}

		// 跳过没有 IP 的 Pod
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
		// 添加 Pod 标签到元数据
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
			Healthy:          true, // 默认运行态的 Pod 为健康
		}

		endpoints = append(endpoints, ep)
	}

	// 更新缓存
	kd.mu.Lock()
	kd.endpoints[model] = endpoints
	kd.mu.Unlock()

	return endpoints, nil
}

// Watch 实现 core.Discovery 接口
func (kd *KubernetesDiscovery) Watch(ctx context.Context, model string) (<-chan []*Endpoint, error) {
	endpointsChan := make(chan []*Endpoint, 10)

	// 先推送当前实例列表
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

	// 启动 watch
	go func() {
		defer close(endpointsChan)

		for {
			select {
			case <-ctx.Done():
				return
			default:
				if err := kd.watchPods(ctx, model, endpointsChan); err != nil {
					// Watch 失败，等待后重试
					time.Sleep(5 * time.Second)
				}
			}
		}
	}()

	return endpointsChan, nil
}

// watchPods 监听 Pod 变化
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

			// Pod 发生变化，重新获取实例列表
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

// Close 实现 core.Discovery 接口
func (kd *KubernetesDiscovery) Close() error {
	kd.mu.Lock()
	defer kd.mu.Unlock()

	// 取消所有 watchers
	for _, cancel := range kd.watchers {
		cancel()
	}
	kd.watchers = make(map[string]context.CancelFunc)

	return nil
}
