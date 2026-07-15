package core

import (
	"context"
	"fmt"
	"net/http"
	"sync"
	"time"

	"go.uber.org/zap"
)

// HealthStatus 健康状态
type HealthStatus string

const (
	HealthStatusHealthy   HealthStatus = "healthy"
	HealthStatusUnhealthy HealthStatus = "unhealthy"
	HealthStatusUnknown   HealthStatus = "unknown"
)

// StaticDiscovery 静态服务发现（直接从配置读取，使用模型 model 为 key）
type StaticDiscovery struct {
	endpoints           map[string][]*Endpoint // model -> endpoints
	mu                  sync.RWMutex
	endpointCheckStates sync.Map // 细粒度探活状态缓存
}

// NewStaticDiscovery 创建静态服务发现
func NewStaticDiscovery() *StaticDiscovery {
	return &StaticDiscovery{
		endpoints: make(map[string][]*Endpoint),
	}
}

// RegisterService 注册服务端点
func (sd *StaticDiscovery) RegisterService(model string, endpoints []*Endpoint) {
	sd.mu.Lock()
	defer sd.mu.Unlock()
	sd.endpoints[model] = endpoints
}

// List 实现 core.Discovery 接口
func (sd *StaticDiscovery) List(ctx context.Context, model string) ([]*Endpoint, error) {
	sd.mu.RLock()
	defer sd.mu.RUnlock()

	endpoints, exists := sd.endpoints[model]
	if !exists {
		return nil, fmt.Errorf("model not found in static config: %s", model)
	}

	// 返回副本，避免外部修改
	result := make([]*Endpoint, len(endpoints))
	copy(result, endpoints)
	return result, nil
}

// Watch 实现 core.Discovery 接口（静态发现不支持 watch，只推送一次）
func (sd *StaticDiscovery) Watch(ctx context.Context, model string) (<-chan []*Endpoint, error) {
	ch := make(chan []*Endpoint, 1)

	go func() {
		defer close(ch)
		endpoints, err := sd.List(ctx, model)
		if err == nil {
			select {
			case ch <- endpoints:
			case <-ctx.Done():
			}
		}
	}()

	return ch, nil
}

// Close 实现 core.Discovery 接口
func (sd *StaticDiscovery) Close() error {
	return nil
}

// UpdateHealthAll 更新指定模型所有实例的健康状态
func (sd *StaticDiscovery) UpdateHealthAll(model string, health HealthStatus) {
	sd.mu.Lock()
	defer sd.mu.Unlock()

	// 这里的 model 参数其实是 Provider Name
	for _, eps := range sd.endpoints {
		for _, ep := range eps {
			if ep.Provider == model {
				ep.Healthy = health == HealthStatusHealthy
			}
		}
	}
}

// GetAllEndpoints 返回静态服务发现注册的所有端点的唯一列表
func (sd *StaticDiscovery) GetAllEndpoints() []*Endpoint {
	sd.mu.RLock()
	defer sd.mu.RUnlock()

	var result []*Endpoint
	seen := make(map[string]bool)
	for _, eps := range sd.endpoints {
		for _, ep := range eps {
			if !seen[ep.ID] {
				seen[ep.ID] = true
				result = append(result, ep)
			}
		}
	}
	return result
}

type endpointCheckState struct {
	lastCheck    time.Time
	successCount int
}

// StartHealthCheck 启动粗粒度和细粒度的健康检测协程
func (sd *StaticDiscovery) StartHealthCheck(
	ctx context.Context,
	providers map[string]Provider,
	cbManager *CircuitBreakerManager,
	logger *zap.Logger,
	interval time.Duration,
	enableActive bool,
) {
	// 1. 启动原有粗粒度 Provider 健康检查
	if len(providers) > 0 {
		go func() {
			ticker := time.NewTicker(interval)
			defer ticker.Stop()
			for {
				select {
				case <-ctx.Done():
					return
				case <-ticker.C:
					sd.runHealthChecks(ctx, providers, logger)
				}
			}
		}()
	}

	// 2. 启动细粒度 Endpoint 自适应探测健康检查
	if enableActive {
		go func() {
			ticker := time.NewTicker(5 * time.Second)
			defer ticker.Stop()
			for {
				select {
				case <-ctx.Done():
					return
				case <-ticker.C:
					sd.runEndpointHealthChecks(ctx, cbManager, logger)
				}
			}
		}()
	}
}

func (sd *StaticDiscovery) runHealthChecks(ctx context.Context, providers map[string]Provider, logger *zap.Logger) {
	for name, provider := range providers {
		checkCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
		err := provider.HealthCheck(checkCtx)
		cancel()

		health := HealthStatusHealthy
		if err != nil {
			health = HealthStatusUnhealthy
			logger.Warn("provider health check failed",
				zap.String("provider", name),
				zap.Error(err),
			)
		}
		sd.UpdateHealthAll(name, health)
	}
}

func (sd *StaticDiscovery) runEndpointHealthChecks(ctx context.Context, cbManager *CircuitBreakerManager, logger *zap.Logger) {
	endpoints := sd.GetAllEndpoints()
	now := time.Now()

	for _, ep := range endpoints {
		url := ep.Metadata["health_check_url"]
		if url == "" {
			continue
		}

		state := cbManager.GetState(ep.ID)

		var lastCheck time.Time
		var successCount int
		if val, ok := sd.endpointCheckStates.Load(ep.ID); ok {
			s := val.(*endpointCheckState)
			lastCheck = s.lastCheck
			successCount = s.successCount
		}

		// 自适应频次：健康(Closed)探测间隔 30s，非健康(Open/HalfOpen)探测间隔 5s
		interval := 30 * time.Second
		if state == CircuitOpen || state == CircuitHalfOpen {
			interval = 5 * time.Second
		}

		if now.Sub(lastCheck) < interval {
			continue
		}

		// 异步进行健康探测，避免慢端点阻碍主检查循环
		go func(epCopy *Endpoint, currentSuccessCount int) {
			err := sd.probeEndpoint(ctx, epCopy)

			var newSuccessCount int
			if err == nil {
				newSuccessCount = currentSuccessCount + 1
				logger.Debug("endpoint health check success",
					zap.String("endpoint_id", epCopy.ID),
					zap.Int("success_count", newSuccessCount))

				// 连续 3 次探测成功，强设熔断器为 Closed (恢复健康)
				if newSuccessCount >= 3 {
					logger.Info("endpoint health check success 3 times consecutively, resetting circuit breaker",
						zap.String("endpoint_id", epCopy.ID))
					cbManager.Reset(epCopy.ID)
					serviceKey := epCopy.Provider + ":" + epCopy.Model
					cbManager.Reset(serviceKey)
					newSuccessCount = 0
				}
			} else {
				newSuccessCount = 0
				logger.Warn("endpoint health check failed",
					zap.String("endpoint_id", epCopy.ID),
					zap.Error(err))
			}

			sd.endpointCheckStates.Store(epCopy.ID, &endpointCheckState{
				lastCheck:    time.Now(),
				successCount: newSuccessCount,
			})
		}(ep, successCount)
	}
}

func (sd *StaticDiscovery) probeEndpoint(ctx context.Context, ep *Endpoint) error {
	url := ep.Metadata["health_check_url"]
	if url == "" {
		return nil
	}

	client := &http.Client{
		Timeout: 3 * time.Second,
	}

	req, err := http.NewRequestWithContext(ctx, "GET", url, nil)
	if err != nil {
		return err
	}

	// 传递必要的认证信息以通过安全校验
	if ep.APIKey != "" {
		if ep.AuthType == "oauth_token" {
			req.Header.Set("Authorization", "Bearer "+ep.APIKey)
			if ep.ProviderProtocol == "anthropic" {
				req.Header.Set("anthropic-version", "2023-06-01")
			}
		} else {
			if ep.ProviderProtocol == "openai" {
				req.Header.Set("Authorization", "Bearer "+ep.APIKey)
			} else if ep.ProviderProtocol == "anthropic" {
				req.Header.Set("x-api-key", ep.APIKey)
				req.Header.Set("anthropic-version", "2023-06-01")
			}
		}
	}

	resp, err := client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return fmt.Errorf("http status code: %d", resp.StatusCode)
	}

	return nil
}
