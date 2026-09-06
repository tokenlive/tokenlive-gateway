package core

import (
	"context"
	"fmt"
	"net/http"
	"sync"
	"time"

	"go.uber.org/zap"
)

// HealthStatus represents health status.
type HealthStatus string

const (
	HealthStatusHealthy   HealthStatus = "healthy"
	HealthStatusUnhealthy HealthStatus = "unhealthy"
	HealthStatusUnknown   HealthStatus = "unknown"
)

// StaticDiscovery reads endpoints directly from config, keyed by model.
type StaticDiscovery struct {
	endpoints           map[string][]*Endpoint // model -> endpoints
	mu                  sync.RWMutex
	endpointCheckStates sync.Map // fine-grained health-check state cache
}

// NewStaticDiscovery creates a static discovery.
func NewStaticDiscovery() *StaticDiscovery {
	return &StaticDiscovery{
		endpoints: make(map[string][]*Endpoint),
	}
}

// RegisterService registers service endpoints.
func (sd *StaticDiscovery) RegisterService(model string, endpoints []*Endpoint) {
	sd.mu.Lock()
	defer sd.mu.Unlock()
	sd.endpoints[model] = endpoints
}

// List implements core.Discovery.
func (sd *StaticDiscovery) List(ctx context.Context, model string) ([]*Endpoint, error) {
	sd.mu.RLock()
	defer sd.mu.RUnlock()

	endpoints, exists := sd.endpoints[model]
	if !exists {
		return nil, fmt.Errorf("model not found in static config: %s", model)
	}

	// Return a copy to prevent external mutation
	result := make([]*Endpoint, len(endpoints))
	copy(result, endpoints)
	return result, nil
}

// Watch implements core.Discovery (static discovery does not support watching; pushes once only).
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

// Close implements core.Discovery.
func (sd *StaticDiscovery) Close() error {
	return nil
}

// UpdateHealthAll updates the health status of all instances for the given model.
func (sd *StaticDiscovery) UpdateHealthAll(model string, health HealthStatus) {
	sd.mu.Lock()
	defer sd.mu.Unlock()

	// The model parameter here is actually the Provider Name
	for _, eps := range sd.endpoints {
		for _, ep := range eps {
			if ep.Provider == model {
				ep.Healthy = health == HealthStatusHealthy
			}
		}
	}
}

// GetAllEndpoints returns the deduplicated list of all registered endpoints.
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

// StartHealthCheck starts coarse-grained and fine-grained health check goroutines.
func (sd *StaticDiscovery) StartHealthCheck(
	ctx context.Context,
	providers func() map[string]Provider,
	cbManager *CircuitBreakerManager,
	logger *zap.Logger,
	interval time.Duration,
	enableActive bool,
) {
	if providers == nil {
		providers = func() map[string]Provider { return nil }
	}
	if interval <= 0 {
		interval = 30 * time.Second
	}

	// 1. Start coarse-grained Provider health check. Re-read providers each tick so
	// hot reload (SetProviders) does not leave a stale map such as openai-local.
	go func() {
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				sd.runHealthChecks(ctx, providers(), logger)
			}
		}
	}()

	// 2. Start fine-grained adaptive Endpoint health check
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

		// Adaptive frequency: Closed (healthy) probes every 30s, Open/HalfOpen probes every 5s
		interval := 30 * time.Second
		if state == CircuitOpen || state == CircuitHalfOpen {
			interval = 5 * time.Second
		}

		if now.Sub(lastCheck) < interval {
			continue
		}

		// Probe asynchronously to avoid slow endpoints blocking the main check loop
		go func(epCopy *Endpoint, currentSuccessCount int) {
			err := sd.probeEndpoint(ctx, epCopy)

			var newSuccessCount int
			if err == nil {
				newSuccessCount = currentSuccessCount + 1
				logger.Debug("endpoint health check success",
					zap.String("endpoint_id", epCopy.ID),
					zap.Int("success_count", newSuccessCount))

				// 3 consecutive probe successes force circuit breaker to Closed (recovered)
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

	// Pass necessary auth credentials to pass security validation
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
