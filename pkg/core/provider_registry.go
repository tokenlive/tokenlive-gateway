package core

import (
	"fmt"
	"sync"
)

// ProviderFactory is a constructor function for creating a Provider.
type ProviderFactory func(name, baseURL, apiKey string, models []string) Provider

var (
	providerFactories = make(map[ProviderType]ProviderFactory)
	factoryMu         sync.RWMutex
)

// RegisterProviderFactory registers a factory for a provider type.
// Typically called from init() in provider implementation packages.
func RegisterProviderFactory(providerType ProviderType, factory ProviderFactory) {
	factoryMu.Lock()
	defer factoryMu.Unlock()
	if _, exists := providerFactories[providerType]; exists {
		panic(fmt.Sprintf("provider factory already registered: %s", providerType))
	}
	providerFactories[providerType] = factory
}

// GetProviderFactory returns the registered factory for a provider type.
func GetProviderFactory(providerType ProviderType) (ProviderFactory, bool) {
	factoryMu.RLock()
	defer factoryMu.RUnlock()
	f, ok := providerFactories[providerType]
	return f, ok
}

// RegisteredProviderTypes returns all registered provider types.
func RegisteredProviderTypes() []ProviderType {
	factoryMu.RLock()
	defer factoryMu.RUnlock()
	types := make([]ProviderType, 0, len(providerFactories))
	for t := range providerFactories {
		types = append(types, t)
	}
	return types
}

// RequestInvoker 专有请求调用处理器，负责特定厂商的特定接口类型
type RequestInvoker interface {
	Invoke(gctx *GatewayContext, p Provider) error
}

var (
	invokerRegistry = make(map[string]RequestInvoker)
	invokerMu       sync.RWMutex
)

// RegisterRequestInvoker 注册一个请求调用器
func RegisterRequestInvoker(providerType ProviderType, requestType RequestType, invoker RequestInvoker) {
	invokerMu.Lock()
	defer invokerMu.Unlock()
	key := fmt.Sprintf("%s:%s", providerType, requestType)
	if _, exists := invokerRegistry[key]; exists {
		panic(fmt.Sprintf("request invoker already registered: %s", key))
	}
	invokerRegistry[key] = invoker
}

// GetRequestInvoker 获取一个注册的请求调用器
func GetRequestInvoker(providerType ProviderType, requestType RequestType) (RequestInvoker, bool) {
	invokerMu.RLock()
	defer invokerMu.RUnlock()
	key := fmt.Sprintf("%s:%s", providerType, requestType)
	invoker, ok := invokerRegistry[key]
	return invoker, ok
}

// ProviderRegistry 管理实例化的 Provider 单例，确保根据提供者名称和 URL 复用
type ProviderRegistry struct {
	impls   map[string]Provider
	implsMu sync.RWMutex
}

// NewProviderRegistry 创建 ProviderRegistry
func NewProviderRegistry(initialImpls map[string]Provider) *ProviderRegistry {
	if initialImpls == nil {
		initialImpls = make(map[string]Provider)
	}
	return &ProviderRegistry{
		impls: initialImpls,
	}
}

// GetOrCreateProvider 根据 Endpoint 获取或按需构建 Provider
func (pr *ProviderRegistry) GetOrCreateProvider(ep *Endpoint) Provider {
	if ep == nil || ep.Provider == "" || ep.ProviderProtocol == "" {
		return nil
	}

	providerCacheKey := ep.Provider + "|" + ep.ProviderProtocol + "|" + ep.URL + "|" + ep.APIKey
	pr.implsMu.RLock()
	cached, ok := pr.impls[providerCacheKey]
	pr.implsMu.RUnlock()
	if ok {
		return cached
	}

	pr.implsMu.Lock()
	defer pr.implsMu.Unlock()
	// 双检锁
	if cached, ok = pr.impls[providerCacheKey]; ok {
		return cached
	}

	pt := ProviderType(ep.ProviderProtocol)
	factory, hasFactory := GetProviderFactory(pt)
	if !hasFactory {
		return nil
	}

	// 动态实例化
	impl := factory(ep.Provider, ep.URL, ep.APIKey, []string{ep.Model})
	pr.impls[providerCacheKey] = impl
	return impl
}
