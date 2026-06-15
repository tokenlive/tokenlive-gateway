package core

import (
	"context"
)

// Discovery 网关层的服务发现接口
// 提供模型维度的服务端点获取与动态监听
type Discovery interface {
	// List 返回支持指定 model 的所有端点
	List(ctx context.Context, model string) ([]*Endpoint, error)
	// Watch 监听支持指定 model 的端点变化
	Watch(ctx context.Context, model string) (<-chan []*Endpoint, error)
	// Close 关闭发现服务
	Close() error
}

// ProviderConfig 提供者配置，描述一个 LLM 提供者及其支持的模型
type ProviderConfig struct {
	Name         string        // 提供者名称，同时作为 serviceName 传给底层 ServiceDiscovery
	Type         string        // 提供者类型，例如 "openai", "anthropic"
	Models       []string      // 该提供者支持的模型列表
	RequestTypes []RequestType // 该提供者支持的请求类型
}
