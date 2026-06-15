# 配置数据源分层：YAML 默认层 + Redis 覆盖层

网关的 model_providers 配置数据来自两个层：YAML 作为默认层保证网关一定能启动，Redis 作为覆盖层由 AdminProject 维护。同一 model 两层都有时用 Redis 的，只有 YAML 有的保留。

## Context

网关需要知道"哪些 model 可以从哪些 provider 获取"。这些配置由独立的 AdminProject 通过管理 API 维护，写入 Redis。但网关启动时 Redis 可能不可用或还没有数据（首次部署、本地调试），需要一种 fallback 机制。

## Decision

- YAML 配置是默认层，启动时立即加载，保证网关可用
- Redis 是覆盖层，AdminProject 将已合并的扁平化 ModelProvider 数据写入 Redis
- 启动时不阻塞等待 Redis，用 YAML 基线先启动
- 运行时懒加载：请求某个 model 时，如果内存缓存没有，从 Redis 拉取该 model 的 model_providers
- 后台轮询版本号（默认 30 秒，可配置），版本变更时清空内存缓存，下次请求重新从 Redis 拉取
- Redis 不可用时保留当前缓存；缓存也空时回退到 YAML 数据

## Considered Options

1. **纯 YAML** — 简单但不支持运行时变更，AdminProject 无法动态推送配置
2. **纯 Redis** — 启动时必须连接 Redis，首次部署或调试不方便
3. **YAML 默认 + Redis 覆盖** — 选中。兼顾启动可靠性和运行时灵活性

## Consequences

- 网关启动依赖 YAML 配置（至少需要一个最小配置）
- AdminProject 需要实现 Redis 写入逻辑，网关和 AdminProject 共享 Redis key 约定
- 配置变更最多有 30 秒延迟（轮询间隔）
