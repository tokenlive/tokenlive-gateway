# Redis Key 结构与懒加载机制

AdminProject 将已合并的扁平化 ModelProvider 数据写入 Redis，网关按需懒加载。

## Key 结构

```
aigw:config:version                          → String (递增版本号)
aigw:config:models:<model_name>              → String (JSON, 给 AdminProject 自用)
aigw:config:providers:<provider_name>        → String (JSON, 给 AdminProject 自用)
aigw:config:model_providers:<model_name>     → String (JSON array, 网关读取)
```

网关只读 `model_providers:<model_name>`，该 key 存储的是已合并继承字段后的扁平数据（ResolvedModelProvider 列表）。`models` 和 `providers` key 由 AdminProject 维护，网关不读取。

## 加载机制

- **懒加载**：请求到来时，如果内存缓存没有该 model 的数据，从 Redis 读取 `aigw:config:model_providers:<model_name>` 并缓存
- **版本轮询**：后台每 30 秒（可配置）读取 `aigw:config:version`，版本变更时清空内存缓存
- **热加载**：缓存清空后，下次请求触发重新从 Redis 拉取，通过 `Engine.UpdateConfig()` 原子切换 Pipeline

## Considered Options

1. **启动时全量加载** — 启动时 SCAN 所有 `model_providers:*` key 并加载到内存。启动慢但请求无 Redis 开销
2. **懒加载 + 版本轮询** — 选中。启动快，只加载实际被请求的 model，版本轮询保证缓存一致性
3. **每次请求都读 Redis** — 最实时但每次请求多一次 Redis GET，延迟高

## Consequences

- 首次请求每个 model 有一次 Redis GET 延迟（通常 < 5ms）
- 版本轮询间隔决定了配置变更的最大延迟（默认 30 秒）
- AdminProject 写入 Redis 时必须递增 `aigw:config:version`，否则网关感知不到变更
