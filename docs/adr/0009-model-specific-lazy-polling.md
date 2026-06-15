# ADR-0009: 细粒度模型版本控制与网关动态按需轮询

将模型端点路由配置的版本控制从“全局单一版本号（aigw:config:version）”改为“哈希表多模型细粒度版本（aigw:config:model_versions）”，网关由“全局粗粒度清空”升级为“基于活跃缓存模型列表的动态 HMGET 轮询局部失效”。

## Context

在 ADR-0008 中，引入了 `aigw:config:version` 来检测后台端点配置的变化。然而，这种全局版本号引发了显著的“过度驱逐 (Over-eviction)”痛点：

- **缓存并发击穿 (Cache Stampede) 风险**：哪怕仅修改或下线了一个低吞吐的非核心模型，都会导致网关把高并发的核心模型缓存（如 `gpt-4`）整只强行驱逐。网关在随后的请求中面临大规模冷启动加载，对 Redis 发起密集查询，造成响应时间抖动。
- **轮询开销风险**：网关无法采用全量 `HGETALL` 对接庞大的模型配置，否则随着模型名增长，轮询网络载荷和 CPU 序列化开销将以 $O(N)$ 恶化。

我们需要一种对 Redis 极其温和、网络延迟极低、且只按需清理变动模型本地缓存的精细控制方案。

## Considered Options

1. **Pub/Sub (发布订阅模式)** — 实时性高，但 Pub/Sub 是“最多发送一次”的不可靠连接。若网关断线重连，极易丢失版本通知造成数据不一致，仍需要额外的定期轮询拉取作为备用兜底。
2. **全局哈希 + 网关全量轮询 (HGETALL)** — 解决了精准清理问题。但网关需高频轮询所有模型的最新版本，对于规模化部署（多节点、万级动态模型），对 Redis 单线程和网络吞吐会造成不必要的浪费。
3. **全局哈希 + 动态按需轮询 (HMGET) (选中)** — 管理后台更新特定模型时，仅递增 `aigw:config:model_versions` 中该模型 field 的数值。网关轮询时，仅收集**当前本地内存中已缓存的活跃模型**，使用 `HMGET` 原子命令按需获取这些活跃模型的最新版本，并实施“精准失效”。

## Decision

### Redis 数据契约变更

| Key | 类型 | 说明 |
|-----|------|------|
| `aigw:config:model_versions` | HASH | 存储每个模型及其自增版本号（Field: modelName, Value: int64） |
| `aigw:config:endpoints:{modelName}` | STRING (JSON) | 对应模型的 `[]ResolvedEndpoint` 配置数据（不变） |

### 写入端 (Admin) 变更

当管理员对某个模型的端点进行增、删、改或上下线操作时：
- 自增版本：`HINCRBY aigw:config:model_versions <modelName> 1`
- 逻辑删除/清空：`HDEL aigw:config:model_versions <modelName>`

### 读取与轮询端 (Gateway) 变更

- **懒加载 (Cache Miss) 组合查询**：
  网关在读取配置时，通过 **Pipeline** 确保在 **1 RTT** 内并发拉取 `GET aigw:config:endpoints:{modelName}` 和 `HGET aigw:config:model_versions {modelName}`。如果取得，将数据放入 `cache`，版本号放入 `lastVersions` 映射。

- **动态 HMGET 按需轮询**：
  网关定时协程（默认 30s）执行：
  1. 通过 `KnownModels()` 获得当前缓存的 `activeModels` 列表。
  2. 若列表为空，**直接跳过轮询（0 RTT）**，达到完美的空闲伸缩。
  3. 若列表非空，发起 `HMGET aigw:config:model_versions activeModels...` 查询。
  4. 遍历查询结果，仅针对本地版本与 Redis 远程版本不一致的**变动模型进行精准驱逐 (`delete(r.cache, model)`)**，其余保持原样。

## Consequences

- **极高的一致性与隔离性**：某个模型的配置变更只会导致该模型本身的本地缓存失效并重新懒加载，其他模型的常驻缓存不受任何波及，彻底消除了全局缓存击穿风险。
- **极致的性能与高并发友好**：网关空闲时 0 轮询；网关运行时仅对已活跃的缓存发起小数据量查验，网关 QPS 与 Redis 带宽开销完全实现 $O(K)$（K 为活跃模型数）的极致收敛，具有无限的前向可伸缩性。
- **垃圾回收 (GC)**：当模型被后台清理删除时，其关联版本信息从 Hash 中删除，网关拉取为 `nil` 即可顺畅驱逐清理本地死信。
