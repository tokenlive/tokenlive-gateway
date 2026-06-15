# ADR-0010: 为延迟统计 Redis Key 添加 TTL

## 状态
已实施 (2026-06-02)

## 背景
在审查 Redis StateStore 实现时，发现延迟统计功能（`aigw:latency:*` key）存在潜在的内存泄漏风险：

1. **限流 Key** (`aigw:rl:*`) - ✅ 已有 TTL（等于滑动窗口大小）
2. **Sticky Session Key** (`aigw:sticky:*`) - ✅ 已有 TTL（会话超时时间）
3. **延迟统计 Key** (`aigw:latency:*`) - ❌ **无 TTL**，存在脏数据累积风险

### 问题分析
延迟统计 key 的清理机制仅依赖样本数量限制（`ZREMRANGEBYRANK`），但 key 本身永不过期：

- 下线的 endpoint 数据会永久保留
- 长期不活跃的 endpoint 数据持续占用内存
- 随着系统运行，废弃 endpoint 的 key 数量不断增长

**典型场景**：
```
aigw:latency:provider1:endpoint1  # 已下线 3 个月，但数据仍在
aigw:latency:provider2:endpoint5  # 已下线 6 个月，但数据仍在
...
```

## 决策
采用 **Lua 脚本原子化 TTL 设置方案**，在记录延迟样本时同步完成：

1. 添加样本到 Sorted Set (`ZADD`)
2. 设置或刷新 TTL (`PEXPIRE`)
3. 清理超出数量的旧样本 (`ZREMRANGEBYRANK`)

### TTL 配置
- **默认值**: 7 天 (`redisLatencyKeyTTL = 7 * 24 * time.Hour`)
- **刷新机制**: 每次记录延迟时刷新 TTL
- **清理效果**: 超过 7 天未更新的 endpoint 数据自动过期清理

## 实现细节

### 1. Lua 脚本 (`pkg/store/lua/record_latency.lua`)
```lua
-- 原子性完成：ZADD + PEXPIRE + ZREMRANGEBYRANK
local key = KEYS[1]
local score = tonumber(ARGV[1])
local member = ARGV[2]
local max_samples = tonumber(ARGV[3])
local ttl_ms = tonumber(ARGV[4])

redis.call('ZADD', key, score, member)
redis.call('PEXPIRE', key, ttl_ms)

local count = redis.call('ZCARD', key)
if count > max_samples then
    redis.call('ZREMRANGEBYRANK', key, 0, count - max_samples - 1)
end

return 1
```

### 2. Go 代码改动
```go
// pkg/store/redis.go

// 添加常量
const redisLatencyKeyTTL = 7 * 24 * time.Hour

// 简化 RecordLatency 方法（去掉 goroutine，使用 Lua 脚本）
func (s *RedisStateStore) RecordLatency(ctx context.Context, endpointID string, latency time.Duration) error {
    redisKey := s.key("latency", endpointID)
    now := float64(s.nowFunc().UnixMilli())
    member := fmt.Sprintf("%d:%d", latency.Nanoseconds(), rand.Int63())
    ttlMs := redisLatencyKeyTTL.Milliseconds()

    _, err := s.recordLatencyScript.Eval(ctx, s.client,
        []string{redisKey},
        now, member, s.latencyMax, ttlMs,
    ).Result()
    
    return err
}
```

### 3. 测试覆盖
新增测试用例：
- `TestRedis_Latency_TTLIsSet` - 验证 TTL 正确设置
- `TestRedis_Latency_TTLRefreshOnUpdate` - 验证更新时 TTL 刷新
- `TestRedis_Latency_MaxSamplesCleaned` - 验证样本数量限制

所有测试通过：
```bash
=== RUN   TestRedis_Latency_TTLIsSet
--- PASS: TestRedis_Latency_TTLIsSet (0.00s)
=== RUN   TestRedis_Latency_TTLRefreshOnUpdate
--- PASS: TestRedis_Latency_TTLRefreshOnUpdate (0.00s)
=== RUN   TestRedis_Latency_MaxSamplesCleaned
--- PASS: TestRedis_Latency_MaxSamplesCleaned (0.22s)
```

## 优势
1. ✅ **自动清理脏数据** - 长期不活跃的 endpoint 数据自动过期
2. ✅ **活跃数据不受影响** - TTL 在每次更新时刷新
3. ✅ **原子性保证** - Lua 脚本确保操作的原子性
4. ✅ **性能优化** - 去掉异步 goroutine，减少网络往返
5. ✅ **简化代码** - 单次脚本调用替代多次 Redis 操作

## 影响
- **向后兼容**: 现有 key 会在首次更新时自动添加 TTL
- **内存节约**: 自动清理废弃数据，避免内存泄漏
- **性能提升**: Lua 脚本原子执行，减少网络延迟

## 监控建议
定期检查延迟统计 key 的数量和内存占用：

```bash
# 统计延迟 key 数量
redis-cli --scan --pattern "aigw:latency:*" | wc -l

# 检查大 key
redis-cli --bigkeys --pattern "aigw:latency:*"

# 查看某个 key 的 TTL
redis-cli TTL "aigw:latency:provider:endpoint"
```

## 替代方案（未采纳）

### 方案 A: 简单 Expire 调用
在每次 `RecordLatency` 时调用 `client.Expire()`：
- ❌ 需要额外的网络往返
- ❌ 不是原子操作
- ✅ 实现简单

### 方案 C: 定期清理任务
通过 cron 任务扫描并清理废弃 key：
- ❌ 增加复杂度
- ❌ 清理不及时
- ❌ SCAN 操作对 Redis 有性能影响
- ✅ 可作为兜底方案

## 监控建议

### 读取 Redis 配置
```bash
# 从 config/local.yml 读取 Redis 配置
REDIS_ADDR=$(grep -A 5 "redis:" config/local.yml | grep "addr:" | awk '{print $2}')
REDIS_PASSWORD=$(grep -A 5 "redis:" config/local.yml | grep "password:" | awk '{print $2}')
REDIS_HOST=$(echo $REDIS_ADDR | cut -d: -f1)
REDIS_PORT=$(echo $REDIS_ADDR | cut -d: -f2)

# 连接 Redis
redis-cli -h $REDIS_HOST -p $REDIS_PORT -a "$REDIS_PASSWORD"
```

### 检查延迟统计 key
定期检查延迟统计 key 的数量和内存占用：

```bash
# 统计延迟 key 数量
redis-cli -h $REDIS_HOST -p $REDIS_PORT -a "$REDIS_PASSWORD" --scan --pattern "aigw:latency:*" | wc -l

# 检查大 key
redis-cli -h $REDIS_HOST -p $REDIS_PORT -a "$REDIS_PASSWORD" --bigkeys --pattern "aigw:latency:*"

# 查看某个 key 的 TTL（应该接近 7 天 = 604800 秒）
redis-cli -h $REDIS_HOST -p $REDIS_PORT -a "$REDIS_PASSWORD" TTL "aigw:latency:provider:endpoint"
```

## 参考资料
- Redis EXPIRE 文档: https://redis.io/commands/expire/
- Redis Lua 脚本: https://redis.io/docs/manual/programmability/eval-intro/
- 原始讨论: 2026-06-02 代码审查
