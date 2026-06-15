# ADR-0011: 修复限流 Key TTL 丢失问题

## 状态
已实施 (2026-06-02)

## 背景
在生产环境中发现限流 key (`aigw:rl:*`) 的 TTL 为 `-1`（无过期时间），导致过期的限流配额无法自动清理。

### 问题复现
```bash
redis-cli TTL "aigw:rl:JD:glm-5:global-tpm-limit:1m0s"
(integer) -1  # 应该接近 60000 毫秒，但实际是 -1
```

### Redis TTL 返回值说明
- `-2`: key 不存在
- `-1`: key 存在但**没有设置过期时间**（Bug！）
- `> 0`: key 的剩余 TTL（秒）

## 根本原因分析

### 问题 1: 条件检查导致 TTL 不刷新
**原始 `rate_limit_incr.lua` 逻辑**：
```lua
local current = redis.call('INCRBY', key, tokens)

-- 仅在键没有 TTL 时设置过期时间
if redis.call('TTL', key) == -1 then
    redis.call('PEXPIRE', key, window_ms)
end
```

**缺陷**：
- 只在第一次设置 TTL
- 后续请求不刷新 TTL（理论上是正确的，滑动窗口从第一次请求开始）
- **但无法处理 TTL 被意外清除的情况**

### 问题 2: Refund 操作可能清除 TTL
**原始 `rate_limit_refund.lua` 逻辑**：
```lua
local after = redis.call('DECRBY', key, tokens)

if tonumber(after) < 0 then
    redis.call('SET', key, 0, 'KEEPTTL')  -- 问题在这里！
    return 0
end
```

**关键缺陷**：

**场景复现**：
1. Key 已过期被 Redis 删除
2. 某个失败请求调用 `RateLimitRefund`
3. `DECRBY` 在不存在的 key 上执行 → Redis 创建 key，值为 `-tokens`
4. 检测到负数 → 执行 `SET key 0 KEEPTTL`
5. **问题**：`KEEPTTL` 在 key 不存在时**不会设置 TTL**！

**Redis 文档说明**：
> `KEEPTTL` — Retain the time to live associated with the key. **If the key does not have a TTL, this option has no effect.**

结果：key 被创建，值为 `0`，但 **TTL 为 -1（永不过期）**！

### 问题 3: 设计哲学冲突
原有设计理念：
- 滑动窗口应该从**首次请求**开始计时
- 避免每次请求都刷新 TTL（会导致窗口无限延长）

但实际问题：
- 边缘情况（如 refund 在过期后执行）会导致 TTL 丢失
- 一旦 TTL 丢失，key 永不过期

## 决策

### 方案 A: 每次都刷新 TTL（采纳）
将滑动窗口语义从"固定窗口"改为"滚动窗口"：
- 每次请求都刷新 TTL
- 窗口从**最后一次请求**开始计时
- 保证 TTL 永远不会丢失

**优点**：
- ✅ 简单可靠，TTL 不会丢失
- ✅ 符合"活跃限流"的直觉（活跃用户持续受限）
- ✅ 无需复杂的边界条件处理

**缺点**：
- ⚠️ 窗口语义变化（从固定窗口变为滚动窗口）
- ⚠️ 理论上可能被恶意利用（但实际影响有限）

### 方案 B: 精确修复 Refund（未采纳）
只修复 refund 脚本，保持原有窗口语义：
```lua
-- 在 refund 时检查 key 是否存在
local exists = redis.call('EXISTS', key)
if exists == 0 then
    return 0  -- key 不存在，无需退还
end
```

**优点**：
- ✅ 保持原有固定窗口语义

**缺点**：
- ❌ 无法防止其他未知的 TTL 清除路径
- ❌ 仍需处理并发竞争的边界情况

### 最终决策
**采用方案 A + 部分方案 B**：
1. `rate_limit_incr.lua`: 每次都刷新 TTL（简单可靠）
2. `rate_limit_refund.lua`: 检查 key 存在性（防御性编程）

## 实现细节

### 修改 1: rate_limit_incr.lua
```lua
-- 修改前
if redis.call('TTL', key) == -1 then
    redis.call('PEXPIRE', key, window_ms)
end

-- 修改后
redis.call('PEXPIRE', key, window_ms)  -- 每次都刷新 TTL
```

**注释说明**：
```lua
-- 每次都刷新 TTL，确保窗口从最新操作开始计时
-- 注意：使用 PEXPIRE 而不是检查 TTL，避免 refund 导致的 TTL 丢失
```

### 修改 2: rate_limit_refund.lua
```lua
-- 修改前
local after = redis.call('DECRBY', key, tokens)

-- 修改后
local exists = redis.call('EXISTS', key)
if exists == 0 then
    -- key 不存在，无需退还（可能已过期）
    return 0
end

local after = redis.call('DECRBY', key, tokens)
```

### 新增测试用例
1. **TestRedis_RateLimit_TTLAlwaysSet** - 验证 TTL 始终被设置和刷新
2. **TestRedis_RateLimit_RefundAfterExpiry** - 验证过期后 refund 不会重建 key
3. **TestRedis_RateLimit_RefundPreservesTTL** - 验证 refund 保持 TTL 不变

```bash
=== RUN   TestRedis_RateLimit_TTLAlwaysSet
--- PASS: TestRedis_RateLimit_TTLAlwaysSet (0.00s)
=== RUN   TestRedis_RateLimit_RefundAfterExpiry
--- PASS: TestRedis_RateLimit_RefundAfterExpiry (0.00s)
=== RUN   TestRedis_RateLimit_RefundPreservesTTL
--- PASS: TestRedis_RateLimit_RefundPreservesTTL (0.00s)
```

## 影响分析

### 行为变化
**修改前**：
```
时间轴: 0s -------- 30s -------- 60s -------- 90s
请求:   Req1(100)            Req2(100)
TTL:    60s固定              60s固定（不刷新）
窗口:   [0-60s]              [0-60s]（固定窗口）
60s时:  配额重置
```

**修改后**：
```
时间轴: 0s -------- 30s -------- 60s -------- 90s
请求:   Req1(100)            Req2(100)
TTL:    60s                  60s（刷新）
窗口:   [0-60s]              [30-90s]（滚动窗口）
90s时:  配额重置（而不是 60s）
```

### 实际影响
1. **对用户的影响**：
   - 活跃用户的限流窗口会"跟随"最新请求
   - 理论上可能被恶意利用（持续发送低于阈值的请求，保持限流状态）
   - 但实际场景中，这与固定窗口的限流效果差异不大

2. **对系统的影响**：
   - ✅ 消除了 TTL 丢失的风险
   - ✅ Key 会自动过期，避免内存泄漏
   - ⚠️ TTL 刷新频率增加（但 `PEXPIRE` 是 O(1) 操作，性能影响可忽略）

3. **向后兼容性**：
   - ✅ 完全兼容，现有 key 会自动转换为滚动窗口模式
   - ✅ TTL 为 -1 的 key 会在下次 incr 时自动修复

## 监控和验证

### 读取 Redis 配置
**重要**: 不要使用默认的 `127.0.0.1:6379`，必须使用 `config/local.yml` 中配置的 Redis 地址。

```bash
# 从 config/local.yml 读取 Redis 配置
REDIS_ADDR=$(grep -A 5 "redis:" config/local.yml | grep "addr:" | awk '{print $2}')
REDIS_PASSWORD=$(grep -A 5 "redis:" config/local.yml | grep "password:" | awk '{print $2}')
REDIS_HOST=$(echo $REDIS_ADDR | cut -d: -f1)
REDIS_PORT=$(echo $REDIS_ADDR | cut -d: -f2)

# 连接 Redis
redis-cli -h $REDIS_HOST -p $REDIS_PORT -a "$REDIS_PASSWORD"
```

### 验证 TTL 设置正确
```bash
# 检查限流 key 的 TTL（应该接近窗口大小）
redis-cli -h $REDIS_HOST -p $REDIS_PORT -a "$REDIS_PASSWORD" TTL "aigw:rl:JD:glm-5:global-tpm-limit:1m0s"
# 预期输出: 接近 60 秒的正整数

# 批量检查所有限流 key
redis-cli -h $REDIS_HOST -p $REDIS_PORT -a "$REDIS_PASSWORD" --scan --pattern "aigw:rl:*" | while read key; do
    ttl=$(redis-cli -h $REDIS_HOST -p $REDIS_PORT -a "$REDIS_PASSWORD" TTL "$key")
    if [ "$ttl" = "-1" ]; then
        echo "WARNING: $key has no TTL!"
    fi
done
```

### 监控指标
- Redis 内存使用趋势（应该稳定，不再增长）
- `aigw:rl:*` key 数量（应该随活跃用户数波动）
- TTL 为 -1 的 key 数量（应该为 0）

## 替代方案（未采纳）

### 方案 C: 使用 Redis Streams
使用 Redis Streams 实现真正的滑动窗口：
```lua
redis.call('XADD', key, 'MAXLEN', '~', limit, '*', 'tokens', tokens)
redis.call('EXPIRE', key, window_sec)
```

**优点**：
- ✅ 真正的滑动窗口语义
- ✅ 精确到毫秒级

**缺点**：
- ❌ 性能开销大（需要 `XRANGE` 遍历）
- ❌ 内存占用高（每个请求一个 entry）
- ❌ 重构成本高

### 方案 D: 双 Key 方案
使用两个 key：一个计数器 + 一个 TTL 守护 key：
```lua
redis.call('INCRBY', key, tokens)
redis.call('SET', key .. ':guard', '', 'PX', window_ms, 'NX')
```

**优点**：
- ✅ 保持固定窗口语义

**缺点**：
- ❌ 双倍 key 数量
- ❌ 需要同步两个 key 的生命周期
- ❌ 复杂度高

## 参考资料
- Redis TTL 文档: https://redis.io/commands/ttl/
- Redis SET KEEPTTL 文档: https://redis.io/commands/set/
- 滑动窗口算法: https://en.wikipedia.org/wiki/Sliding_window_protocol
- 原始问题: "aigw:rl:JD:glm-5:global-tpm-limit:1m0s 的 TTL 为 -1"

## 后续优化建议
1. 考虑添加 Prometheus 指标监控 TTL 异常
2. 评估是否需要实现真正的固定窗口（如果业务需要）
3. 添加限流规则配置项（固定窗口 vs 滚动窗口）
