-- record_latency.lua
-- 记录延迟样本：原子性地添加样本、设置 TTL、清理旧样本。
-- KEYS[1] = latency key (Sorted Set)
-- ARGV[1] = score (时间戳，毫秒)
-- ARGV[2] = member (格式: "<latency_ns>:<rand>")
-- ARGV[3] = max_samples (最大样本数)
-- ARGV[4] = ttl_ms (key 过期时间，毫秒)
-- 返回: 1 (成功)

local key = KEYS[1]
local score = tonumber(ARGV[1])
local member = ARGV[2]
local max_samples = tonumber(ARGV[3])
local ttl_ms = tonumber(ARGV[4])

-- 添加样本到 Sorted Set
redis.call('ZADD', key, score, member)

-- 设置或刷新 TTL
redis.call('PEXPIRE', key, ttl_ms)

-- 清理超出数量的旧样本（保留最新的 max_samples 个）
local count = redis.call('ZCARD', key)
if count > max_samples then
    redis.call('ZREMRANGEBYRANK', key, 0, count - max_samples - 1)
end

return 1
