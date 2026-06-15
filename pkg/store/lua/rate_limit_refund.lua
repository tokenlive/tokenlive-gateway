-- rate_limit_refund.lua
-- 限流退还：原子性地减少计数器（最低为 0）。
-- KEYS[1] = 限流键
-- ARGV[1] = 退还 tokens 数量
-- 返回: 退还后的计数值

local key = KEYS[1]
local tokens = tonumber(ARGV[1])

-- 检查 key 是否存在
local exists = redis.call('EXISTS', key)
if exists == 0 then
    -- key 不存在，无需退还（可能已过期）
    return 0
end

-- DECRBY 是原子操作
local after = redis.call('DECRBY', key, tokens)

-- 若减至负数则钳位为 0（保持原有 TTL）
if tonumber(after) < 0 then
    redis.call('SET', key, 0, 'KEEPTTL')
    return 0
end
return after
