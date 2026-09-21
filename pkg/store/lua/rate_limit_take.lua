-- rate_limit_take.lua
-- 令牌桶扣减：原子性地计算令牌填充并执行令牌扣减
-- KEYS[1] = 限流键
-- ARGV[1] = 扣减令牌数 (int)
-- ARGV[2] = 填充速率 rate (float)
-- ARGV[3] = 桶容量 capacity (float)
-- ARGV[4] = 填充单位毫秒 window_ms (float)
-- ARGV[5] = 当前毫秒时间戳 now_ms (float)
-- ARGV[6] = 可选：1 表示实际用量结算，允许余额为负；省略时保持准入检查
-- 返回: {1, 剩余令牌} 表示成功；{0, 剩余令牌} 表示超限失败

local key = KEYS[1]
local requested = tonumber(ARGV[1])
local rate = tonumber(ARGV[2])
local capacity = tonumber(ARGV[3])
local window_ms = tonumber(ARGV[4])
local now_ms = tonumber(ARGV[5])
local adjustment = tonumber(ARGV[6]) == 1

-- 从 Hash 获取当前令牌数和上次更新时间
local data = redis.call('HMGET', key, 'tokens', 'last_updated', 'settlement')
local tokens = tonumber(data[1])
local last_updated = tonumber(data[2])
local debt_aware = adjustment or tonumber(data[3]) == 1

if not tokens then
    if requested < 0 then
        -- 如果是退还操作且 key 不存在，说明已过期，无需退还，直接返回成功
        return {1, math.floor(capacity)}
    end
    -- 首次初始化，桶是满的
    tokens = capacity
    last_updated = now_ms
else
    -- 计算时间差
    local delta = now_ms - last_updated
    if delta > 0 then
        -- 计算生成的新令牌
        local gen_tokens = delta * (rate / window_ms)
        tokens = math.min(capacity, tokens + gen_tokens)
        last_updated = now_ms
    end
end

-- 判定是否足够
if adjustment or requested < 0 or tokens >= requested then
    tokens = math.min(capacity, tokens - requested)
    redis.call('HMSET', key, 'tokens', tokens, 'last_updated', last_updated)
    if adjustment then
        redis.call('HSET', key, 'settlement', 1)
    end
    
    -- 只有在扣减令牌操作（requested > 0）时才刷新过期时间
    -- 退还操作（requested < 0）应当保持原有的 TTL，不刷新过期时间，以防止锁死
    if requested > 0 then
        -- 结算可能产生超过两个窗口的债务，过期不能早于桶恢复到满额。
        -- rate=0 的欠额无法随时间恢复，必须保留，直到后续结算退款。
        if not debt_aware then
            -- 普通（未参与实际用量结算）令牌桶保留原有的过期策略。
            redis.call('PEXPIRE', key, math.floor(window_ms * 2))
        elseif rate > 0 then
            local refill_ms = math.ceil((capacity - tokens) * window_ms / rate)
            redis.call('PEXPIRE', key, math.max(math.floor(window_ms * 2), refill_ms))
        else
            redis.call('PERSIST', key)
        end
    end
    return {1, math.floor(tokens)}
else
    return {0, math.floor(tokens)}
end
