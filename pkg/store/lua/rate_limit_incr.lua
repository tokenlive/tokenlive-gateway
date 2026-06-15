-- rate_limit_incr.lua
-- 限流递增：原子性地增加计数器并返回当前已消耗量。
-- KEYS[1] = 限流键
-- ARGV[1] = 本次消耗 tokens
-- ARGV[2] = 窗口大小（毫秒）
-- 返回: 当前已消耗量

local key = KEYS[1]
local tokens = tonumber(ARGV[1])
local window_ms = tonumber(ARGV[2])

-- INCRBY 是原子操作，键不存在时从 0 开始递增
local current = redis.call('INCRBY', key, tokens)

-- 仅当键刚刚创建（或当前 PTTL 为 -1）时才设置过期时间
-- 这可以保证在整个窗口时间内 TTL 不会被不断的请求递增给往后推迟，实现真正固定窗口
if redis.call('PTTL', key) == -1 then
    redis.call('PEXPIRE', key, window_ms)
end

return current

