-- update_ema.lua
-- EMA 滚动更新：原子性地计算并更新指数移动平均值。
-- KEYS[1] = 缓存键
-- ARGV[1] = 本次真实 Completion 数量
-- ARGV[2] = 平滑因子 alpha (0~1 浮点数)
-- ARGV[3] = 过期时间 (秒)
-- 返回: 最新的 EMA 值

local key = KEYS[1]
local actual = tonumber(ARGV[1])
local alpha = tonumber(ARGV[2])
local ttl_sec = tonumber(ARGV[3])

local old = redis.call('GET', key)
local new_ema
if not old then
    new_ema = actual
else
    new_ema = actual * alpha + tonumber(old) * (1 - alpha)
end

redis.call('SET', key, new_ema, 'EX', ttl_sec)
return tostring(new_ema)
