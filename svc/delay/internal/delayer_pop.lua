-- KEYS[1] - The source queue (e.g., {delay:queue:prefix}:delayed)
-- KEYS[2] - The destination queue (e.g., {delay:queue:prefix}:reserved)
-- ARGV[1] - The threshold UNIX timestamp
-- ARGV[2] - The batch size

local val = redis.call('zrangebyscore', KEYS[1], '-inf', ARGV[1], 'limit', 0, ARGV[2])
local taskVal = {}

if next(val) ~= nil then
    redis.call('zremrangebyrank', KEYS[1], 0, #val - 1)
    for i = 1, #val do
        table.insert(taskVal, val[i])
        redis.call('zadd', KEYS[2], ARGV[1], val[i])
    end
end

return taskVal
