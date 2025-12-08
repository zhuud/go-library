-- KEYS[1] - The queue we are removing jobs from, for example: queues:foo:delayed
-- KEYS[2] - The queue we are moving jobs to, for example: queues:foo:reserved
-- ARGV[1] - The current UNIX timestamp
-- ARGV[2] - The batch size

-- Get all of the jobs with an expired "score"...
local val = redis.call('zrangebyscore', KEYS[1], '-inf', ARGV[1], 'limit', 0, ARGV[2])
local attemptVal = {}
-- If we have values in the array, we will remove them from the first queue
-- and add them onto the destination queue.
if next(val) ~= nil then
    redis.call('zremrangebyrank', KEYS[1], 0, #val - 1)
    for _, v in ipairs(val) do
        local reserved = cjson.decode(v)
        local oriscore = reserved['timestamp']
        reserved['attempts'] = reserved['attempts'] + 1
        reserved = cjson.encode(reserved)
        -- Because only when the values are the same during subsequent deletions can the deletion be successful. Since attempts is incremented by 1, the return value needs to be processed.
        table.insert(attemptVal, reserved)
        redis.call('zadd', KEYS[2], oriscore, reserved)
    end
end

return attemptVal


