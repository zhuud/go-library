-- KEYS[1] - The queue we are removing jobs from, for example: queues:foo:delayed
-- KEYS[2] - The queue we are moving jobs to, for example: queues:foo:reserved
-- ARGV[1] - The current data
-- ARGV[2] - The current data UNIX timestamp

-- Remove the job from the current queue...
redis.call('zrem', KEYS[2], ARGV[1])
-- Add the job onto the "delayed" queue...
redis.call('zadd', KEYS[1], ARGV[2], ARGV[1])

return true