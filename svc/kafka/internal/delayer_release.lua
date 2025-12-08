-- KEYS[1] - The delay queue key (新的 bucket，要添加到的队列)
-- KEYS[2] - The reserved queue key (旧的 bucket，要删除的队列)
-- ARGV[1] - The old taskJson (需要删除的旧值)
-- ARGV[2] - The new timestamp (新的时间戳)
-- ARGV[3] - The new taskJson (需要添加的新值)

-- 删除 reserved 队列中的旧值
redis.call('zrem', KEYS[2], ARGV[1])

-- 添加新的任务到 delay 队列（使用新的 taskJson）
redis.call('zadd', KEYS[1], ARGV[2], ARGV[3])

return true