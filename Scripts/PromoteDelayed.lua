redis.replicate_commands()

local time = redis.call('TIME')
local now = (time[1] * 1000) + math.floor(time[2] / 1000)
local due = redis.call('ZRANGEBYSCORE', KEYS[1], 0, now)

for _, member in ipairs(due) do
    redis.call('ZREM', KEYS[1], member)
    local job = cjson.decode(member)
    redis.call('XADD', KEYS[2], '*',
        'id', job.id,
        'data', job.data,
        'attempt', job.attempt,
        'timestamp', job.timestamp)
end

return #due
