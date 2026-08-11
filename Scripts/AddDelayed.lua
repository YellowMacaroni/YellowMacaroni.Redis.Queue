redis.replicate_commands()

local time = redis.call('TIME')
local now = (time[1] * 1000) + math.floor(time[2] / 1000)
local readyAt = now + tonumber(ARGV[1])
local job = cjson.decode(ARGV[2])

if job.timestamp == nil then
    job.timestamp = tostring(now)
end

return redis.call('ZADD', KEYS[1], readyAt, cjson.encode(job))
