redis.replicate_commands()

local time = redis.call('TIME')
local timestamp = (time[1] * 1000) + math.floor(time[2] / 1000)
local fields = { '*' }

for index = 1, #ARGV, 2 do
    if ARGV[index] ~= 'timestamp' then
        table.insert(fields, ARGV[index])
        table.insert(fields, ARGV[index + 1])
    end
end

table.insert(fields, 'timestamp')
table.insert(fields, tostring(timestamp))

return redis.call('XADD', KEYS[1], unpack(fields))
