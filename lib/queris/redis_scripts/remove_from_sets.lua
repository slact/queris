local keys = KEYS
local id = ARGV[1]
local removed = 0
for i, key in ipairs(keys) do
  local keytype = redis.call('type', key).ok
  if keytype == 'set' then
    removed = removed + redis.call('srem', key, id)
  elseif keytype == 'zset' then
    removed = removed + redis.call('zrem', key, id)
  end
end
redis.log(redis.LOG_NOTICE, "Queris deleted " .. removed .. " ids from " .. #keys .. " keys.")
return removed