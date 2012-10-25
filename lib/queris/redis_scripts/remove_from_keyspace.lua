local element, keysearch, ttl = ARGV[1], ARGV[2], tonumber(ARGV[3])
local keys_key = "Queris:removing_from_keyspace:" .. keysearch
local keys

--relevant keys
if redis.call('zcard', keys_key) > 0 then
  keys = redis.call('zrange', keys_key, 0, -1)
else
  keys = redis.call('keys', keysearch)
  if #keys > 0 then
    for i,v in ipairs(keys) do
      redis.call('zadd', keys_key, 0, v)
    end
    redis.call('expire', keys_key, ttl)
  end
end

local removed = 0
for i,key in ipairs(keys) do
  local keytype= redis.call('type', key).ok
  if     keytype == 'zset' then
    removed = removed + redis.call('zrem', key, element)
  elseif keytype == 'set'  then
    removed = removed + redis.call('srem', key, element)
  end
end
return removed
