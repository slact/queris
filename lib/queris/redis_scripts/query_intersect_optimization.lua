local dstkey, key, smallkey = KEYS[1], KEYS[2], KEYS[3]
local ttl = ARGV[1] or 20
if redis.call('type', dstkey).ok ~= 'zset' then
  redis.call('zinterstore', dstkey, 2, key, smallkey)
  redis.call('expire', dstkey, ttl)
end
