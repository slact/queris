local dstkey, key, smallkey = KEYS[1], KEYS[2], KEYS[3]
local ttl = ARGV[1] or 120
local t=redis.call('type', dstkey).ok

local multisize = function(key)
  local keytype = redis.call('type', key).ok
  if keytype=='set' then
    return "set card=" .. redis.call('scard', key)
  elseif keytype=='zset' then
    return "zset card=" .. redis.call('zcard', key)
  elseif keytype=='list' then
    return "list card=" .. redis.call('llen', key)
  elseif keytype=='hash' then
    return "hash card=" .. redis.call('hlen', key)
  elseif keytype=='none' then
    return "none"
  elseif keytype=='string' then
    return "string length=" .. redis.call('strlen', key)
  else
    return "WTF for " .. keytype
  end
end
if t ~= 'zset' then
  redis.call('zinterstore', dstkey, 2, key, smallkey, 'weights', 1, 0)
  redis.call('expire', dstkey, ttl)
  for i,v in pairs({dstkey, key, smallkey}) do
    redis.log(redis.LOG_WARNING, " inter: " .. v .. ": " .. multisize(v))
  end
else
  redis.log(redis.LOG_WARNING, "intersectoptimization " .. dstkey .. " already exists")
end
