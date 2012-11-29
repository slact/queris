local key = KEYS[1]
local keytype = redis.call('type', key).ok

if keytype=='set' then
  return redis.call('scard', key)
elseif keytype=='zset' then
  return redis.call('zcard', key)
elseif keytype=='list' then
  return redis.call('llen', key)
elseif keytype=='hash' then
  return redis.call('hlen', key)
elseif keytype=='none' then
  return 0
elseif keytype=='string' then
  return redis.call('strlen', key)
else
  redis.log(redis.LOG_WARNING, "I wonder what the size of key " .. key .. "(" .. keytype .. ")".. "is?")
  return "WTF for " .. keytype
end
