local key = KEYS[1]
local keytype = redis.call('type', key).ok

if keytype=='set' then
  return redis.call('scard', key)
elseif keytype=='zset' then
  return redis.call('zcard', key)
else
  return "WTF?"
end
