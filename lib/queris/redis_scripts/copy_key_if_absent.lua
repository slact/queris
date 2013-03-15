local dst, src = KEYS[1], KEYS[2]
local t = redis.call('type', src).ok
if t == 'zset' then
  redis.call('zunionstore', dst, 1, src)
elseif t == 'set' then
  redis.call('sunionstore', dst, 1, src)
elseif t == 'string' then
  redis.call('set', dst, redis.call('get', src))
elseif t == 'none' then
  --noop
else
  redis.log(redis.LOG_WARNING, "copy_key_if_absent: unsupported key type " .. t .. ".")
end