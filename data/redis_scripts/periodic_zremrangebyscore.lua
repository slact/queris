local ttl = tonumber(ARGV[1])
local key = KEYS[1]
local timer_key = key .. ":timer"
if redis.call('exists', timer_key) == 1 then
  return 0
else
  redis.call('setex', timer_key, ttl, "don't do it as long as i exist")
end
return redis.call('zremrangebyscore', key, ARGV[2], ARGV[3])