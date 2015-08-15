local index_key, delta_key = KEYS[1], KEYS[2]
local now, ttl, follow_schedule = tonumber(ARGV[1]), tonumber(ARGV[2]), ARGV[3]
local too_old = now - ttl
if follow_schedule=="true" then
  local timer_key = delta_key .. ":timer"
  if redis.call('exists', timer_key) == 1 then
    return 0
  else
    redis.call('setex', timer_key, ttl/2, "don't update until this key expires") --ttl/2 is rather arbitrary
  end
end

local res = redis.call('zrangebyscore', index_key, '-inf', too_old)
if #res > 0 then
  for i, id in ipairs(res) do
    redis.call('zadd', delta_key, now, id)
  end
  redis.call('zremrangebyscore', index_key, '-inf', too_old)
end
return #res
