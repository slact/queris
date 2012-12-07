local existence_key, results_key = KEYS[1], KEYS[2]
local ttl, min_ttl = ARGV[1], tonumber(ARGV[2])
if redis.call('exists', existence_key) == 1 then
  for i, k in ipairs(KEYS) do
    if min_ttl then
      local key_ttl = redis.call('ttl', k)
      if key_ttl < min_ttl then
        redis.call('expire', k, min_ttl)
      end
    else
      redis.call('expire', k, ttl)
    end
  end
  return true
else
  redis.call('setnx', results_key, 1)
  redis.call('expire', results_key, ttl)
  return false
end
