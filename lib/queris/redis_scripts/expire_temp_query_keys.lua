local ttl = ARGV[1]
for i,k in ipairs(KEYS) do
  if redis.call('exists', k) ~= 1 then
    redis.call('setnx', k, 1337)
    redis.call('expire', k, ttl)
  end
end
