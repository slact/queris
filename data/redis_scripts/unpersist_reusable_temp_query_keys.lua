local key = KEYS[1]

local k, ttl
for i, v in ipairs(redis.call('zrange', key, 0, -1, 'withscores')) do
  if i%2==1 then k=v; else
    ttl=v
    redis.call('expire', k, ttl)
    redis.log(redis.LOG_WARNING, "unpersisted key " .. k .. " ttl=" .. ttl)
  end
end
redis.call('del', key)