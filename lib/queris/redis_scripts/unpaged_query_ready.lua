local results_key, exists_key, runstate_key = KEYS[1], KEYS[2], KEYS[3]
redis.log(redis.LOG_WARNING, "unpaged_query_ready - " .. redis.call('exists', exists_key) .. "  " .. exists_key)

if redis.call('exists', exists_key) ~= 1 then
  redis.log(redis.LOG_WARNING, "exists is absent")
  return nil
end
local t = redis.call('type', results_key).ok
if t == 'string' then
  redis.log(redis.LOG_WARNING, "stringy results_key")
  return nil
elseif t == 'set' or t == 'zset' then
  if runstate_key then
    redis.call('set', runstate_key, 1)
  end
  return true
end
