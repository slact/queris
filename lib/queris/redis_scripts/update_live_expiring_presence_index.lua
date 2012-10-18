local delta_keyf, exists_keyf = '%s:delta', '%s:exists'
local index_key, queries_key = KEYS[1], KEYS[2]
local update_query_keys = redis.call('zrange', queries_key, 0, -1)
local now, ttl, follow_schedule = tonumber(ARGV[1]), tonumber(ARGV[2]), ARGV[3]
if follow_schedule then
  local schedule_key = queries_key .. ":wait"
  if redis.call('exists', schedule_key) == 1 then
    return 0
  else
    redis.call('setex', schedule_key, ttl/2, "don't update until this key expires") --ttl/2 is rather arbitrary
  end
end
local update_keys = {}
if #update_query_keys == 0 then
  --redis.log(redis.LOG_DEBUG, "no live queries to update for index " .. index_key)
  return 0
else
  --are they all valid? delete the ones that aren't
  local removed = 0
  for i,key in ipairs(update_query_keys) do
    if redis.call('exists', exists_keyf:format(key))==1 then
      table.insert(update_keys, delta_keyf:format(key))
    else
      redis.call('zrem', queries_key, key)
      removed = removed + 1
    end
  end
  --redis.log(redis.LOG_DEBUG, ("using %d out of %d query keys"):format(#update_keys, #update_query_keys))
end
local too_old = now - ttl
local res = redis.call('zrangebyscore', index_key, '-inf', too_old)
if #res > 0 then
  for i, id in ipairs(res) do
    for j, update_key in ipairs(update_keys) do
      redis.call('zadd', update_key, 0, id)
    end
  end
  redis.call('zremrangebyscore', index_key, '-inf', too_old)
end
--redis.log(redis.LOG_NOTICE, ("updated %d keys for %d queries for index %s"):format(#res, #update_keys, index_key))
return #res