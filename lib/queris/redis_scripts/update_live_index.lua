local delta_keyf, exists_keyf = '%s:delta', '%s:exists'
local live_query_zset = KEYS[1]
local id = ARGV[1]
local update_query_keys = redis.call('zrange', live_query_zset, 0, -1)
local update_keys = {}
if #update_query_keys == 0 then
  --redis.log(redis.LOG_VERBOSE, ("no live queries to update found at %s"):format(live_query_zset))
  return 0
else
  --are they all present? delete the ones that aren't
  local removed = 0
  for i,key in ipairs(update_query_keys) do
    if redis.call('exists', exists_keyf:format(key))==1 then
      table.insert(update_keys, delta_keyf:format(key))
    else
      redis.call('zrem', live_query_zset, key)
      removed = removed + 1
    end
  end
  for i,key in ipairs(update_keys) do
    redis.call('zadd', key, 0, id)
  end
  --redis.log(redis.LOG_VERBOSE, ("updated %d out of %d queries found at %s"):format(#update_keys, #update_query_keys, live_query_zset))
  return removed
end
