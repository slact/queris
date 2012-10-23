local delta_keyf, exists_keyf = '%s:delta', '%s:exists'
local live_query_zset = KEYS[1]
local id = ARGV[1]
local update_query_keys = redis.call('zrange', live_query_zset, 0, -1)
local update_keys = {}
if #update_query_keys == 0 then
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
  return removed
end
