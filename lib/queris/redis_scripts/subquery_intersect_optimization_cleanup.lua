local querykey , tmpkey = KEYS[1], KEYS[2]
if redis.call('type', querykey).ok =='string' and redis.call('get', querykey) == tmpkey then
  redis.call('move', tmpkey, querykey)
  redis.log(redis.LOG_WARNING, "moved subquery key back from " .. tmpkey .. " to " .. querykey)
end
