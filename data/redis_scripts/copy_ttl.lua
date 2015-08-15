if redis.call('exists', KEYS[2]) == 0 then
  return nil
end
local ttl, delete_if_absent = redis.call("ttl", KEYS[1]), (ARGV[1] or false)
if ttl ~= -1 then
  redis.call("expire", KEYS[2], ttl)
  return ttl
elseif delete_if_absent and redis.call('exists', KEYS[1]) == 0 then
  if redis.call("del", KEYS[2]) == 1 then
    return "DELETED"
  end
end
return 'OK'