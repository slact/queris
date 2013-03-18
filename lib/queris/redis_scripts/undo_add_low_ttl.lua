local undo_key = KEYS[1]
local ttl_diff = tonumber(ARGV[1])
for i,v in ipairs(redis.call('smembers', undo_key)) do
  local ttl = tonumber(redis.call('ttl', k))
  if ttl > 0 then
    redis.call('expire', ttl + ttl_diff)
  end
end