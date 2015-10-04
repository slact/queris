local min_ttl=tonumber(ARGV[1])
local keys=KEYS
local state=table.remove(keys) --last key is state key
for i, k in ipairs(keys) do
  local ttl = tonumber(redis.call('ttl', k))
  if ttl>0 and ttl < min_ttl then
    redis.call('expire', k, ttl + min_ttl)
    table.sadd(state, k)
  end
end