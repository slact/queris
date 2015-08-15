local dst=table.remove(KEYS, 1)
if redis.call('exists', dst) == 1 then
  redis.log(redis.LOG_WARNING, "persist_reusable_temp_query_keys storage key " .. dst .. " already exist. WTF?")
  redis.call('del', dst)
end
for i,k in ipairs(KEYS) do
  local t= redis.call('type', k).ok
  if t ~= 'none' then
    ttl = redis.call('ttl', k)
    redis.call('persist', k)
    redis.zadd(dst, ttl, k)
    redis.log(redis.LOG_WARNING, "persisted key " .. k .. " ttl=" .. ttl)
  end
end