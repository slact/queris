local ttl, renew, dummy = ARGV[1], ARGV[2], ARGV[3]
redis.log(redis.LOG_WARNING, "master_expire keys [" .. table.concat(KEYS, ', ') .. "] with ttl " .. ttl .. " renew: " .. (renew and "yes" or "no") .. " dummy: " .. (dummy and "yes" or "no"))
for i,k in ipairs(KEYS) do
  local exists = redis.call('exists', k) == 1
  if not exists and dummy then
    exists = 1
    redis.call('setnx', k, 'dummy')
  end
  if exists then
    local current_ttl = redis.call('ttl', k)
    if current_ttl < 0 or renew  then
      redis.call('expire', k, ttl)
    end
  end
end