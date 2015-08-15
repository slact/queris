local cmd=ARGV[1]
local keys = KEYS

local info = function(k)
  local t = redis.call('type', k).ok
  if t == 'zset' then
    return "zset size=" .. redis.call('zcard', k)
  elseif t== 'set' then
    return "set size=" .. redis.call('scard', k)
  elseif t== 'string' then
    return "string = " .. redis.call('get', k)
  else
    return t
  end
end

redis.log(redis.LOG_WARNING, "info for " ..cmd.. ": " .. #KEYS .. " keys")
for k,v in pairs(keys) do
  redis.log(redis.LOG_WARNING, ("  " .. v .. ": " .. info(v)))
end