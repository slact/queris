local dstkey, srckey, smallkey = KEYS[1], KEYS[2], KEYS[3]
local multiplier = tonumber(ARGV[1])
local setsize=function(k)
  local t = redis.call('type', k).ok
  if t=='zset' then
    return redis.call('zcard', k)
  elseif t == 'set' then
    return redis.call('scard', k)
  else
    return 0
  end
end
local srcsize, smallsize = setsize(srckey), setsize(smallkey)
redis.log(redis.LOG_WARNING, ("QWOPTIMIZE |%s| against |%s|. mult=%s. do it? %s"):format(srcsize, smallsize, multiplier, (smallsize * multiplier < srcsize) and 'yes' or 'no'))
if smallsize * multiplier < srcsize then
  redis.log(redis.LOG_WARNING, "serverside optimizing " .. dstkey)
  redis.call('zinterstore', dstkey, 2, srckey, smallkey, 'weights', 1, 0)
else
  redis.log(redis.LOG_WARNING, "serverside nonoptimizing " .. dstkey)
  redis.log(redis.LOG_WARNING, "moving "..srckey .." to "..dstkey)
  
  redis.call('rename', srckey, dstkey)
  redis.log(redis.LOG_WARNING, "moved.")
  redis.call('set', srckey, dstkey) -- temp state to be cleaned up later
end
