local dst, source = KEYS[1], KEYS[2]
local min, max = ARGV[1], ARGV[2]
local dst_type = redis.call('type', dst).ok
redis.log(redis.LOG_WARNING, "want a page at " .. dst .. " from " .. source)
if dst_type == 'string' then
  redis.call('del', dst)
elseif dst_type == 'zset' then
  redis.log(redis.LOG_WARNING, 'page ' .. dst .. ' already exists with size ' .. redis.call('zcard', dst))
  return redis.call('zcard', dst)
end

local res = redis.call("zrange", source, min, max, "withscores")
local id, score
if next(res) ~= nil then
  redis.call('del', dst)
end
for i,v in ipairs(res) do
  if i%2==1 then id=v; else
    score=v
    redis.call('zadd', dst, score, id)
  end
end
redis.log(redis.LOG_WARNING, 'pagesize of ' .. dst .. ': ' .. redis.call('zcard', dst))
return #res/2 
