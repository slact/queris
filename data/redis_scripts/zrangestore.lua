local res = redis.call("zrange", KEYS[2],  ARGV[1], ARGV[2], "withscores")
local id, score
if next(res) ~= nil then
  redis.call('del', KEYS[1])
end
for i,v in ipairs(res) do
  if i%2==1 then id=v; else
    score=v
    redis.call('zadd', KEYS[1], score, id)
  end
end
return #res/2