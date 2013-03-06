local smallset=KEYS[1]
local keys={}
local key=nil
for i,v in ipairs(ARGV) do
  if i%2==0 then
    key = v
  else
    optimize_if_necessary(key, smallkey, v)
  end
end
function optimize_if_necessary(key, smallkey, dstkey)
  if redis.call('type', dstkey).ok ~= 'zset' then
    redis.call('zinterstore', dstkey, 2, key, smallkey)
  end
end
