local results_key = KEYS[1]
local command, min, max, hashkeyf = ARGV[1], ARGV[2], ARGV[3], ARGV[4]
local ids
if command == "smembers" then
  ids = redis.call(command, results_key)
else
  ids = redis.call(command, results_key, min, max)
end
local ret, notfound = {}, {}
for i,id in ipairs(ids) do
  local flathash = redis.call("hgetall", hashkeyf:format(id))
  if #flathash>0 then
    ret[i] = flathash
  else --notfound
    ret[i] = id
    table.insert(notfound, i-1) --0-based index
  end
end
return {ret, notfound}
