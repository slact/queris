local results_key = KEYS[1]
local existence_keyf = ARGV[1]
local keytype = redis.call('type', results_key).ok
local ids
if keytype == "set" then
  redis.call("echo", "IS A SET")
  ids = redis.call("smembers", results_key)
elseif keytype == "zset" then
  redis.call("echo", "IS A SORTED SET")
  ids = redis.call("zrange", results_key, 0, -1)
else
  ids = {}
end
redis.call("echo", "FOUND " .. #ids .. " ids")
local existing, expired = {}, {}
for i,id in ipairs(ids) do
  local exists = redis.call("exists", existence_keyf:format(id))
  table.insert(exists and existing or expired, id)
end
return {existing, expired}
