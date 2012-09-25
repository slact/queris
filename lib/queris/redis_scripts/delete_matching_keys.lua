local keymatch = ARGV[1]
local keys = redis.call('keys', keymatch)
local deleted = 0
for i,key in ipairs(keys) do
  deleted = deleted + redis.call('del', key)
end
return deleted
