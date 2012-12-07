local key = KEYS[1]
local acceptable_types = ARGV
local keytype = redis.call('type', key).ok
for i, t in ipairs(acceptable_types) do
  if t == keytype then
    return true
  end
end
return false