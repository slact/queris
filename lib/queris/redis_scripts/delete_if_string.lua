local count = 0
for i,key in ipairs(KEYS) do
  if redis.call('type', key).ok == "string" then
    redis.call('del', key)
    count = count + 1
  end
end
return count
