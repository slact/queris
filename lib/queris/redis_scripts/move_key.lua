local oldkey, newkey = KEYS[1], KEYS[2]
if redis.call('exists', oldkey) == 1 then
  redis.call('rename', oldkey, newkey)
  return 1
else
  redis.call('del', newkey) --clear newkey
  return 0
end
