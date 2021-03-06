local oldkey, newkey = KEYS[1], KEYS[2]
if redis.call('exists', oldkey) == 1 then
  redis.call('rename', oldkey, newkey)
  return 1
else
  --oldkey is empty, so we move that emptiness to newkey
  if redis.call('exists', newkey) == 1 then
    redis.call('del', newkey) --clear newkey
  end
  return 0
end
