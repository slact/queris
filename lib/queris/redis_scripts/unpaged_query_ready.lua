local existence_key, key = KEYS[1], KEYS[2]
return  redis.call('exists', existence_key) == 1 and redis.call('type', key).ok ~= 'string'
