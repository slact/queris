local ttl = redis.call("ttl", KEYS[1])
redis.call("expire", KEYS[2], ttl)
return 'OK'