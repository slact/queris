local dst, src, rangehack_set_key, not_needed_key = KEYS[1], KEYS[2], KEYS[3], KEYS[4]
local min, max, exclude_max = tonumber(ARGV[1]), tonumber(ARGV[2]), ARGV
redis.log(redis.LOG_WARNING, "want rangehack ("..min..","..max..") for " .. src)
if redis.call('exists', not_needed_key) == 1 then
  --nevermind, we don't need to run this
  redis.log(redis.LOG_WARNING, "nevermind, the query doesn't need it..")
  return
end
local opf = "(%s" --)
local t = redis.call('type', dst).ok
if t == 'zset' then
  --already exists, nothing to do
  redis.log(redis.LOG_WARNING, "it's already here.")
  return
else
  redis.call('zunionstore', dst, 1, src) --slow as a fat, barbed turd
  --remove inverse range
  redis.call('zremrangebyscore', dst, '-inf', opf:format(min))
  redis.call('zremrangebyscore', dst, (exclude_max and max or opf:format(max)), 'inf')
  
  --add to rangehack 'index'
  redis.call('sadd', rangehack_set_key, dst)
  
  redis.log(redis.LOG_WARNING, "made one at " .. dst .. " with size " .. redis.call('zcard', dst))
  if redis.call('zcard', dst) == 0 then
    --dummy one-element zset
    redis.call('zadd', dst, 0, "=-{*_*}-= <- the nullset ghost")
  end
  redis.log(redis.LOG_WARNING, "made one at " .. dst .. " with size " .. redis.call('zcard', dst))
end