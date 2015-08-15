local results_key, exists_key, sort_key, page_key, runstate_key = KEYS[1], KEYS[2], KEYS[3], KEYS[4], KEYS[5]
local pagesize, min, max = tonumber(ARGV[1]), tonumber(ARGV[2]), tonumber(ARGV[3])

if redis.call('exists', exists_key) ~= 1 then
  return nil
end

local last_loaded_page = redis.call('get', page_key)

if not last_loaded_page then
  return nil
end

local t = redis.call('type', results_key).ok
local current_count
if t == 'zset' then
  current_count = redis.call('zcard', results_key)
elseif t=='set' then
  current_count = redis.call('scard', results_key)
else
  current_count = 0
end

if current_count > max then
  if runstate_key then
    redis.call('set', runstate_key, 1)
  end
  return true
elseif (last_loaded_page + 1) * pagesize >= redis.call('zcard', sort_key) then
  --we've reached the end
  if runstate_key then
    redis.call('set', runstate_key, 1)
  end
  return true
end
