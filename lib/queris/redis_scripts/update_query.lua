local log = function(msg, level)
  local loglevel
  if not level or level == 'debug' then
    loglevel=redis.LOG_DEBUG
  elseif level == "verbose" then
    loglevel=redis.LOG_VERBOSE
  elseif level == "warning" then
    loglevel=redis.LOG_WARNING 
  elseif level == "notice" then
    loglevel=redis.LOG_NOTICE
  else
    loglevel=redis.LOW_DEBUG
  end
  redis.log(loglevel, ("query update: %s"):format(msg))
end

local query_marshaled_key= KEYS[1]
local now = tonumber(ARGV[1])
local marshaled = redis.call("get", query_marshaled_key)
if not marshaled then
  log("Queris couldn't update query with key " .. query_marshaled_key .. " : redis-friendly marshaled query contents not found.", "warning")
  return false
end
--log("Fetched marshaled query", "debug")
local success, query = pcall(cjson.decode, marshaled)
if not success then
  log("Error unpacking json-serialized query at " .. query_marshaled_key .. " : " ..   query .. "\r\n " .. marshaled, "warning")
  return false
end
--log("Unmarshaled query", "debug")
local query_member
local is_member = function(op, id)
  --log("" .. id .. " is_member?", "debug")
  if op.query then
    return query_member(op.query, id)
  end
  local t = redis.call('type', op.key).ok
  if t == 'set' then
    return redis.call('sismember', op.key, id) == 1
  elseif t == 'zset' then
    local score = tonumber(redis.call('zscore', op.key, id))
    --log("found " .. id .. " in zset " .. op.key .. " with score " ..  score, "debug")
    local m = score and true
    if op.index == "ExpiringPresenceIndex" then
      m = m and (score + op.ttl) >= now
    else
      if op.min then m = m and score > op.min end
      if op.max then m = m and score < op.max end
      if op.max_or_equal then m = m and score <= op.max_or_equal end
      if op.equal then m = m and score == op.equal end
    end
    return m
  elseif t == 'none' then
    return false
  else
    log("Unexpected index type " .. (t or "?"), "warning")
    return false
  end
end

query_member = function(query, id)
  --log("Query member? " .. id, "debug")
  local revops = query.ops_reverse
  for i, op in ipairs(revops) do
    local member = is_member(op, id)
    log("OP: " .. op.op .. " member:" .. (member and "true" or "false") .. " id: " .. id .. " indexkey: " .. (op.key or "nokey"), 'debug')
    if op.op == "intersect" then
      if not member then
        return false
      elseif member and i == #revops then
        return true
      end
    elseif op.op == "diff" then
      if member then
        return false
      end
    elseif op.op == "union" then
      if member then
        return true
      end
    end
  end
  return false
end

local score = function(ops, id)
  --log("score " .. id, "debug")
  local sum = 0
  for i,op in ipairs(ops) do
    local myscore = redis.call('zscore', op.key, id)
    sum = sum+ op.multiplier * (myscore or 0)
  end
  return sum
end

local added, removed = 0, 0
local changed_ids = redis.call('zrange', query.delta_key, 0, -1)
--log("got " .. #changed_ids .. "delta ids,", "debug")
for i, id in ipairs(changed_ids) do
  if query_member(query, id) then
    log( id .. " is a member of query at " .. query.key, "verbose")
    redis.call('zadd', query.key, score(query.sort_ops, id), id)
    added = added + 1
  else
    log(id .. " is NOT a member of query at " .. query.key, "verbose")
    redis.call('zrem', query.key, id)
    removed = removed + 1
  end
end
redis.call('del', query.delta_key)
local status_message = ("added %d, removed %d out of %d for %s"):format(added, removed, #changed_ids, query.key)
log(status_message, "notice")
return status_message
