local query_marshaled_key= KEYS[1]
local id = ARGV[1]
local marshaled = redis.call("get", query_marshaled_key)
if not marshaled then
  redis.log(redis.LOG_WARNING, "Queris couldn't update query with key " .. query_marshaled_key .. ": redis-friendly marshaled query contents not found.")
  return false
end

redis.call("echo", marshaled)
local success, query = pcall(cjson.decode, marshaled)
if not success then
  redis.log(redis.LOG_WARNING, "Error unpacking json-serialized query at " .. query_marshaled_key .. ": " ..   query .. "\r\n " .. marshaled)
  return false
end
local query_member
local is_member = function(op, id)
  if op.query then
    return query_member(op.query, id)
  end
  local t = redis.call('type', op.key).ok
  if t == 'set' then
    return redis.call('sismember', op.key, id) == 1
  elseif t == 'zset' then
    local score = tonumber(redis.call('zscore', op.key, id))
    local m = score and true
    if op.min then m = m and score > op.min end
    if op.max then m = m and score < op.max end
    if op.max_or_equal then m = m and score <= op.max_or_equal end
    if op.equal then m = m and score == op.equal end
    return m
  elseif t == 'none' then
    return false
  else
    redis.log(redis.LOG_WARNING, "Unexpected index type " .. (t or "?"))
    return false
  end
end

query_member = function(query, id)
  local revops = query.ops_reverse
  for i, op in ipairs(revops) do
    local member = is_member(op, id)
    --redis.log(redis.LOG_WARNING , "OP: " .. op.op .. " member:" .. (member and "true" or "false") .. " id: " .. id .. " indexkey: " .. (op.key or "nokey"))
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
  local sum = 0
  for i,op in ipairs(ops) do
    local myscore = redis.call('zscore', op.key, id)
    sum = sum+ op.multiplier * (myscore or 0)
  end
  return sum
end

if query_member(query, id) then
  redis.log(redis.LOG_WARNING, id .. " is a member of query at " .. query.key)
  redis.call('zadd', (query.realtime and query.key or query.delta_union_key), score(query.sort_ops, id), id)
else
  redis.log(redis.LOG_WARNING, id .. " is NOT a member of query at " .. query.key)
  if query.realtime then
    redis.call('zrem', query.key, id)
  else
    redis.call('zadd', query.delta_diff_key, 0, id)
  end
end
