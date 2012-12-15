local log = function(msg, level)
  local loglevel
  if     level == 'debug' then loglevel=redis.LOG_DEBUG
  elseif level == "notice" then loglevel=redis.LOG_NOTICE
  elseif level == "verbose" then loglevel=redis.LOG_VERBOSE
  elseif level == "warning" then loglevel=redis.LOG_WARNING
  else loglevel=redis.LOG_WARNING end
  local txt = ("query update: %s"):format(msg)
  redis.log(loglevel, txt)
  return txt
end
local query_marshaled_key= KEYS[1]
local live_index_changesets_key = KEYS[2]
local now = tonumber(ARGV[1])

local delta_key, deltas = nil, {}
local changeset = {}
--assemble query changeset from live index changeset keys, noting the scores as we go along
log("|" .. live_index_changesets_key .. "|=" .. redis.call('zcard', live_index_changesets_key))
for i, v in ipairs(redis.call('zrange', live_index_changesets_key, 0, -1, 'withscores')) do
  if i%2==1 then delta_key=v; else
    log("rangethrough (" .. v .. ", inf on " .. tostring(delta_key))
    local el, last = nil, v
    local res = redis.call('zrevrangebyscore', delta_key, 'inf', '('..v, 'withscores')
    log("|" .. delta_key .. "(" .. v .. " .. inf)|=" .. #res/2 .. ", total=" .. redis.call('zcard', delta_key))
    for j, val in ipairs(res) do
      --log("delta " .. j .. " " .. val)
      if j%2==1 then el=val; else
        log(tostring(el) .. " has changed")
        if val > last then last = val end
        changeset[el]=true
      end
    end
    log( "done with delta set " .. tostring(delta_key) .. " with last element updated at " .. tostring(last))
    redis.call('zadd', live_index_changesets_key, last, delta_key)
  end
end

if next(changeset) == nil then
  return log("nothing changed, no updates to query")
end
log("non-empty changeset pulled from " .. live_index_changesets_key)

local marshaled = redis.call("get", query_marshaled_key)
if not marshaled then
  return log("Queris couldn't update query with key " .. query_marshaled_key .. " : redis-friendly marshaled query contents not found.", "warning")
end
local success, query = pcall(cjson.decode, marshaled)
if not success then
  return log("Error unpacking json-serialized query at " .. query_marshaled_key .. " : " ..   query .. "\r\n " .. marshaled, "warning")
end
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
    --log("OP: " .. op.op .. " member:" .. (member and "true" or "false") .. " id: " .. id .. " indexkey: " .. (op.key or "nokey"), 'debug')
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

local total, added, removed = 0, 0, 0

for id,_ in pairs(changeset) do
  total = total + 1
  if query_member(query, id) then
    local sc = score(query.sort_ops, id)
    log("add " .. id .. " with score " .. sc)
    redis.call('zadd', query.key, sc, id)
    added = added + 1
  else
    redis.call('zrem', query.key, id)
    removed = removed + 1
  end
end
local status_message = ("added %d, removed %d out of %d for %s"):format(added, removed, total, query.key)
log(status_message, "notice")
return status_message
