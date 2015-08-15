local rangehacks_key, range_key = KEYS[1], KEYS[2]

local function to_f(val)
  if val=="-inf" then
    return -math.huge
  elseif val == 'inf' then
    return math.huge
  else
    return tonumber(val)
  end
end

local action, id, val = ARGV[1], ARGV[2], to_f(ARGV[3])

local enable_debug=true
local dbg = (function(on)
  if on then return function(...) 
    local arg, cur = {...}, nil
    for i = 1, #arg do
      arg[i]=tostring(arg[i])
    end
    redis.call('echo', table.concat(arg))
  end; else
    return function(...) return; end
  end
end)(enable_debug)

dbg("##### UPDATE_RANGEHACKS ####")

if action == "incr" then
  dbg("translate incr to add")
  local cur_val=redis.call('zscore', range_key, id)
  if cur_val then
    cur_val=to_f(cur_val)
    val = cur_val + val
  else
    error("i don't know how to do this man")
  end
  action = "add"
end

local hacks = redis.call('smembers', rangehacks_key)

local function update_hack(key)
  dbg("update_hack ", key)
  if action == "add" then 
    redis.call('zadd', key, val, id)
  elseif action == "del" then
    redis.call('zrem', key, id)
  else
    --dbg(action, " none of the abose")
    error(("%s action disallowed here. should've been converted to an add in this script"):format(action))
  end
end

local function maybe_update_hack(key)
  local hackval = key:match("^.*:(.*)$")
  local hnum = to_f(hackval)
  dbg("yeeah", key, "  ", hackval)
  if hnum then
    if hnum == val then
      return update_hack(key)
    else
      --dbg("hackval ~= val ", hackval, "  ", val) 
    end
    return
  end
  
  local min, interval, max = hackval:match("(.*%w)(%.%.%.?)(.*)")
  --dbg("val: ", val, " FOUND ", min," ", interval, " ", max)
  if min and interval and max then
    min, max = to_f(min), to_f(max)
    if interval == ".." then
      if val >= min and val < max then
        return update_hack(key)
      end
    else
      if val >= min and val <= max then
        return update_hack(key)
      end
    end
    return
  end

end

for i,hack_key in pairs(hacks) do
  if redis.call('exists', hack_key) == 1 then
    maybe_update_hack(hack_key, val)
  else
    --it's not there anymore
    redis.call("srem", hack_key)
  end
end