local results_key = KEYS[1]
local command, min, max, hashkeyf, lim, offset, withscores, replace_keyf, replace_id_attr = ARGV[1], ARGV[2], ARGV[3], ARGV[4], ARGV[5], ARGV[6], ARGV[7], ARGV[8], ARGV[9]
local ids, scores, ids_with_scores = {}, {}, {}

if command == "smembers" then
  ids = redis.call(command, results_key)
elseif command == "zrangebyscore" or command == "zrevrangebyscore" then
  local offset, lim = ARGV[5], ARGV[6]
  if not lim or (type(lim)=='string' and #lim==0) then
    ids_with_scores = redis.call(command, results_key, min, max, "withscores")
  else
    ids_with_scores = redis.call(command, results_key, min, max, "withscores", "LIMIT", offset, lim)
  end
else
  ids_with_scores = redis.call(command, results_key, min, max, "withscores")
end

for i, v in ipairs(ids_with_scores) do
  if i%2==1 then  
    if replace_keyf and #replace_keyf > 0 then
      --replace/join logic
      local replacement_id = redis.call("hget", hashkeyf:format(v), replace_id_attr)
      if not replacement_id then
        redis.call("echo", "replacement_id is empty!...")
      else
        v=replacement_id
      end
    end
    
    table.insert(ids, v)
  else
    table.insert(scores, v)
  end
end

if replace_keyf and #replace_keyf > 0 then
  hashkeyf=replace_keyf
end

local ret, notfound = {}, {}
for i,id in ipairs(ids) do
  local flathash = redis.call("hgetall", hashkeyf:format(id))
  if #flathash>0 then
    if #withscores>0 then
      table.insert(flathash, "____score")
      table.insert(flathash, scores[i])
    end
    ret[i] = flathash
  else --notfound
    ret[i] = id
    table.insert(notfound, i-1) --0-based index
  end
end
return {ret, ids, notfound}
