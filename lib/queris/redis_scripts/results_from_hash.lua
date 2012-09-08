local results_key = KEYS[1]
local command, min, max, hashkeyf = ARGV[1], ARGV[2], ARGV[3], ARGV[4]
local ids = redis.call(command, results_key, min, max)
local ret, notfound = {}, {}
for i,id in ipairs(ids) do
  local flathash = redis.pcall("hgetall", hashkeyf:format(id))
  if type(flathash)=="hash" and flathash.err and not next(KEYS, next(KEYS))  then
    --single-key hash with 'err' set, probably an error
    flathash = nil
  end
  if flathash then
    ret[i] = flathash
  else --notfound
    ret[i] = id
    table.insert(notfound, i)
  end
end
return {ret, notfound}
