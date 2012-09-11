local results_key, union_key, diff_key = KEYS[1], KEYS[2], KEYS[3]
local diff_set = redis.call('zrange', diff_key, 0, -1)
for i,v in ipairs(diff_set) do
  redis.call('zrem', results_key, v)
end
local union_and_scores_set = redis.call('zrange', union_key, 0, -1, "withscores")
for i=1,#union_and_scores_set, 2 do
  redis.call('zadd', results_key, union_and_scores_set[i+1], union_and_scores_set[i])
end
return 'OK'