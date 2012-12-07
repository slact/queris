# encoding: utf-8
require 'json'
require 'securerandom'
require "queris/query/operations"
require "queris/query/trace"
module Queris
  class Query
    MINIMUM_QUERY_TTL = 5 #seconds. Don't mess with this number unless you fully understand it, setting it too small may lead to subquery race conditions
    attr_accessor :redis_prefix, :ttl, :created_at, :ops, :sort_ops, :model, :params
    attr_reader :subqueries
    def initialize(model, arg=nil, &block)
      if model.kind_of?(Hash) and arg.nil?
        arg, model = model, model[:model]
      elsif arg.nil?
        arg= {}
      end
      raise "Can't create query without a model" unless model
      raise "include Queris in your model (#{model.inspect})." unless model.include? Queris
      @model = model
      @params = {}
      @ops = []
      @sort_ops = []
      @used_index = {}
      @redis_prefix = arg[:complete_prefix] || ((arg[:prefix] || arg[:redis_prefix] || model.redis_prefix) + self.class.name + ":")
      @redis=arg[:redis]
      @profile = arg[:profiler] || model.query_profiler.new(nil, :redis => @redis || model.redis)
      @subqueries = []
      @ttl= arg[:ttl] || 600 #10 minutes default time-to-live
      @trace=nil
      live! if arg[:live]
      realtime! if arg[:realtime]
      @created_at = Time.now.utc
      if @expire_after = (arg[:expire_at] || arg[:expire] || arg[:expire_after])
        raise "Can't create query with expire_at option and check_staleness options at once" if arg[:check_staleness]
        raise "Can't create query with expire_at option with track_stats disabled" if arg[:track_stats]==false
        arg[:track_stats]=true
        arg[:check_staleness] = Proc.new do |query|
          query.time_cached < (@expire_after || Time.at(0))
        end
      end

      @from_hash = arg[:from_hash]
      @restore_failed_callback = arg[:restore_failed]
      @delete_missing = arg[:delete_missing]

      @track_stats = arg[:track_stats]
      @check_staleness = arg[:check_staleness]
      if block_given?
        instance_eval(&block)
      end
      self
    end

    def redis_master
      Queris.redis :master || @redis || model.redis
    end
    private :redis_master
    
    def redis
      @redis || Queris.redis(:slave) || model.redis || redis_master
    end
    
    def use_redis(redis_instance)
      @redis = redis_instance
      subqueries.each {|sub| sub.use_redis redis_instance}
      self
    end

    def stats
      raise "Query isn't profiled, no stats available" if @profile.nil?
      @profile.load
    end
    
    #TODO: obsolete this
    def track_stats?
      @track_stats
    end
    
    #TODO: obsolete this
    def stats_key
      "#{@redis_prefix}:stats:#{digest explain}"
    end
    
    def time_cached
      Time.at(redis.hget(stats_key, 'time_cached').to_f || 0)
    end
    
    def set_time_cached(val)
      redis.hset stats_key, 'time_cached', val.to_f
    end
    
    def is_stale?
      if @check_staleness.kind_of? Proc
        @check_staleness.call self
      end
    end
    
    #retrieve query parameters, as fed through union and intersect and diff
    def param(param_name)
      @params[param_name.to_sym]
    end

    #the set operations
    def union(index, val=nil)
      prepare_op UnionOp, index, val
    end
    alias :'∪' :union #UnionOp::SYMBOL

    def intersect(index, val=nil)
      prepare_op IntersectOp, index, val
    end
    alias :'∩' :intersect #IntersectOp::SYMBOL

    def diff(index, val=nil)
      prepare_op DiffOp, index, val
    end
    alias :'∖' :diff #DiffOp::SYMBOL

    def prepare_op(op_class, index, val)
      index = @model.redis_index index
      raise "Recursive subquerying doesn't do anything useful." if index == self
      set_param_from_index index, val

      #set range and enumerable hack
      if op_class != UnionOp && ((Range === val && !index.handle_range?) || (Enumerable === val &&  !(Range === val)))
        #wrap those values in a union subquery
        sub_union = subquery
        val.each { |v| sub_union.union index, v }
        index, val = sub_union, nil
      end

      use_index index  #accept string index names and indices and queries
      @results_key = nil
      op = op_class.new
      last_op = ops.last
      if last_op && !op.fragile && !last_op.fragile && last_op.class == op_class
        last_op.push index, val
      else
        op.push index, val
        ops << op
      end
      self
    end
    private :prepare_op

    #undo the last n operations
    def undo (num_operations=1)
      return self if num_operations == 0 || ops.empty?
      @results_key = nil
      op = ops.last
      raise "Unexpected operand-less query operation" unless (last_operand = op.operands.last)
      if Query === (sub = last_operand.index)
        subqueries.delete sub
      end
      op.operands.pop
      ops.pop if op.operands.empty?
      undo(num_operations - 1)
    end

    def sort(index, reverse = nil)
      # accept a minus sign in front of index name to mean reverse
      @results_key = nil
      if Query === index
        raise ArgumentError, "Sort can't be extracted from queries over different models." unless index.model == model
        sort_query = index
        if sort_query.sort_ops.empty?
          index = nil
        else #copy sort from another query
          sort_query.sort_ops.each do |op|
            op.operands.each do |operand| 
              use_index operand.index unless Query === index
            end
          end
          self.sort_ops = sort_query.sort_ops.dup
          sort_query.sort false #unsort sorted query - legacy behavior, probably a bad idea in the long run
        end
      else
        if index.respond_to?('[]') 
          if index[0] == '-'
            reverse, index = true, index[1..-1]
          elsif index[0] == '+'
            reverse, index = false, index[1..-1]
          end
        end
        if index
          index = use_index index #accept string index names and indices and queries
          real_index = ForeignIndex === index ? index.real_index : index
          raise ArgumentError, "Must have a RangeIndex for sorting, found #{real_index.class.name}" unless RangeIndex === real_index
          self.sort_ops.clear << SortOp.new.push(index, reverse)
        else
          self.sort_ops.clear
        end
      end
      self
    end
    def sortby(index, direction = 1) #SortOp::SYMBOL
      sort index, direction == -1
    end
    
    def sorting_by? index
      val = 1
      if index.respond_to?("[]") && !(Index === index) then
        if index[0]=='-' then
          index, val = index[1..-1], -1
        end
      end
      begin
        index=@model.redis_index(index)
      rescue
        index = nil
      end
      if index
        sort_ops.each do |op|
          op.operands.each do |o|
            return true if o.index == index && o.value == val
          end
        end
      end
      nil
    end
    
    #get an object's sorting score, or its previous sorting score if asked for
    def sort_score(obj, arg={})
      score = 0
      sort_ops.each do |op|
        op.operands.each do |o|
          if arg[:previous]
            val = obj.send "#{o.index.attribute}_was"
          else
            val = obj.send o.index.attribute
          end
          
          score += o.value * (val || 0).to_f
        end
      end
      score
    end
    
    def sorting_by
      sorting = sort_ops.map do |op|
        op.operands.map{|o| "#{(o.value < 0) ? '-' : ''}#{o.index.name}" }.join('+')
      end.join('+')
      sorting.empty? ? nil : sorting.to_sym
    end
    def sort_mult #sort multiplier (direction) -- currently, +1 or -1
      return 1 if sort_ops.empty?
      sort_ops.first.operands.first.value
    end

    def resort #apply a sort to set of existing results
      @resort=true
      self
    end

    def profiler
      @profile
    end
    
    def live?; @live; end
    def live=(val);  @live= val; end
    #static queries are updated only after they expire
    def static?; !live!; end
    def static! 
      @live=false; @realtime=false; self; 
    end
    #live queries have pending updates stored nearby
    def live!; @live=true; @realtime=false; self; end
    #realtime queries are updated automatically, on the spot
    def realtime!
      live!
      @realtime = true
    end
    def realtime?
      live? && @realtime
    end

    #update query results with object(s)
    def update(obj, arg={})
      if uses_index_as_results_key? # DISCUSS : query.union(subquery) won't be updated
        return true
      end
      obj_id = model === obj ? obj.id : obj  #BUG-IN-WAITING: HARDCODED id attribute
      myredis = arg[:redis] || redis_master || redis
      if arg[:delete]
        ret = myredis.zrem results_key, obj_id
      else
        ret = myredis.zadd results_key(:delta), 0, obj_id
      end
      ret
    end

    def delta(arg={})
      myredis = arg[:redis] || redis_master || redis
      myredis.zrange results_key(:delta), 0, -1
    end
    def uses_index_as_results_key?
      if ops.length == 1 && sort_ops.empty? && ops.first.operands.length == 1
        first_op = ops.first.operands.first
        first_index = first_op.index
        if first_index.usable_as_results? first_op.value
          return first_index.key first_op.value
        end
      end
      nil
    end

    def usable_as_results?(*arg)
      true
    end
    
    #a list of all keys that need to be refreshed (ttl extended) for a query
    def volatile_query_keys
      #first element MUST be the existence flag key
      #second element MUST be the results key
      [results_key(:exists), results_key, results_key(:marshaled)]
    end
    
    #all keys related to a query
    def all_query_keys
      all = volatile_query_keys
      all << results_key(:delta) if live?
    end
    
    #check for query existence and atomically extend its ttl so that the query keys do not expire while said query is running
    def ensure_temporary_existence(r=nil, min_ttl=nil)
      # redis expires keys ONLY on master, and propagates to slaves by issuing DEL commands.
      # if we don't renew an existing query on master before it is executed, a race condition may arise
      # when a master sends DELetes just after said query is done running on a slave.
      #there is also the option to extend an existing query ttl to ensure it will not expire in the next min_ttl seconds

      return true if uses_index_as_results_key?
      #check for existence on master
      ret = Queris.run_script :query_ensure_existence, redis_master, volatile_query_keys, [ttl, min_ttl]
      r ||= redis
      if ret &&  redis_master != r
        # check if the result set exists on our slave server (if it doesn't it will be a string)
        ret = Queris.run_script :match_key_type, r, [results_key], [:set, :zset]
      end
      ret
    end
    
    #check for the existence of a result set. We need to do this in case the result set is empty
    def exists?(r=nil)
      return true if uses_index_as_results_key?
      raise "No redis connection to check query existence" if r.nil?
      Queris.run_script :query_exists_locally, (r || redis), [results_key(:exists), results_key]
    end
    
    def run(opt={})
      raise "No redis connection found for query #{self} for model #{self.model.name}." if redis.nil?
      @profile.id=self

      force = opt[:force] || is_stale?
      force = nil if Numeric === force && force <= 0
      @trace = (opt[:trace] || @must_trace) ? Trace.new(self, (opt[:trace] || @must_trace)) : false
      model.run_query_callbacks :before_run, self
      if uses_index_as_results_key?
        #puts "QUERY #{@model.name} #{explain} shorted to #{results_key}"
        #do nothing, we're using a results key directly
        @profile.record :cache_hit, 1
        @trace.message "Using index as results key." if @trace
        set_time_cached Time.now if track_stats?
      else
        if (query_not_found = (opt[:assume_missing] || force || !ensure_temporary_existence)) #see what i did there?
          #puts "#{self} does not exist or is being forced"
          @profile.record :cache_miss, 1
          run_static_query force
        else
          if live? && !opt[:no_update]
            live_update_msg = Queris.run_script(:update_query, redis, [results_key(:marshaled)], [Time.now.utc.to_f])
            @trace.message "Live query update: #{live_update_msg}" if @trace
          end
          @trace.message "Query results already exist, no trace available." if @trace
          @profile.record :cache_hit, 1
        end
        #okay, this query is now ready
        redis_master.multi do |r|
          r.setex results_key(:exists), ttl, 1
          r.setnx results_key, 1
          r.expire results_key, ttl
          if query_not_found
            r.setex results_key(:marshaled), ttl, JSON.dump(json_redis_dump) 
            r.del results_key(:delta) if live?
          end
        end
      end

      if @resort #just sort
        #TODO: profile resorts
        redis.multi do |predis|
          sort_ops.each { |op| op.run predis, results_key }
        end
      end
      self
    end
    alias :query :run

    def run_static_query(force=nil)
      @profile.start :time
      # we must use the same redis server everywhere to prevent replication race conditions
      # redis cluster should, I suspect, provide the tools to address this in the future.
      
      #only run nonexistent subqueries, unless tracing
      subq_exist={}
      if @trace #check everything when tracing
        @subqueries.each{|q| subq_exist[q]=false}
      else
        redis.multi { |r| @subqueries.each { |q| subq_exist[q] = q.ensure_temporary_existence(r, self.class::MINIMUM_QUERY_TTL) } }
      end
      subq_exist.each do |q, exist|
        next if (Redis::Future === exist ? exist.value : exist)
        q.run :force => (Numeric === force ? force - 1 : force), :assume_missing => true, :trace => @trace
      end
    
      #puts "QUERY #{@model.name} #{explain} #{force ? "forced" : ''} full query"
      @profile.start :own_time
      temp_results_key = results_key "querying:#{SecureRandom.hex}"
      trace_callback = @trace ? @trace.method(:op) : nil
      redis.pipelined do |pipelined_redis|
      #pipelined_redis = redis #for debugging
        first_op = ops.first
        [ops, sort_ops].each do |ops|
          ops.each do |op|
            op.run pipelined_redis, temp_results_key, first_op == op, trace_callback
          end
        end
        pipelined_redis.multi do |r|
          r.evalsha Queris.script_hash(:move_key), [temp_results_key, results_key]
          r.setex results_key(:exists), ttl, 1
        end
        #puts "QUERY TTL: ttl"
        (redis_master || pipelined_redis).pipelined do |r|
          #live query registration
          if live?
            all_live_indices.each do |index|
              index.add_live_query self
            end
          end
        end
      end
      @profile.finish :own_time
      set_time_cached Time.now if track_stats?
      @profile.finish :time
      @profile.save
    end
    private :run_static_query
    def trace!(opt={})
      if opt==false
        @must_trace = false
        return self
      elsif opt
        @must_trace = opt
      end
      
    end
    def trace?
      @must_trace || @trace
    end
    def trace(opt={})
      indent = opt[:indent] || 0
      buf = "#{"  " * indent}#{indent == 0 ? 'Query' : 'Subquery'} #{self}:\r\n"
      buf << "#{"  " * indent}ttl:#{ttl}, |#{redis.type(key)} results key|=#{count(:no_run => true)}\r\n"
      buf << "#{"  " * indent}trace:\r\n"
      case @trace
      when nil
        buf << "#{"  " * indent}No trace available, query hasn't been run yet."
      when false
        buf << "#{"  " * indent}No trace available, query was run without :trace parameter. Try query.run(:trace => true)"
      else
        buf << @trace.indent(indent).to_s
      end
      opt[:output]!=false ? puts(buf) : buf
    end
    
    def uses_index?(*arg)
      arg.each do |ind|
        index_name = Queris::Index === ind ? ind.name : ind.to_sym
        return true if @used_index[index_name]
      end
      false
    end

    #list all indices used by a query (no subqueries, unless asked for)
    def indices(opt={})
      ret = @used_index.dup
      if opt[:subqueries]
        subqueries.each do |sub|
          ret.merge! sub.indices(subqueries: true, hash: true)
        end
      end
      if opt[:hash]
        ret
      else
        ret.values
      end
    end
    #list all indices (including subqueries)
    def all_indices
      indices :subqueries => true
    end
    def all_live_indices
      return [] unless live?
      ret = all_indices
      ret.select! do |i|
        (ForeignIndex === i ? i.real_index : i).live?
      end
      ret
    end

    # recursively and conditionally flush query and subqueries
    # arg parameters: flush query if:
    #  :ttl - query.ttl <= ttl
    #  :index (symbol or index or an array of them) - query uses th(is|ese) ind(ex|ices)
    # or flush conditionally according to passed block: flush {|query| true }
    # when no parameters or block present, flush only this query and no subqueries
    def flush(arg={})
      model.run_query_callbacks :before_flush, self
      return if uses_index_as_results_key?
      flushed = 0
      if block_given? #efficiency hackety hack - anonymous blocs are heaps faster than bound ones
        subqueries.each { |sub| flushed += sub.flush arg, &Proc.new }
      elsif arg.count>0
        subqueries.each { |sub| flushed += sub.flush arg }
      end
      if flushed > 0 || arg.count==0 || ttl <= (arg[:ttl] || 0) || (uses_index?(*arg[:index])) || block_given? && (yield sub)
        #this only works because of the slave EXPIRE hack requiring dummy query results_keys on master.
        #otherwise, we'd have to create the key first (in a MULTI, of course)
        res = (redis_master || redis).multi do |r|
          r.del all_query_keys
        end
        #Queris::QueryStore.remove(self) if live? && !uses_index_as_results_key?
        flushed += res.first if res
      end
      flushed
    end
    alias :clear :flush
    
    #flexible query results retriever
    #results(x..y) from x to y
    #results(x, y) same
    #results(x) first x results
    #results(x..y, :reverse) range in reverse
    #results(x..y, :score =>a..b) results from x to y with scores in given score range
    #results(x..y, :with_scores) return scores with results as [ [score1, res1], [score2, res2] ... ]
    def results(*arg)
      run :no_update => !realtime?
      opt= Hash === arg.last ? arg.pop : {}
      opt[:reverse]=true if arg.member?(:reverse)
      opt[:with_scores]=true if arg.member?(:with_scores)
      opt[:range]=arg.shift if Range === arg.first
      opt[:range]=(arg.shift .. arg.shift) if Numeric === arg[0] && arg[0].class == arg[1].class
      opt[:range]=(0..arg.shift) if Numeric === arg[0]
      opt[:raw] = true if arg.member? :raw
      
      key = results_key
      case (keytype=redis.type(key))
      when 'set'
        raise "Can't range by score on a regular results set" if opt[:score]
        raise "Cannot get result range from shortcut index result set (not sorted); must retrieve all results. This is a temporary queris limitation." if opt[:range]
        cmd, first, last, rangeopt = :smembers, nil, nil, {}
      when 'zset'
        rangeopt = {}
        rangeopt[:with_scores] = true if opt[:with_scores]
        if (r = opt[:range])
          first, last = r.begin, r.end - (r.exclude_end? ? 1 : 0)
          raise "Query result range must have numbers, instead there's a #{first.class} and #{last.class}" unless Numeric === first && Numeric === last
          raise "Query results range must have whole-number endpoints" unless first.round == first && last.round == last
        end
        if (scrange = opt[:score])
          rangeopt[:limit] = [ first, last - first ] if opt[:range]
          raise "Can't fetch results with_scores when also limiting them by score. Pick one or the other." if opt[:with_scores]
          raise "query.results :score parameter must be a Range" unless Range === scrange
          first = Queris::to_redis_float(scrange.begin * sort_mult)
          last = Queris::to_redis_float(scrange.end * sort_mult)
          last = "(#{last}" if scrange.exclude_end?
          first, last = last, first if sort_mult == -1
          cmd = opt[:reverse] ? :zrevrangebyscore : :zrangebyscore
        else
          cmd = opt[:reverse] ? :zrevrange : :zrange
        end
      else
        return []
      end
      @profile.start :results_time
      if block_given? && opt[:replace_command]
        res = yield cmd, key, first || 0, last || -1, rangeopt
      elsif @from_hash && !opt[:raw]
        if rangeopt[:limit]
          limit, offset = *rangeopt[:limit]
        else
          limit, offset = nil, nil
        end
        raw_res, ids, failed_i = redis.evalsha(Queris::script_hash(:results_from_hash), [key], [cmd, first || 0, last || -1, @from_hash, limit, offset])
        res, to_be_deleted = [], []
        raw_res.each_with_index do |raw_hash, i|
          if failed_i.first == i
            failed_i.shift
            if @restore_failed_callback
              obj = @restore_failed_callback.call raw_hash, self
            else
              obj = model.find_cached raw_hash
            end
          else
            hash = Hash[*raw_hash] if Array === raw_hash
            unless (obj = model.restore(hash, ids[i]))
              #we could stil have received an invalid cache object (too few attributes, for example)
              if @restore_failed_callback
                obj = @restore_failed_callback.call ids[i], self
              else
                obj = model.find_cached ids[i]
              end
            end
          end
          if not obj.nil?
            res << obj
          elsif @delete_missing
            to_be_deleted << ids[i]
          end
        end
        redis.evalsha Queris.script_hash(:remove_from_sets), all_index_keys, to_be_deleted unless to_be_deleted.empty?
      else
        if cmd == :smembers
          res = redis.send(cmd, key)
        else
          res = redis.send(cmd, key, first || 0, last || -1, rangeopt)
        end
      end
      if block_given? && !opt[:replace_command]
        if opt[:with_scores]
          ret = []
          res.each do |result|
            obj = yield result.first
            ret << [obj, result.last] unless obj.nil?
          end
          res = ret
        else
          res.map!(&Proc.new).compact!
        end
      end
      @profile.finish :results_time
      @profile.save
      res
    end
    def raw_results(*arg)
      arg.push :raw
      results(*arg)
    end
    
    def member?(id)
      id = id.id if model === id
      run :no_update => !realtime?
      case redis.type(results_key)
      when 'set'
        redis.sismember(results_key, id)
      when 'zset'
        !redis.zrank(results_key, id).nil?
      when 'none'
        false
      else
        #what happened?
      end
    end
    alias :contains? :member?
    
    
    def result(n)
      res = results(n...n+1)
      if res.length > 0 
        res.first
      else
        nil
      end
    end
    
    def first_result
      return result 0
    end
    
    def results_key(suffix = nil, raw_id = nil)
      if @results_key.nil? or raw_id
        if (reused_set_key = uses_index_as_results_key?)
          @results_key = reused_set_key
        else
          theid = raw_id || (digest(explain :subqueries => false) << ":subqueries:#{(@subqueries.length > 0 ? @subqueries.map{|q| q.id}.sort.join('&') : 'none')}" << ":sortby:#{sorting_by || 'nothing'}")
          thekey = "#{@redis_prefix}results:#{theid}"
          if raw_id
            return thekey
          else
            @results_key = thekey
          end
        end
      end
      if suffix
        "#{@results_key}:#{suffix}"
      else
        @results_key
      end
    end
    def key arg=nil
      results_key
    end
    alias :key_for_query :key
    
    def id
      digest results_key
    end
    
    def count(opt={})
      run(:no_update => !realtime?) unless opt [:no_run]
      key = results_key
      case redis.type(key)
      when 'set'
        raise "Query results are not a sorted set (maybe using a set index directly), can't range" if opt[:score]
        redis.scard key
      when 'zset'
        if opt[:score]
          range = opt[:score]
          raise "'score' option must be a Range, but it's a #{range.class} instead" unless Range === range
          first = range.begin * sort_mult
          last = range.exclude_end? ? "(#{range.end.to_f * sort_mult}" : range.end.to_f * sort_mult
          first, last = last, first if sort_mult == -1
          redis.zcount(key, first, last)
        else
          redis.zcard key
        end
      else #not a set. 
        0
      end
    end
    alias :size :count
    alias :length :count

    
    def subquery arg={}
      @used_subquery ||= {}
      @results_key = nil
      if arg.kind_of? Query
        subq = arg
      else
        subq = self.class.new((arg[:model] or model), arg.merge(:redis_prefix => redis_prefix, :ttl => @ttl))
      end
      subq.use_redis redis
      unless @used_subquery[subq]
        @used_subquery[subq]=true
        @subqueries << subq
      end
      subq
    end
    def subquery_id(subquery)
      @subqueries.index subquery
    end
    
    def explain(opt={})
      return "(∅)" if ops.nil? || ops.empty?
      first_op = ops.first
      r = ops.map do |op|
        operands = op.operands.map do |o|
          if Query === o.index
            if opt[:subqueries] != false 
              o.index.explain opt
            else
              "{subquery #{subquery_id o.index}}"
            end
          else
            value = case opt[:serialize]
            when :json
              JSON.dump o.value
            when :ruby
              Marshal.dump o.value
            else #human-readable and sufficiently unique
              o.value.to_s
            end
            "#{o.index.name}#{(value.empty? || opt[:structure])? nil : "<#{value}>"}"
          end
        end
        op_str = operands.join " #{op.symbol} "
        if first_op == op
          op_str.insert 0, "∅ #{op.symbol} " if DiffOp === op
        else
          op_str.insert 0, " #{op.symbol} "
        end
        op_str
      end
      "(#{r.join})"
    end
    def to_s
      explain
    end
    
    def structure
      explain :structure => true
    end
    
    def info(opt={})
      ind = opt[:indent] || ""
      info = "#{ind}#{self} info:\r\n"
      info <<  "#{ind}key: #{results_key}\r\n" unless opt[:no_key]
      info << "#{ind}redis key type:#{redis.type key}, size: #{count :no_run => true}\r\n" unless opt[:no_size]
      info << "#{ind}liveliness:#{live? ? (realtime? ? 'realtime' : 'live') : 'static'}"
      if live?
        
      end
      info << "\r\n"
      info << "#{ind}id: #{id}, ttl: #{ttl}, sort: #{sorting_by || "none"}\r\n" unless opt[:no_details]
      info << "#{ind}remaining ttl: results: #{redis.pttl(results_key)}ms, existence flag: #{redis.pttl results_key(:exists)}ms, marshaled: #{redis.pttl results_key(:marshaled)}ms\r\n" if opt[:debug_ttls]
      unless @subqueries.empty? || opt[:no_subqueries]
        info << "#{ind}subqueries:\r\n"
        @subqueries.each do |sub|
          info << sub.info(opt.merge(:indent => ind + "  ", :output=>false))
        end
      end
      opt[:output]!=false ? puts(info) : info
    end
    
    def marshal_dump
      subs = {}
      @subqueries.each { |sub| subs[sub.id.to_sym]=sub.marshal_dump }
      unique_params = params.dup
      each_operand do |op|
        unless Query === op.index
          param_name = op.index.name
          unique_params.delete param_name if params[param_name] == op.value
        end
      end
      {
        model: model.name.to_sym,
        ops: ops.map{|op| op.marshal_dump},
        sort_ops: sort_ops.map{|op| op.marshal_dump},
        subqueries: subs,
        params: unique_params,
        
        args: {
          complete_prefix: redis_prefix,
          ttl: ttl,
          expire_after: @expire_after,
          track_stats: @track_stats,
          live: @live,
          realtime: @realtime,
          from_hash: @from_hash,
          delete_missing: @delete_missing
        }
      }
    end

    def json_redis_dump
      queryops = []
      ops.each {|op| queryops.concat(op.json_redis_dump)}
      queryops.reverse!
      sortops = []
      sort_ops.each{|op| sortops.concat(op.json_redis_dump)}
      ret = {
        key: results_key,
        delta_key: results_key(:delta),
        live: live?,
        realtime: realtime?,
        ops_reverse: queryops,
        sort_ops: sortops
      }
      ret
    end

    def marshal_load(data)
      if Hash === data
        initialize Queris.model(data[:model]), data[:args]
        subqueries = {}
        data[:subqueries].map do |id, sub|
          q = Query.allocate
          q.marshal_load sub
          subqueries[id]=q
        end
        [ data[:ops], data[:sort_ops] ].each do |operations| #replay all query operations
          operations.each do |operation|
            operation.last.each do |op|
              index = subqueries[op[0]] || @model.redis_index(op[0])
              self.send operation.first, index, op.last
            end
          end
        end
        data[:params].each do |name, val|
          params[name]=val
        end
      else #legacy
        if data.kind_of? String
          arg = JSON.load(data)
        elsif data.kind_of? Enumerable
          arg = data
        else
          arg = [] #SILENTLY FAIL RELOADING QUERY. THIS IS A *DANGER*OUS DESIGN DECISION MADE FOR THE SAKE OF CONVENIENCE.
        end
        arg.each { |n,v| instance_variable_set "@#{n}", v }
      end
    end
    def marshaled
      Marshal.dump self
    end

    def each_operand(which_ops=nil) #walk though all query operands
      (which_ops == :sort ? sort_ops : ops).each do |operation|
        operation.operands.each do |operand|
          yield operand, operation
        end
      end
    end
    
    def all_index_keys
      keys = []
      [ops, sort_ops].each do |ops|
        ops.each { |op| keys.concat op.keys(nil, true) }
      end
      keys << key
      keys.uniq
    end

    private
    
    def use_index *arg
      if (res=@model.redis_index(*arg)).nil?
        raise ArgumentError, "Invalid Queris index (#{arg.inspect}) passed to query. May be a string (index name), an index, or a query."
      else
        if Query === res
          subquery res
        else
          @used_index[res.name.to_sym]=res if res.respond_to? :name
        end
        res
      end
    end
    
    def used_indices; @used_index; end
    def set_param_from_index(index, val)
      index = use_index index
      @params[index.name]=val if index.respond_to? :name
      val
    end
    
    def digest(value)
      Queris.debug? ? value : Digest::SHA1.hexdigest(value.to_s)
    end
  end
end
