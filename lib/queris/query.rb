# encoding: utf-8
require 'json'
require 'securerandom'
module Queris
  class Query
    
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
      if live?
        @redis || redis_master || model.redis
      else
        @redis || model.redis || Queris.redis(:slave) || redis_master
      end
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
        if index.respond_to?('[]') && index[0] == '-'
          reverse, index = true, index[1..-1]
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
      if uses_index_as_results_key?
        #puts "No need to update #{self}"
        return self 
      end
      obj_id = model === obj ? obj.id : obj  #BUG-IN-WAITING: HARDCODED id attribute
      if arg[:delete]
        (redis_master || redis).zrem results_key, obj_id
      else
        (redis_master || redis).evalsha Queris.script_hash(:update_query), [results_key(:marshaled)], [obj_id]
      end
      #puts "updated #{self}"
      self
    end

    def uses_index_as_results_key?
      if ops.length == 1 && sort_ops.empty? && ops.first.operands.length == 1
        first_op = ops.first.operands.first
        first_index = first_op.index
        unless Enumerable === first_op.value || first_index.respond_to?(:before_query_op) || first_index.respond_to?(:after_query_op)
          return first_index.key first_op.value
        end
      end
      nil
    end

    
    #Level Cleared. Time extended!
    def extend_ttl(r=nil, opt={})
      return (redis_master || redis).multi{ |multir| extend_ttl multir } if r.nil?
      r.expire results_key, ttl
      r.setex results_key(:exists), ttl, ""
      if live?
        r.expire results_key(:marshaled), ttl
        QueryStore.update self
      end
      self
    end
    
    #check for the existence of a result set. We need to do this in case the result set is empty
    def results_exist?(r=nil)
    return true if uses_index_as_results_key?
    r ||= redis_master || redis
    raise "No redis connection to check query existence" if r.nil?
    r.exists results_key(:exists)
    end
    
    def run(opt={})
      raise "No redis connection found for query #{self} for model #{self.model.name}." if redis.nil?
      @profile.id=self
      force=opt[:force] || is_stale?
      if uses_index_as_results_key?
        #puts "QUERY #{@model.name} #{explain} shorted to #{results_key}"
        #do nothing, we're using a results key directly
        @profile.record :cache_hit, 1
        set_time_cached Time.now if track_stats?
      elsif force || !results_exist?
        #puts "#{self} does not exist or is being forced"
        @profile.record :cache_miss, 1
        run_static_query force, opt[:debug], opt[:forced_results_redis]
        if live? && !uses_index_as_results_key?
          Queris::QueryStore.add(self)
        end
      elsif live?
        run_live_query opt[:no_update]
      else
        @profile.record :cache_hit, 1
        extend_ttl
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

    def run_live_query(skip_update = nil)
      if realtime?
        #nothing to do but update ttl
        #puts "#{self} is a realtime query, nothing to do..."
        realtime!
        return extend_ttl
      end
      return if skip_update
      #puts "#{self} live query"
      union_key= results_key 'delta:union'
      diff_key= results_key 'delta:diff'

      #all live operations happen on master
      (redis_master || redis).evalsha Queris.script_hash(:apply_query_delta), [results_key, union_key, diff_key]
      extend_ttl
    end
    private :run_live_query

    attr_accessor :share_results
    def share_results? #run query on master or slave
      live? || @share_results
    end
    def share_results!; @share_results = true; self; end
    def run_static_query(force=nil, debug=nil, forced_results_redis=nil)
      @profile.start :time
      master = redis_master
      results_redis = forced_results_redis || share_results? ? master : redis
      # we must use the same redis server everywhere to prevent replication race conditions
      # (query on slave, subquery on master that doesn't finish replicating to slave when the query needs subquery results)
      # redis cluster should, I suspect, provide the tools to address this in the future.
      subq_exist ={} 
      results_redis.multi do |r|
        @subqueries.each { |q| subq_exist[q] = q.results_exist? r }
      end
      subq_exist.each do |q, exist|
        next if exist.value
        q.query :force => force, :debug => debug, :forced_results_redis => results_redis
      end
      
      #puts "QUERY #{@model.name} #{explain} #{force ? "forced" : ''} full query"
      @profile.start :own_time
      debug_info = []
      temp_results_key = results_key "querying:#{SecureRandom.hex}"
      #results_redis.pipelined do |pipelined_redis|
      pipelined_redis = results_redis
        first_op = ops.first
        [ops, sort_ops].each do |ops|
          ops.each do |op|
            op.run pipelined_redis, temp_results_key, first_op == op
            debug_info << [op.to_s, pipelined_redis.zcard(temp_results_key)] if debug
          end
        end
        pipelined_redis.evalsha Queris.script_hash(:rename_if_present), [temp_results_key, results_key]
        #puts "QUERY TTL: ttl"
        if results_redis == master
          extend_ttl(pipelined_redis)
          pipelined_redis.setex(results_key(:marshaled), ttl, JSON.dump(json_redis_dump)) if live?
        end
      #end
      @profile.finish :own_time
      unless master.nil? || master == results_redis
        #Redis slaves can't expire keys by themselves (for the sake of data consistency). So we have to store some dummy value at results_keys in master with an expire.
        #this is gnarly. Hopefully future redis versions will give slaves optional EXPIRE behavior.
        master.multi do |m|
          m.setnx results_key, 1
          #setnx because someone else might've created it while the app was twiddling its thumbs. Setting it again would erase some slave's result set
          extend_ttl m
          m.setex(results_key(:marshaled), ttl, JSON.dump(json_redis_dump)) if live?
        end
      end
      set_time_cached Time.now if track_stats?
      @profile.finish :time
      @profile.save

      unless debug_info.empty?
        #debug_info.map {|line| "#{line.first.symbol} #{line.first.attribute} .#{line[0].value}
        puts "Debugging query #{self}"
        debug_info.each { |l| puts " #{l.last.value}   #{l.first}"}
      end
    end
    private :run_static_query
    
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
        if ForeignIndex === i
          i.real_index.model.live_index? i
        else
          i.model.live_index? i
        end
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
          r.del results_key
          r.del results_key(:exists)
          r.del results_key(:marshaled) if live?
        end
        Queris::QueryStore.remove(self) if live? && !uses_index_as_results_key?
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
    #results(x..y, :with_scores) return scores with results as [ [score1, res1], [score2, res2] ... ]
    def results(*arg)
      run :no_update => true
      opt= Hash === arg.last ? arg.pop : {}
      opt[:reverse]= true if arg.member?(:reverse)
      opt[:with_scores]=true if arg.member?(:with_scores)
      opt[:range]=arg.shift if Range === arg.first
      first, last = arg.shift, arg.shift if Numeric === arg[0] && arg[0].class == arg[1].class
      opt[:range]=(0..arg.shift) if Numeric === arg[0]
      
      @profile.start :results_time
      key = results_key
      case redis.type(key)
      when 'set'
        if block_given? && opt[:replace_command]
          res = yield :smembers, key, nil, nil, {}
        else
          res = redis.smembers key
        end
        raise "Cannot get result range from shortcut index result set (not sorted); must retrieve all results. This is a temporary queris limitation." unless arg.empty?
      when 'zset'
        rangeopt = {}
        rangeopt[:with_scores] = true if opt[:with_scores]
        if (scrange = opt[:score])
          raise "query.results :score parameter must be a Range" unless Range === scrange
          raise "Can't select result range numerically and by score (pick one, not both)" if opt[:range]
          first = Queris::to_redis_float(scrange.begin)
          last = Queris::to_redis_float(scrange.end)
          last = "(#{last}" if scrange.exclude_end?
          cmd = opt[:reverse] ? :zrevrangebyscore : :zrangebyscore
        else
          if (range = opt[:range])
            first, last = range.begin, range.end - (range.exclude_end? ? 1 : 0)
          end
          cmd = opt[:reverse] ? :zrevrange : :zrange
        end
        if block_given? && opt[:replace_command]
          res = yield cmd, key, first, last, rangeopt
        else
          res = redis.send(cmd, key, first || 0, last || -1, rangeopt)
        end
      else
        res = []
      end
      if block_given? && !opt[:replace_command]
        if opt[:with_scores]
          ret = []
          res.each do |r|
            obj = yield r.first
            ret << [obj, r.last] unless obj.nil?
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
    alias :raw_results :results
    
    def contains?(id)
      run :no_update => true
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
    
    def id
      digest results_key
    end
    
    def length
      run :no_update => true
      key = results_key
      case redis.type(key)
      when 'set'
        redis.scard key
      when 'zset'
        redis.zcard key
      else #not a set. 
        0
      end
    end
    alias :size :length
    alias :count :length

    
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
    
    def info(indent="", output = true)
      info =  "#{indent}key: #{results_key}\r\n"
      info << "#{indent}id: #{id}, ttl: #{ttl}, sort: #{sorting_by || "none"}\r\n"
      info << "#{indent}#{explain}\r\n"
      if !@subqueries.empty?
        info << "#{indent}subqueries:\r\n"
        @subqueries.each do |sub|
          info << sub.info(indent + "  ", false)
        end
      end
      output ? puts(info) : info
    end
    
    def marshal_dump
      subs = {}
      @subqueries.each { |sub| subs[sub.id.to_sym]=sub.marshal_dump }
      unique_params = params.dup
      each_operand do |op|
        unless Query === op.index
          #binding.pry
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
          realtime: @realtime
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
        live: live?,
        realtime: realtime?,
        ops_reverse: queryops,
        sort_ops: sortops
      }
      if live? && !realtime?
        ret[:union_delta_key] = results_key :'delta:union'
        ret[:diff_delta_key] = results_key :'delta:diff'
      end
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

    private

    class Op #query operation
      class Operand #boilerplate
        attr_accessor :index, :value
        def initialize(op_index, val)
          @index = op_index
          @value = val
        end
        def marshal_dump
          [(Query === index ? index.id : index.name).to_sym, value]
        end
        
        def json_redis_dump(op_name = nil)
          ret = []
          miniop = {}
          if Query === @index 
            miniop = {query: index.json_redis_dump}
          elsif Range === @value && @index.handle_range?
            miniop[:min] = @index.val(@value.begin)
            miniop[@value.exclude_end? ? :max : :max_or_equal] = @index.val(@value.end)
            miniop[:key] = @index.key
          elsif Enumerable === value
            value.each do |val|
              miniop = { equal: @index.val(val), key: @index.key(val) }
              miniop[:op] = op_name if op_name
              ret << miniop
            end
            return ret
          else
            miniop[:equal] = @index.val(@value)
          end
          miniop[:key] = @index.key(@value)
          miniop[:op]=op_name if op_name
          ret << miniop
          ret
        end
      end

      attr_accessor :operands, :fragile
      def initialize(fragile=false)
        @operands = []
        @keys = []
        @weights = []
        @fragile = fragile
      end
      def push(index, val) # push operand
        @ready = nil
        @operands << Operand.new(index,val)
        self
      end
      def symbol
        @symbol || self.class::SYMBOL
      end
      def command
        @command || self.class::COMMAND
      end
      def keys(target_key, first = nil)
        prepare
        @keys[0]=target_key
        first ? @keys[1..-1] : @keys
      end
      def weights(first = nil)
        prepare
        first ? @weights[1..-1] : @weights
      end
      def target_key_weight
        1
      end
      def operand_key_weight(op)
        1
      end
      def prepare
        return if @ready
        @keys, @weights = [:result_key], [target_key_weight]
        operands.each do |op|
          k = op.index.key op.value
          num_keys = @keys.length
          if Array === k
            @keys |= k
          else
            @keys << k
          end
          if (@keys.length - num_keys) < 0
            raise ArgumentError, "something really wrong here"
          end
          @weights += [ operand_key_weight(op) ] * (@keys.length - num_keys)
        end
        @ready = true
      end
      def run(redis, target, first=false)
        operands.each { |op| op.index.before_query_op(redis, target, op.value, op) if op.index.respond_to? :before_query_op }
        redis.send self.class::COMMAND, target, keys(target, first), :weights => weights(first)
        operands.each { |op| op.index.after_query_op(redis, target, op.value, op) if op.index.respond_to? :after_query_op }
      end

      def marshal_dump
        [self.class::SYMBOL, operands.map {|op| op.marshal_dump}]
      end
      def json_redis_dump(etc={})
        all_ops = []
        operands.map do |op|
          all_ops.concat op.json_redis_dump(self.class::NAME)
        end
        all_ops
      end
      def to_s
        "#{symbol} #{operands.map{|o| Query === o.index ? o.index : "#{o.index.name}<#{o.value}>"}.join(" #{symbol} ")}"
      end
    end
    class UnionOp < Op
      COMMAND = :zunionstore
      SYMBOL = :'∪'
      NAME = :union
    end
    class IntersectOp < Op
      COMMAND = :zinterstore
      SYMBOL = :'∩'
      NAME = :intersect
    end
    class DiffOp < Op
      COMMAND = :zunionstore
      SYMBOL = :'∖'
      NAME = :diff
      def target_key_weight; 0; end
      def operand_key_weight(op=nil); :'-inf'; end
      def run(redis, result_key, first=nil)
        super redis, result_key, first
        redis.zremrangebyscore result_key, :'-inf', :'-inf'
        # BUG: sorted sets with -inf scores will be treated incorrectly when diffing
      end
    end
    class SortOp < Op
      COMMAND = :zinterstore
      SYMBOL = :sortby
      def json_redis_dump
        operands.map do |op|
          {key: op.index.key, multiplier: op.value}
        end
      end
      def push(index, reverse=nil)
        super(index, reverse ? -1 : 1)
      end
      def target_key_weight; 0; end
      def operand_key_weight(op); op.value; end
    end
    
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
