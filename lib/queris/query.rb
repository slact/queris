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
      Queris.redis :master
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
      if (index=@model.redis_index(index))
        sort_ops.each do |op|
          op.operands.each { |o| return true if o.index == index }
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
      Queris::QueryStore.clear_flag(self, 'realtime') if realtime?
      @live=false; @realtime=false; self; 
    end
    #live queries have pending updates stored nearby
    def live!; @live=true; @realtime=false; self; end
    #realtime queries are updated automatically, on the spot
    def realtime!
      if realtime? 
        Queris::QueryStore.refresh_flag(self, 'realtime')
      else
        Queris::QueryStore.set_flag(self, 'realtime')
        live!
      end
    end
    def realtime?
      @realtime ||= live? && Queris::QueryStore.get_flag(self, 'realtime')
    end

    #update query results with object(s)
    def update(obj, arg={})
      if uses_index_as_results_key?
        #puts "No need to update #{self}"
        return self 
      end
      master = redis_master || redis
      if realtime? #update query in-place
        if arg[:delete] || !member?(obj)
          redis.zrem results_key, obj.id #BUG-IN-WAITING: HARDCODED id attribute
        else
          score = sort_score obj
          redis.zadd results_key, score || 0, obj.id #BUG-IN-WAITING: HARDCODED id attribute
        end
        realtime!
        master.expire results_key, ttl #query is fresh, so its ttl gets reset
      else
        master.multi do |r|
          if arg[:delete] || !member?(obj)
            delta_key = results_key('delta:diff')
            r.zadd delta_key, '-inf', obj.id #BUG-IN-WAITING: HARDCODED id attribute
          else
            #differential score because Z*STORE commands can aggregate scores only with addition (also MIN & MAX, but we don't care about those. If only there were a LAST or OVERRIDE aggregate function)
            score = sort_score(obj) - sort_score(obj, previous: true)
            r.zincrby results_key('delta:union'), score || 0, obj.id #BUG-IN-WAITING: HARDCODED id attribute
          end
          r.expire delta_key, ttl
          master.expire results_key, ttl
        end
      end
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
    def extend_ttl(r=nil)
      return (redis_master || redis).multi{ |multir| extend_ttl multir } if r.nil?
      r.expire results_key, ttl
      r.setex results_key(:exists), ttl, ""
      self
    end
    
    #check for the existence of a result set. We need to do this in case the result set is empty
    def results_exist?(r=nil)
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
        run_static_query force, opt[:debug], opt[:use_cached_queries]
        Queris::QueryStore.add(self) if live? && !uses_index_as_results_key?
      elsif live?
        run_live_query opt[:debug]
      else
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

    def run_live_query(opt={})
      if realtime?
        #nothing to do but update ttl
        #puts "#{self} is a realtime query, nothing to do..."
        realtime!
        return extend_ttl
      end
      #puts "#{self} live query"
      randstr=SecureRandom.hex
      union_key= results_key 'delta:union'
      temp_union_key = results_key("temp:%s:delta:union" % randstr)
      diff_key= results_key 'delta:diff'
      temp_diff_key = results_key("temp:%s:delta:diff" % randstr)

      union_size, diff_size = redis.multi do |r|
        r.zcard union_key
        r.zcard diff_key
      end
      #puts "union-size: #{union_size}, diff-size: #{diff_size}"
      master = redis_master
      if master && master != redis
        #reserve the delta sets just for us
        #puts "rename on master"
        master.multi do |r|
          r.rename union_key, temp_union_key if union_size > 0
          r.rename diff_key, temp_diff_key if diff_size > 0
        end
      end
      delta_size = union_size + diff_size
      if delta_size > 0
        #puts "update > 0"
        redis.multi
        #just in case the renames from master haven't yet reached this server
        redis.renamenx union_key, temp_union_key if union_size > 0
        redis.renamenx diff_key, temp_diff_key if diff_size > 0

        if delta_size > 100 #mmm, hardcoded optimization thresholds...
          #there's an update available to the live query, and it's large-ish. Do it on the server
          r.zunionstore results_key, results_key, temp_union_key if union_size > 0
          if diff_size > 0
            r.zunionstore results_key, results_key, temp_diff_key, aggregate: :min
            r.zremrangebyscore results_key, '-inf', '-inf'
          end
          redis.exec
        else
          #there's an update, but it's relatively small. do it client-side
          union_zset = redis.zrange temp_union_key, 0, -1, :with_scores => true
          diff_set = redis.zrange temp_diff_key, 0, -1
          redis.exec
          redis.multi do |r|
            union_zset.value.each { |z| r.zincrby results_key, z.last, z.first }
            r.zrem results_key, diff_set.value
          end
        end
        (master || redis).multi do |r|
          r.del temp_union_key, temp_diff_key #we're done with these
          extend_ttl r
        end
        
      end
    end
    private :run_live_query
    
    def run_static_query(force=nil, debug=nil, use_cached_subqueries=nil)
      @profile.start :time
      master = redis_master
      @subqueries.each do |q|
        q.query(:force => force, :debug => debug) unless use_cached_subqueries
      end
      #puts "QUERY #{@model.name} #{explain} #{force ? "forced" : ''} full query"
      @profile.start :own_time
      debug_info = []
      redis.multi do |pipelined_redis|
        first_op = ops.first
        [ops, sort_ops].each do |ops|
          ops.each do |op|
            op.run pipelined_redis, results_key, first_op == op
            debug_info << [op.to_s, pipelined_redis.zcard(results_key)] if debug
          end
        end
        #puts "QUERY TTL: ttl"
        extend_ttl(pipelined_redis) if master.nil? || master == redis
      end
      @profile.finish :own_time
      unless master.nil? || master == redis
        #Redis slaves can't expire keys by themselves (for the sake of data consistency). So we have to store some dummy value at results_keys in master with an expire.
        #this is gnarly. Hopefully future redis versions will give slaves optional EXPIRE behavior.
        master.multi do |m|
          m.setnx results_key, 1
          #setnx because someone else might've created it while the app was twiddling its thumbs. Setting it again would erase some slave's result set
          extend_ttl m
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
      return [] if !@live
      @live_indices || all_indices
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
        end
        Queris::QueryStore.remove(self) if live? && !uses_index_as_results_key?
        flushed += res.first
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
      query
      opt= Hash === arg.last ? arg.pop : {}
      binding.pry
      opt[:reverse]= true if arg.member?(:reverse)
      opt[:with_scores]=true if arg.member?(:with_scores)
      opt[:range]=arg.shift if Range === arg.first
      first, last = arg.shift, arg.shift if Numeric === arg[0] && arg[0].class == arg[1].class
      opt[:range]=(0..arg.shift) if Numeric === arg[0]
      
      @profile.start :results_time
      key = results_key
      case redis.type(key)
      when 'set'
        res = redis.smembers key
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
        res = redis.send(cmd, key, first || 0, last || -1, rangeopt)
      else
        res = []
      end
      if block_given?
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
      query
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
    
    def results_key(suffix = nil)
      if @results_key.nil?
        if (reused_set_key = uses_index_as_results_key?)
          @results_key = reused_set_key
        else
          @results_key ||= "#{@redis_prefix}results:" << digest(explain :subqueries => false) << ":subqueries:#{(@subqueries.length > 0 ? @subqueries.map{|q| q.id}.sort.join('&') : 'none')}" << ":sortby:#{sorting_by || 'nothing'}"
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
      query
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
      return "(∅)" if ops.empty?
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
          op_str.prepend "∅ #{op.symbol} " if DiffOp === op
        else
          op_str.prepend " #{op.symbol} "
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
      each_operand do |index, val|
        unless Query === index
          #binding.pry
          param_name = index.name
          unique_params.delete param_name if params[param_name] == val
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
          live: @live && !@live_indices.nil? ? @live_indices.map{|i| i.name} : @live
        }
      }
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

    def member? (obj)
      ops.reverse_each do |op|
        unless (member = op.member?(obj)).nil?
          return member
        end
      end
      false
    end
    def marshaled
      Marshal.dump self
    end
    private

    def each_operand #walk though all query operands
      ops.each do |operation|
        operation.operands.each do |operand|
          yield operand.index, operand.value, operation
        end
      end
    end

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
      def to_s
        "#{symbol} #{operands.map{|o| Query === o.index ? o.index : "#{o.index.name}<#{o.value}>"}.join(" #{symbol} ")}"
      end
      
      private
      def member?(obj)
        operands.reverse_each do |op|
          index, val = op.index, op.value
          if Query === index 
            member = index.member? obj
          elsif ForeignIndex === index
            next
          else
            obj_val = obj.send index.attribute
            member = case val
            when Range
              val.cover? member unless member.nil?
            when Enumerable
              val.member? member unless member.nil?
            else
              val == obj_val
            end
          end
          member = yield member if block_given?
          return member unless member.nil?
        end
        nil
      end
    end
    class UnionOp < Op
      COMMAND = :zunionstore
      SYMBOL = :'∪'
      def member? obj
        super(obj) { |m| m ? m : nil }
      end
    end
    class IntersectOp < Op
      COMMAND = :zinterstore
      SYMBOL = :'∩'
      def member? obj
        super(obj) { |m| m ? nil : m }
      end
    end
    class DiffOp < Op
      COMMAND = :zunionstore
      SYMBOL = :'∖'
      def target_key_weight; 0; end
      def operand_key_weight(op=nil); :'-inf'; end
      def run(redis, result_key, first=nil)
        super redis, result_key, first
        redis.zremrangebyscore result_key, :'-inf', :'-inf'
        # BUG: sorted sets with -inf scores will be treated incorrectly when diffing
      end
      def member? obj
        super(obj) { |m| m ? !m : nil }
      end
    end
    class SortOp < Op
      COMMAND = :zinterstore
      SYMBOL = :sortby
      def push(index, reverse=nil)
        super(index, reverse ? -1 : 1)
      end
      def target_key_weight; 0; end
      def operand_key_weight(op); 1; end
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
