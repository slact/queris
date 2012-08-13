# encoding: utf-8
require 'json'

module Queris
  class Query
    
    attr_accessor :redis_prefix, :ttl, :created_at, :ops, :sort_ops, :model, :params, :used_index
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
      @explanation = []
      @redis_prefix = (arg[:prefix] || arg[:redis_prefix] || model.redis_prefix) + self.class.name + ":"
      @redis=arg[:redis] || Queris.redis(:query, :slave, :master)
      @profile = model.query_profiler.new(nil, :redis => @redis || model.redis)
      @subqueries = []
      @ttl= arg[:ttl] || 600 #10 minutes default expire
      @created_at = Time.now.utc
      if expire = (arg[:expire_at] || arg[:expire] || arg[:expire_after])
        raise "Can't create query with expire_at option and check_staleness options at once" if arg[:check_staleness]
        raise "Can't create query with expire_at option with track_stats disabled" if arg[:track_stats]==false
        arg[:track_stats]=true
        arg[:check_staleness] = Proc.new do |query|
          query.time_cached < (expire || Time.at(0))
        end
      end
      @track_stats = arg[:track_stats]
      @check_staleness = arg[:check_staleness]
      if block_given?
        instance_eval &block
      end
      self
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
      Time.at (@redis.hget(stats_key, 'time_cached').to_f || 0)
    end
    
    def set_time_cached(val)
      @redis.hset stats_key, 'time_cached', val.to_f
    end
    
    def is_stale?
      if @check_staleness.kind_of? Proc
        stale = @check_staleness.call self
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
    def intersect(index, val=nil)
      prepare_op IntersectOp, index, val
    end
    def diff(index, val=nil)
      prepare_op DiffOp, index, val
    end

    def prepare_op(op_class, index, val)
      index = @model.redis_index index

      #set range and enumerable hack
      if op_class != UnionOp && ((Range === val && !index.handle_range?) || (Enumerable === val &&  !(Range === val)))
        #wrap those values in a union subquery
        sub_union = subquery
        val.each { |v| sub_union.union index, v }
        index, val = sub_union, nil
      end
      
      use_index index  #accept string index names and indices and queries
      set_param_from_index index, val
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

    def sort(index, reverse = nil)
      # accept a minus sign in front of index name to mean reverse
      @results_key = nil
      if Query === index
        raise "sort can be extracted only from query using the same model..." unless index.model == model
        sort_query = index
        if sort_query.sort_ops.empty?
          index = nil
        else #copy sort from another query
          sort_query.sort_ops.each do |op|
            op.each {|operand| use_index operand.index}
          end
          self.sort_ops = sort_query.sort_ops.dup
          # sort_query.sort false #unsort sorted query - legacy behavior, probably a bad idea
        end
        return self
      end
      if index.respond_to?('[]') && index[0] == '-'
        reverse, index = true, index[1..-1]
      end
      if index
        index = use_index index #accept string index names and indices and queries
        real_index = ForeignIndex === index ? index.real_index : index
        raise Exception, "Must have a RangeIndex for sorting, found " unless RangeIndex === real_index
        self.sort_ops.clear << SortOp.new.push(index, reverse)
      else
        self.sort_ops.clear
      end
      self
    end
    
    def sorting_by? index
      if (index=@model.redis_index(*arg))
        sort_ops.each do |op|
          op.operands.each { |o| return true if o.index == index }
        end
      end
      nil
    end
    
    def sorting_by
      sorting = sort_ops.map do |op|
        op.operands.map{|o| "#{(o.value < 0) ? '-' : ''}#{o.index.name}" }.join('*')
      end.join('*')
      sorting.empty? ? nil : sorting
    end

    def resort #apply a sort to set of existing results
      @resort=true
      self
    end

    def profiler
      @profile
    end

    def using_index_as_results_key?
      if ops.length == 1 && sort_ops.empty? && ops.first.operands.length == 1
        first_index = ops.first.operands.first.index
        unless first_index.respond_to?(:before_query_op) || first_index.respond_to?(:after_query_op)
          true
        end
      end
      nil
    end

    def query(force=nil, opt={})
      @profile.id=self
      force||=is_stale?
      if using_index_as_results_key?
        puts "QUERY #{@model.name} #{explain} shorted to #{results_key}"
        #do nothing, we're using a results key directly
        @profile.record :cache_hit, 1
        set_time_cached Time.now if track_stats?
      elsif force || (results_key_type = @redis.type(results_key))[-3..-1] != 'set'
        #Redis slaves can't expire keys by themselves (for the sake of data consistency). So we have to store some dummy value at results_keys in master with an expire.
        #this is gnarly. Hopefully future redis versions will give slaves optional EXPIRE behavior.
        @profile.start :time
        if results_key_type == 'string'
          #clear dummy key
          @redis.del results_key
        end
        
        @subqueries.each do |q|
          q.query force unless opt[:use_cached_subqueries]
        end
        #puts "QUERY #{@model.name} #{explain} #{force ? "forced" : ''} full query"
        @profile.start :own_time
        @redis.multi do |pipelined_redis|
        pipelined_redis = @redis
          first_op = ops.first
          ops.each { |op| op.run pipelined_redis, results_key, first_op == op }
          sort_ops.each { |op| op.run pipelined_redis, results_key }
          #puts "QUERY TTL: @ttl"
          pipelined_redis.expire results_key, @ttl
        end
        @profile.finish :own_time
        if (master = Queris.redis :master) != @redis && !master.nil?  #we're on a slave
          if results_key_type == 'none'
            master.setnx results_key, '' #setnx because someone else might've created it while the app was twiddling its thumbs. Setting it again would erase some slave's result set
            master.expire results_key, @ttl
          end
        end
        set_time_cached Time.now if track_stats?
        @profile.finish :time
        @profile.save
        #puts "updating query profile for #{structure}"
      end
      if @resort #just sort
        #TODO: profile resorts
        #puts "QUERY #{explain} resort"
        @redis.multi do |predis|
           sort_ops.each { |op| op.run predis, results_key }
        end
      end
      #puts "QUERY #{explain} ttl #{@redis.ttl results_key} (should be #{@ttl})"
      self
    end
    alias :run :query

    def uses_index?(*index)
      index.each do |ind|
        index_name = Queris::Index === ind ? ind.name : ind.to_sym
        return true if @used_index[index_name]
      end
      false
    end
    
    # recursively and conditionally flush query and subqueries
    # arg parameters: flush query if:
    #  :ttl - query.ttl <= ttl
    #  :index (symbol or index or an array of them) - query uses th(is|ese) ind(ex|ices)
    # or flush conditionally according to passed block: flush {|query| true }
    # when no parameters or block present, flush only this query and no subqueries
    def flush(arg={})
      return if using_index_as_results_key?
      flushed = 0
      if block_given? #efficiency hackety hack - anonymous blocs are heaps faster than bound ones
        subqueries.each { |sub| flushed += sub.flush arg, Proc.new }
      elsif arg.count>0
        subqueries.each { |sub| flushed += sub.flush arg }
      end
      if flushed > 0 || arg.count==0 || ttl <= (arg[:ttl] || 0) || (uses_index? *arg[:index]) || block_given? && (yield sub)
        #this only works because of the slave EXPIRE hack requiring dummy query results_keys on master.
        #otherwise, we'd have to create the key first (in a MULTI, of course)
        flushed += (Queris.redis(:master) || @redis).del results_key
      end
      flushed
    end
    alias :clear :flush
    
    def results(*arg, &block)
      query
      @profile.start :results_time
      if arg.last == :reverse
        reverse = true
        arg.shift
      end
      key = results_key
      case @redis.type(key)
      when 'set'
        res = @redis.smembers key
        raise "Cannot get result range from shortcut index result set (not sorted); must retrieve all results. This is a temporary queris limitation." unless arg.empty?
      when 'zset'
        if arg.first && arg.first.kind_of?(Range)
          first, last = arg.first.begin, arg.first.end - (arg.first.exclude_end? ? 1 : 0)
        else
          first, last = arg.first.to_i, (arg[1] || -1).to_i
        end
        res = reverse ? @redis.zrange(key, first, last) : @redis.zrevrange(key, first, last)
      else
        res = []
      end
      if block_given?
        res.map!(&block)
      end
      
      @profile.finish :results_time
      #puts "updating results profile for #{structure}"
      @profile.save
      res
    end
    alias :raw_results :results
    
    def contains?(id)
      query
      case @redis.type(results_key)
      when 'set'
        @redis.sismember(results_key, id)
      when 'zset'
        !@redis.zrank(results_key, id).nil?
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
    
    def results_key
      if @results_key.nil?
        if using_index_as_results_key? && (reused_set_key = ops.first.operands.first.key) && 
          @results_key = reused_set_key
        else
          @results_key ||= "#{@redis_prefix}results:" << digest(explain :subqueries => false) << ":subqueries:#{(@subqueries.length > 0 ? @subqueries.map{|q| q.id}.sort.join('&') : 'none')}" << ":sortby:#{sorting_by || 'nothing'}"
        end
      end
      @results_key
    end
    alias :key :results_key
    
    def id
      digest results_key
    end
    
    def length
      query
      key = results_key
      case @redis.type(key)
      when 'set'
        @redis.scard key
      when 'zset'
        @redis.zcard key
      else #not a set. 
        0
      end
    end
    alias :size :length
    alias :count :length

    
    def subquery arg={}
      @results_key = nil
      if arg.kind_of? Query
        subq = arg
      else
        subq = self.class.new((arg[:model] or model), arg.merge(:redis_prefix => redis_prefix, :ttl => @ttl))
      end
      subq.use_redis @redis
      @subqueries << subq
      @subqueries.last
    end
    def subquery_id(subquery)
      @subqueries.index subquery
    end
    
    def subqueries
      @subqueries
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
    
    def structure
      explain :structure => true
    end
    
    def info(indent="", output = true)
      info =  "#{indent}key: #{results_key}\r\n"
      info << "#{indent}id: #{id}, ttl: #{@ttl}, sort: #{sorting_by || "none"}\r\n"
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
      instance_values.merge "redis" => false
    end
    
    def marshal_load(data)
      if data.kind_of? String
        arg = JSON.load(data)
      elsif data.kind_of? Enumerable
        arg = data
      else
        arg = [] #SILENTLY FAIL RELOADING QUERY. THIS IS A *DANGER*OUS DESIGN DECISION MADE FOR THE SAKE OF CONVENIENCE.
      end
      arg.each do |n,v|
        instance_variable_set "@#{n}", v
      end
      @redis ||= Queris.redis :query, :slave, :master
    end

    private
    class Op #query operation
      class Operand #boilerplate
        attr_accessor :index, :value
        def initialize(op_index, val)
          @index = op_index
          @value = val
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
        puts "before send #{self.class.name}"
        operands.each { |op| op.index.before_query_op(redis, target, op.value, op) if op.index.respond_to? :before_query_op }
        puts "SEND #{self.class::COMMAND},  #{target}, #{keys(target)}"
        redis.send self.class::COMMAND, target, keys(target, first), :weights => weights(first)
        puts "after send #{self.class.name}"
        operands.each { |op| op.index.after_query_op(redis, target, op.value, op) if op.index.respond_to? :after_query_op }
      end
    end
    
      
    class UnionOp < Op
      COMMAND = :zunionstore
      SYMBOL = :'∪'
    end
    class IntersectOp < Op
      COMMAND = :zinterstore
      SYMBOL = :'∩'
    end
    class DiffOp < Op
      COMMAND = :zunionstore
      SYMBOL = :'∖'
      def target_key_weight; 0; end
      def operand_key_weight(op=nil); :'-inf'; end
      def run(redis, result_key, first=nil)
        super redis, result_key, first=nil
        redis.zremrangebyscore result_key, :'-inf', :'-inf'
        # BUG: sorted sets with -inf scores will be treated incorrectly when diffing
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
        @used_index[res.name.to_sym]=true if res.respond_to? :name
        subquery res if Query === res
        res
      end
    end
    
    def set_param_from_index(index, val)
      index = use_index index
      @params[index.name]=val if index.respond_to? :name
      val
    end
    
    def digest(value)
      #value
      Digest::SHA1.hexdigest value.to_s
    end
  end
end
