# encoding: utf-8
require 'json'
require 'securerandom'
require "queris/query/operations"
require "queris/query/trace"
require "queris/query/timer"
require "queris/query/page"

module Queris
  class Query
    
    class Error < StandardError
    end
    
    MINIMUM_QUERY_TTL = 30 #seconds. Don't mess with this number unless you fully understand it, setting it too small may lead to subquery race conditions
    attr_accessor :redis_prefix, :created_at, :ops, :sort_ops, :model, :params
    attr_reader :subqueries, :ttl, :temp_key_ttl
    def initialize(model, arg=nil, &block)
      if model.kind_of?(Hash) and arg.nil?
        arg, model = model, model[:model]
      elsif arg.nil?
        arg= {}
      end
      raise ArgumentError, "Can't create query without a model" unless model
      raise Error, "include Queris in your model (#{model.inspect})." unless model.include? Queris
      @model = model
      @params = {}
      @ops = []
      @sort_ops = []
      @used_index = {}
      @redis_prefix = arg[:complete_prefix] || ((arg[:prefix] || arg[:redis_prefix] || model.redis_prefix) + self.class.name.split('::').last + ":")
      @redis=arg[:redis]
      @profile = arg[:profiler] || model.query_profiler.new(nil, :redis => @redis || model.redis)
      @subqueries = []
      self.ttl=arg[:ttl] || 600 #10 minutes default time-to-live
      @temp_key_ttl = arg[:temp_key_ttl] || 300
      @trace=nil
      @pageable = arg[:pageable] || arg[:paged]
      live! if arg[:live]
      realtime! if arg[:realtime]
      @created_at = Time.now.utc
      if @expire_after = (arg[:expire_at] || arg[:expire] || arg[:expire_after])
        raise ArgumentError, "Can't create query with expire_at option and check_staleness options at once" if arg[:check_staleness]
        raise ArgumentError, "Can't create query with expire_at option with track_stats disabled" if arg[:track_stats]==false
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
    
    def use_redis(redis_instance) #seems obsolete with the new run pipeline
      @redis = redis_instance
      subqueries.each {|sub| sub.use_redis redis_instance}
      self
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
      raise Error, "Recursive subquerying doesn't do anything useful." if index == self
      validate_ttl(index) if Index === index && live? && index.live?
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
      raise ClientError, "Unexpected operand-less query operation" unless (last_operand = op.operands.last)
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
      use_page make_page
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
    def live!; @live=true; @realtime=false; validate_ttl; self; end
    #realtime queries are updated automatically, on the spot
    def realtime!
      live!
      @realtime = true
    end
    def realtime?
      live? && @realtime
    end
    def ttl=(time_to_live=nil)
      validate_ttl(nil, time_to_live)
      @ttl=time_to_live
    end
    def validate_ttl(index=nil, time_to_live=nil)
      time_to_live ||= ttl
      tooshort = (index ? [ index ] : all_live_indices).select { |i| time_to_live > i.delta_ttl if time_to_live }
      if tooshort.length > 0
        raise Error, "Query time-to-live is too long to use live ind#{tooshort.length == 1 ? 'ex' : 'ices'} #{tooshort.map {|i| i.name}.join(', ')}. Shorten query ttl or extend indices' delta_ttl."
      end
      ttl
    end
    private :validate_ttl

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
      volatile = [ results_key(:exists), results_key ]
      volatile |=[ results_key(:live), results_key(:marshaled) ] if live?
      volatile |= @page.volatile_query_keys(self) if paged?
      volatile
    end
    
    def add_temp_key(k)
      @temp_keys ||= {}
      @temp_keys[k]=true
    end
    
    def temp_keys
      @temp_keys ||= {}
      @temp_keys.keys
    end
    #all keys related to a query
    def all_query_keys
      all = volatile_query_keys
      all |= temp_keys
      all
    end

    def optimize
      smallkey, smallest = nil, Float::INFINITY
      puts "optimizing query #{self}"
      ops.reverse_each do |op|
        #puts "optimizing op #{op}"
        op.notready!
        smallkey, smallest = op.optimize(smallkey, smallest, @page)
      end
    end
    private :optimize
    def no_optimize!
      @no_optimize=true
      subqueries.each &:no_optimize!
    end
    def run_static_query(r, force=nil)
      #puts "running static query #{self.id}, force: #{force || "no"}"
    
      temp_results_key = results_key "running:#{SecureRandom.hex}"
      trace_callback = @trace ? @trace.method(:op) : nil

      #optimize that shit
      optimize unless @no_optimize
      
      first_op = ops.first
      [ops, sort_ops].each do |ops|
        ops.each do |op|
          op.run r, temp_results_key, first_op == op, trace_callback
        end
      end
      
      if paged?
        r.zunionstore results_key, [results_key, temp_results_key], :aggregate => 'max'
        r.del temp_results_key
      else
        Queris.run_script :move_key, r, [temp_results_key, results_key]
        r.setex results_key(:exists), ttl, 1
      end
    end
    private :run_static_query
    
    def reusable_temp_keys
      tkeys = []
      each_operand do |op|
        tkeys |= op.temp_keys
      end
      tkeys << @page.key if paged?
      tkeys
    end
    private :reusable_temp_keys
    def reusable_temp_keys?
      return true if paged?
      ops.each {|op| return true if op.temp_keys?}
      nil
    end
    private :reusable_temp_keys?
    
    def all_subqueries
      ret = subqueries.dup
      subqueries.each { |sub| ret.concat sub.all_subqueries }
      ret
    end
    
    def each_subquery(recursive=true) #dependency-ordered
      subqueries.each do |s|
        s.each_subquery do |ss|
          yield ss
        end
        yield s
      end
    end
    
    def trace!(opt={})
      if opt==false
        @must_trace = false
      elsif opt
        @must_trace = opt
      end
      self
    end
    def trace?
      @must_trace || @trace
    end
    def trace(opt={})
      indent = opt[:indent] || 0
      buf = "#{"  " * indent}#{indent == 0 ? 'Query' : 'Subquery'} #{self}:\r\n"
      buf << "#{"  " * indent}key: #{key}\r\n"
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
      return 0 if uses_index_as_results_key?
      flushed = 0
      if block_given? #efficiency hackety hack - anonymous blocs are heaps faster than bound ones
        subqueries.each { |sub| flushed += sub.flush arg, &Proc.new }
      elsif arg.count>0
        subqueries.each { |sub| flushed += sub.flush arg }
      end
      if flushed > 0 || arg.count==0 || ttl <= (arg[:ttl] || 0) || (uses_index?(*arg[:index])) || block_given? && (yield self)
        #this only works because of the slave EXPIRE hack requiring dummy query results_keys on master.
        #otherwise, we'd have to create the key first (in a MULTI, of course)
        n_deleted = 0
        (redis_master || redis).multi do |r|
          all = all_query_keys
          all.each do |k|
            r.setnx k, 'baleted'
          end
          n_deleted = r.del all 
        end
        @known_to_exist = nil
        flushed += n_deleted.value
      end
      flushed
    end
    alias :clear :flush
    
    
    def range(range)
      raise ArgumentError, "Range, please." unless Range === range
      pg=make_page
      pg.range=range
      use_page pg
      self
    end
    
    #flexible query results retriever
    #results(x..y) from x to y
    #results(x, y) same
    #results(x) first x results
    #results(x..y, :reverse) range in reverse
    #results(x..y, :score =>a..b) results from x to y with scores in given score range
    #results(x..y, :with_scores) return results with result.query_score attr set to the score
    def results(*arg)
      opt= Hash === arg.last ? arg.pop : {}
      opt[:reverse]=true if arg.member?(:reverse)
      opt[:with_scores]=true if arg.member?(:with_scores)
      opt[:range]=arg.shift if Range === arg.first
      opt[:range]=(arg.shift .. arg.shift) if Numeric === arg[0] && arg[0].class == arg[1].class
      opt[:range]=(0..arg.shift) if Numeric === arg[0]
      opt[:raw] = true if arg.member? :raw
      if opt[:range] && !sort_ops.empty? && pageable?
        range opt[:range]
        run :no_update => !realtime?
      else
        use_page nil
        run :no_update => !realtime?
      end
      @timer.start("results") if @timer
      key = results_key
      case (keytype=redis.type(key))
      when 'set'
        raise Error, "Can't range by score on a regular results set" if opt[:score]
        raise NotImplemented, "Cannot get result range from shortcut index result set (not sorted); must retrieve all results. This is a temporary queris limitation." if opt[:range]
        cmd, first, last, rangeopt = :smembers, nil, nil, {}
      when 'zset'
        rangeopt = {}
        rangeopt[:with_scores] = true if opt[:with_scores]
        if (r = opt[:range])
          first, last = r.begin, r.end - (r.exclude_end? ? 1 : 0)
          raise ArgumentError, "Query result range must have numbers, instead there's a #{first.class} and #{last.class}" unless Numeric === first && Numeric === last
          raise ArgumentError, "Query results range must have integer endpoints" unless first.round == first && last.round == last
        end
        if (scrange = opt[:score])
          rangeopt[:limit] = [ first, last - first ] if opt[:range]
          raise NotImplemented, "Can't fetch results with_scores when also limiting them by score. Pick one or the other." if opt[:with_scores]
          raise ArgumentError, "Query.results :score parameter must be a Range" unless Range === scrange
          first = Queris::to_redis_float(scrange.begin * sort_mult)
          last = Queris::to_redis_float(scrange.end * sort_mult)
          last = "(#{last}" if scrange.exclude_end? #)
          first, last = last, first if sort_mult == -1
          cmd = opt[:reverse] ? :zrevrangebyscore : :zrangebyscore
        else
          cmd = opt[:reverse] ? :zrevrange : :zrange
        end
      else
        return []
      end
      @timer.start :results
      if block_given? && opt[:replace_command]
        res = yield cmd, key, first || 0, last || -1, rangeopt
      elsif @from_hash && !opt[:raw]
        
        if rangeopt[:limit]
          limit, offset = *rangeopt[:limit]
        else
          limit, offset = nil, nil
        end
        raw_res, ids, failed_i = redis.evalsha(Queris::script_hash(:results_from_hash), [key], [cmd, first || 0, last || -1, @from_hash, limit, offset, rangeopt[:with_scores] && :true])
        res, to_be_deleted = [], []
        raw_res.each_with_index do |raw_hash, i|
          my_id = ids[i]
          if failed_i.first == i
            failed_i.shift
            obj = model.find_cached my_id, :assume_missing => true
          else
            if (raw_hash.count % 2) > 0
              binding.pry
              1+1.12
            end
            unless (obj = model.restore(raw_hash, my_id))
              #we could stil have received an invalid cache object (too few attributes, for example)
              obj = model.find_cached my_id, :assume_missing => true
            end
          end
          if not obj.nil?
            res << obj
          elsif @delete_missing
            to_be_deleted << my_id
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
      if @timer
        @timer.finish :results
        #puts "Timing for #{self}: \r\n  #{@timer}"
      end
      res
    end
    
    def result_scores(*ids)
      val=[]
      if ids.count == 1 && Array === ids[0]
        ids=ids[0]
      end
      val=redis.multi do |r|
        ids.each do |id|
          redis.zscore(results_key, id)
        end
      end
      val
    end
    
    def make_page
      Query::Page.new(@redis_prefix, sort_ops, model.page_size, ttl)
    end
    private :make_page
    
    def raw_results(*arg)
      arg.push :raw
      results(*arg)
    end

    def use_page(page)
      @results_key=nil
      if !pageable?
        return nil
      end
      raise ArgumentError, "must be a Query::Page" unless page.nil? || Page === page
      if block_given?
        temp=@page
        use_page page
        yield
        use_page temp
      else
        @page=page
        subqueries.each {|s| s.use_page @page}
      end
    end
    def paged?
      @page
    end
    def pageable?
      @pageable
    end
    def pageable!
      @pageable = true
    end
    def unpageable!
      @pageable = false
    end

    def member?(id)
      id = id.id if model === id
      use_page nil
      run :no_update => !realtime?
      case t = redis.type(results_key)
      when 'set'
        redis.sismember(results_key, id)
      when 'zset'
        !redis.zrank(results_key, id).nil?
      when 'none'
        false
      else
        raise ClientError, "unexpected result set type #{t}"
      end
    end
    alias :contains? :member?
    
    
    def result(n=0)
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
          theid = raw_id || (Queris.digest(explain :subqueries => false) << ":subqueries:#{(@subqueries.length > 0 ? @subqueries.map{|q| q.id}.sort.join('&') : 'none')}" << ":sortby:#{sorting_by || 'nothing'}")
          thekey = "#{@redis_prefix}results:#{theid}"
          thekey << ":paged:#{@page.source_id}" if paged?
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
      Queris.digest results_key
    end
    
    def count(opt={})
      use_page nil
      run(:no_update => !realtime?) unless opt [:no_run]
      key = results_key
      case redis.type(key)
      when 'set'
        raise Error, "Query results are not a sorted set (maybe using a set index directly), can't range" if opt[:score]
        redis.scard key
      when 'zset'
        if opt[:score]
          range = opt[:score]
          raise ArgumentError, ":score option must be a Range, but it's a #{range.class} instead" unless Range === range
          first = range.begin * sort_mult
          last = range.exclude_end? ? "(#{range.end.to_f * sort_mult}" : range.end.to_f * sort_mult #)
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

    #current query size
    def key_size(redis_key=nil, r=nil)
      Queris.run_script :multisize, (r || redis), [redis_key || key]
    end

    def subquery arg={}
      if arg.kind_of? Query #adopt a given query as subquery
        raise Error, "Trying to use a subquery from a different model" unless arg.model == model
      else #create new subquery
        arg[:model]=model
      end
      @used_subquery ||= {}
      @results_key = nil
      if arg.kind_of? Query
        subq = arg
      else
        subq = self.class.new((arg[:model] or model), arg.merge(:complete_prefix => redis_prefix, :ttl => @ttl))
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
        #live_keyscores = redis.zrange(results_key(:live), 0, -1, :with_scores => true)
        #info << live_keyscores.map{|i,v| "#{i}:#{v}"}.join(", ")
        info << " (live indices: #{redis.zcard results_key(:live)})"
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
          delete_missing: @delete_missing,
          pageable: pageable?
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
          raise Query::Error, "Reloading query failed. data: #{data.to_s}"
          #arg = [] #SILENTLY FAIL RELOADING QUERY. THIS IS A *DANGER*OUS DESIGN DECISION MADE FOR THE SAKE OF CONVENIENCE.
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
    
    attr_accessor :timer
    def new_run
      @run_id = SecureRandom.hex
      @runstate_keys={}
      @ready = nil
      @itshere = {}
      @temp_keys = {}
      @reserved = nil
    end
    private :new_run
    attr_accessor :run_id
    def runstate_keys
      @runstate_keys ||= {}
      @runstate_keys.keys
    end
    def runstate_key(sub=nil)
      @runstate_keys ||= {}
      k="#{redis_prefix}run:#{run_id}"
      k << ":#{sub}"if sub
      @runstate_keys[k]=true
      k
    end
    
    def run_stage(stage, r, recurse=true)
      #puts "query run stage #{stage} START for #{self}"
      method_name = "query_run_stage_#{stage}".to_sym
      yield(r) if block_given?
      
      #page first
      @page.send method_name, r, self if paged? && @page.respond_to?(method_name)
      
      #then subqueries
      each_subquery do |sub| 
        #assumes this iterator respects subquery dependency ordering (deepest subqueries first)
        sub.run_stage stage, r, false
      end
      #now operations, indices etc.
      [ops, sort_ops].each do |arr|
        arr.each do |operation|
          if operation.respond_to? method_name
            operation.send method_name, r, self
          end
          operation.operands.each do |op|
            if op.respond_to? method_name
              op.send method_name, r, self
            end
            if !op.is_query? && op.index.respond_to?(method_name)
              op.index.send method_name, r, self
            end
          end
        end
      end
      #and finally, the meat
      self.send method_name, r, self if respond_to? method_name
      #puts "query run stage #{stage} END for #{self}"
    end
    
    def run_pipeline(redis, *stages)
      #{stages.join ', '}
      pipesig = "pipeline #{stages.join ','}"
      @timer.start pipesig
      #redis.pipelined do |r|
      r = redis
        stages.each do |stage|
          run_stage(stage, r)
        end
        yield(r, self) if block_given?
      #end
      @timer.finish pipesig
    end
    
    def run_callbacks(event, with_subqueries=true)
      each_subquery { |s| s.model.run_query_callbacks(event, s) } if with_subqueries
      model.run_query_callbacks event, self
      self
    end
    
    def query_run_stage_begin(r,q)
      if uses_index_as_results_key?
        @trace.message "Using index as results key." if @trace_callback
      end
      new_run #how procedural...
    end
    def query_run_stage_inspect(r,q)
      gather_ready_data r
      if live?
        @itshere[:marshaled]=r.exists results_key(:marshaled)
        @itshere[:live]=r.exists results_key(:live)
      end
    end
    def query_run_stage_reserve(r,q)
      return if ready?
      @reserved = true
      if live?
        #marshaled query 
        r.setex results_key(:marshaled), ttl, JSON.dump(json_redis_dump) unless fluxcap @itshere[:marshaled]
        unless fluxcap @itshere[:live]
          now=Time.now.utc.to_f
          all_live_indices.each do |i|
            r.zadd results_key(:live), now, i.live_delta_key
          end
          r.expire results_key(:live), ttl
        end
      end
      @already_exists = r.get results_key(:exists) if paged?
      #make sure volatile keys don't disappear
      min = Query::MINIMUM_QUERY_TTL
      
      Queris.run_script :add_low_ttl, r, [ *volatile_query_keys, runstate_key(:low_ttl) ], [ min, min ]
    end

    def query_run_stage_prepare(r,q)
      return unless @reserved
    end

    def query_run_stage_run(r,q)
      return unless @reserved
      unless ready?
        run_static_query r
      else
        
        if live?
          @live_update_msg = Queris.run_script(:update_query, r, [results_key(:marshaled), results_key(:live)], [Time.now.utc.to_f])
        end
      end
    end

    def query_run_stage_release(r,q)
      return unless @reserved
      r.setnx results_key(:exists), 1
      if paged? && fluxcap(@already_exists)
        Queris.run_script :master_expire, r, volatile_query_keys, [ ttl, nil, true ]
        Queris.run_script :master_expire, r, reusable_temp_keys, [ temp_key_ttl, nil, true ]
      else
        Queris.run_script :master_expire, r, volatile_query_keys, [ ttl , true, true]
        Queris.run_script :master_expire, r, reusable_temp_keys, [ temp_key_ttl, nil, true ]
      end
      
      r.del q.runstate_keys
      @reserved = nil
      
      #make sure volatile keys don't overstay their welcome
      min = Query::MINIMUM_QUERY_TTL
      Queris.run_script :undo_add_low_ttl, r, [ runstate_key(:low_ttl) ], [ min ]
    end
    
    def ready?(r=nil, subs=true)
      return true if uses_index_as_results_key? || fluxcap(@ready)
      r ||= redis
      ready = if paged?
        @page.ready?
      else
        fluxcap @ready
      end
      ready
    end
    def gather_ready_data(r)
      unless paged?
        @ready = Queris.run_script :unpaged_query_ready, r, [ results_key, results_key(:exists), runstate_key(:ready) ]
      end
    end

    
    def run(opt={})
      raise ClientError, "No redis connection found for query #{self} for model #{self.model.name}." if redis.nil?
      @timer = Query::Timer.new
      #parse run options
      force = opt[:force]
      force = nil if Numeric === force && force <= 0
      if trace?
        @trace= Trace.new self, @must_trace
      end
      run_callbacks :before_run
      if uses_index_as_results_key?
        #do nothing, we're using a results key directly from an index which is guaranteed to already exist
        @trace.message "Using index as results key." if @trace
        return count(:no_run => true)
      end

      Queris::RedisStats.querying = true
      #run this sucker
      #the following logic must apply to ALL queries (and subqueries),
      #since all queries run pipeline stages in 'parallel' (on the same redis pipeline)
      @timer.start :time
      run_pipeline redis, :begin do |r, q|
        #run live index callbacks
        all_live_indices.each do |i|
          if i.respond_to? :before_query
            i.before_query r, self
          end
        end
        run_stage :inspect, r
        @page.inspect_query(r, self) if paged?
      end
      if !ready? || force
        run_pipeline redis_master, :reserve
        if paged?
          begin
            @page.seek
            run_pipeline redis, :prepare, :run, :after_run do |r, q|
              @page.inspect_query(r, self)
            end
          end until @page.ready?
        else
          run_pipeline redis, :prepare, :run, :after_run
        end
        run_pipeline redis_master, :release
      else
        if live? && !opt[:no_update]
          @timer.start :live_update
          live_update_msg = Queris.run_script(:update_query, redis, [results_key(:marshaled), results_key(:live)], [Time.now.utc.to_f])
          @trace.message "Live query update: #{live_update_msg}" if @trace
          @timer.finish :live_update
        end
        @trace.message "Query results already exist, no trace available." if @trace
      end
      @timer.finish :time
      Queris::RedisStats.querying = false
    end
    private
    
    def fluxcap(val) #flux capacitor to safely and blindly fold the Future into the present
      begin
        Redis::Future === val ? val.value : val
      rescue Redis::FutureNotReady
        nil
      end
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
  end
end
