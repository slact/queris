# encoding: utf-8
module Queris
  class Query
    attr_accessor :redis_prefix, :ttl, :created_at, :sort_queue, :sort_index_name, :model, :params
    def initialize(model, arg=nil)
      if model.kind_of?(Hash) and arg.nil?
        arg, model = model, model[:model]
      elsif arg.nil?
        arg= {}
      end
      raise "include Queris in your model (#{model.inspect})." unless model.include? Queris
      @model = model
      @params = {}
      @queue, @sort_queue = [], []
      @explanation = []
      @redis_prefix = (arg[:prefix] || arg[:redis_prefix] || model.redis_prefix) + self.class.name + ":"
      @redis=arg[:redis] || Queris.query_redis
      @subquery = []
      @ttl ||= 600 #10 minutes default expire
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
      self
    end
    
    def track_stats?
      @track_stats
    end
    
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
    
    def union(index, val=nil)
      index = check_index index  #accept string index names and indices and queries
      set_param_from_index index, val
      @results_key = nil
      push_commands index.build_query_part(:zunionstore, self, val, 1)
      push_explanation :union, index, val.to_s
    end
    
    def intersect(index, val=nil)
      index = check_index index  #accept string index names and indices and queries
      set_param_from_index index, val
      @results_key = nil
      push_commands index.build_query_part(:zinterstore, self, val, 1)
      push_explanation :intersect, index, val.to_s
    end
    
    def diff(index, val=nil)
      index = check_index(index) #accept string index names and indices and queries
      set_param_from_index index, val
      @results_key = nil
      if val.kind_of?(Range) && index.kind_of?(RangeIndex) #this doubtfully belongs here. But our Sorted Set diff is a bit of a hack anyway, so...
        sub = subquery.union(index, val)
        push_commands sub.build_query_part(:zunionstore, self, val, "-inf")
      else
        push_commands index.build_query_part(:zunionstore, self, val, "-inf")
      end
      push_command :zremrangebyscore , :arg =>['-inf', '-inf']
      push_explanation :diff, index, val.to_s
    end
    
    def sort(index, reverse = nil)
      # accept a minus sign in front of index name to mean reverse
      if index.kind_of?(Query)
        raise "sort can be extracted only from query using the same model..." unless index.model == model
        sort_query = index
        if sort_query.sorting_by.nil?
          index = nil
        else
          index = sort_query.sort_index_name
          sort_query.sort nil #unsort sorted query
        end
      end
      if index.respond_to?('[]') && index[0] == '-'
        reverse, index = true, index[1..-1]
      end
      if index.nil?
        @sort_queue = []
        @sort_index_name = nil
      else
        index = check_index index #accept string index names and indices and queries
        if not index.kind_of? ForeignIndex
          raise Exception, "Must have a RangeIndex for sorting" unless index.kind_of? RangeIndex
        end
        @results_key = nil
        @sort_queue = index.build_query_part(:zinterstore, self, nil, reverse ? -1 : 1)
        @sort_index_name = "#{reverse ? '-' : ''}#{index.name}".to_sym
      end
      self
    end
    
    def sorting_by? what
      @sort_index_name == what.to_sym
    end
    def sorting_by
      @sort_index_name
    end
    
    def resort #apply a sort to set of existing results
      @resort=true
      self
    end
    
    def query(force=nil, opt={})
      #puts "QUERYING #{results_key}"
      force||= is_stale?
      if !@queue.empty? && !@queue.first[:key].empty? && results_key == @queue.first[:key].first
        #do nohing, we're using a results key directly
        set_time_cached Time.now if track_stats?
      elsif force || !@redis.exists(results_key)
        @subquery.each do |q|
          q.query force unless opt[:use_cached_subqueries]
        end
        temp_set = "#{@redis_prefix}Query:temp_sorted_set:#{digest results_key}"
        @redis.multi do
          [@queue, @sort_queue].each do |queue|
            first = queue.first
            queue.each do |cmd|
              if cmd[:subquery] 
                if !cmd[:subquery_id] || !(subquery = @subquery[cmd[:subquery_id]])
                  raise "Unable to process query #{id}: expected subquery #{cmd[:subquery_id] || "<unknown>"} missing."
                end
                raise "Can't transform redis command for subquery: no idea where the key should be placed..." unless cmd[:key]
                raise "Invalid redis command containing subquery..." unless cmd[:key].count == 1
                cmd[:key] = [ subquery.results_key ]
              end
              send_command cmd, temp_set, (queue==@queue && first==cmd)
            end
          end
          @redis.rename temp_set, results_key #don't care if there's no temp_set, we're in a multi.
          @redis.expire results_key, @ttl
        end
        set_time_cached Time.now if track_stats?
      end
      if @resort #just sort
        #puts "QUERY #{explain} resort"
        @redis.multi do
          @sort_queue.each { |cmd| send_command cmd, results_key }
        end
      end
      self
    end

    def results(*arg, &block)
      query
      if arg.last == :reverse
        reverse = true
        arg.shift
      end
      key = results_key
      if @redis.type(key) == 'set'
        res = @redis.smembers key
        raise "Cannot get result range from shortcut index result set (not sorted); must retrieve all results. This is a temporary queris limitation." unless arg.empty?
      else
        if arg.first && arg.first.kind_of?(Range)
          first, last = arg.first.begin, arg.first.end - (arg.first.exclude_end? ? 1 : 0)
        else
          first, last = arg.first.to_i, (arg.second || -1).to_i
        end
        res = reverse ? @redis.zrange(key, first, last) : @redis.zrevrange(key, first, last)
      end
      if block_given?
        res.map!(&block)
      end
      res
    end
    alias :raw_results :results
    
    def contains?(id)
      query
      if @redis.type(results_key) == 'set'
        @redis.sismember(results_key, id)
      else
        !@redis.zrank(results_key, id).nil?
      end
    end
    
    def first_result
      res = results(0...1)
      if res.length > 0 
        res.first
      else
        nil
      end
    end
    
    def results_key
      if !@queue.empty? && @queue.length == 1 && @sort_queue.empty? && @queue.first[:key].length == 1 && [:sunionstore, :sinterstore, :zunionstore, :zinterstore].member?(@queue.first[:command]) && (reused_set_key = @queue.first[:key].first) && @redis.type(reused_set_key)=='set'
        @queue.first[:key].first
      else
        @results_key ||= "#{@redis_prefix}results:" << digest(explain true) << ":subqueries:#{(@subquery.length > 0 ? @subquery.map{|q| q.id}.sort.join('&') : 'none')}" << ":sortby:#{@sort_index_name || 'nothing'}"
      end
    end
    
    def id
      digest results_key
    end
    
    def length
      query
      key = results_key
      if @redis.type(key) == 'set'
        @redis.scard key
      else #not a set. assume it's a sorted set
        @redis.zcard key
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
      @subquery << subq
      @subquery.last
    end
    def subquery_id(subquery)
      @subquery.index subquery
    end
    
    def explain(omit_subqueries=false)
      explaining = @explanation.map do |part| #subqueries!
        if !omit_subqueries && (match = part.match(/(?<op>.*){subquery (?<id>\d+)}$/))
          "#{match[:op]}#{@subquery[match[:id].to_i].explain}"
        else
          part
        end
      end
      if explaining.empty?
        "(∅)"
      else 
        "(#{explaining.join ' '})"
      end
    end
    
    def info(indent="")
      info =  "#{indent}key: #{results_key}\r\n"
      info << "#{indent}id: #{id}, ttl: #{@ttl}, sort: #{sorting_by || "none"}\r\n"
      info << "#{indent}#{explain}\r\n"
      if !@subquery.empty?
        info << "#{indent}subqueries:\r\n"
        @subquery.each do |sub|
          info << sub.info(indent + "  ")
        end
      end
      info
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
      @redis ||= Queris.query_redis
    end

    def build_query_part(command, query, val=nil, multiplier = 1)
      query.subquery(self) unless query.subquery_id(self)
      [{ :command => command, :subquery => true, :subquery_id => query.subquery_id(self), :key => 'NOT_THE_REAL_KEY_AT_ALL', :weight => multiplier }]
    end

    private
    def check_index *arg
      if (res=@model.redis_index(*arg)).nil?
        raise ArgumentError, "Invalid Queris index (#{arg.inspect}) passed to query. May be a string (index name), an index, or a query."
      else
        res
      end
    end
    
    def set_param_from_index(index, val)
      index = check_index index
      @params[index.name]=val if index.respond_to? :name
      val
    end
    
    def push_explanation(operation, index, value)
      #set operation
      if !@explanation.empty?
        case operation
        when :diff
          op= "∖ "
        when :intersect
          op= "∩ "
        when :union
          op= "∪ "
        end
      elsif operation == :diff
        op = "∅ ∖ "
      else
        op = ""
      end

      #index and value
      if index.kind_of? Query #we take this roundabout route because subqueries can be altered
        s = "{subquery #{subquery_id index}}"
      else
        s = "#{index.name}#{!value.to_s.empty? ? '<' + value.to_s + '>' : nil}"
      end
      
      @explanation << "#{op}#{s}"
      self
    end
  
    def push_command(*args)
      if args.first.respond_to? :to_sym
        cmd, arg =  args.first, args.second
      else
        cmd = args.first[:command]
        arg = args.first
      end
      raise "command must be symbol-like" unless cmd.respond_to? :to_sym
      cmd = cmd.to_sym
      if (@queue.length == 0 || @queue.last[:command]!=cmd) || @queue.last[:subquery] || arg[:subquery] || !([:zinterstore, :zunionstore].member? cmd)
        @queue.push :command => cmd, :key =>[], :weight => []
      end
      last = @queue.last
      
      unless arg[:key].nil?
        last[:key] << arg[:key]
        last[:weight] << arg[:weight] || 0
      end
      last[:arg] = arg[:arg]
      [:subquery, :subquery_id].each do |param|
        if last[param].kind_of? Enumerable
          if arg[param].kind_of? Enumerable
            last[param] += arg[param]
          else
            last[param] << val || 0
          end
        else
          last[param] = arg[param]
        end
      end
      self
    end
    
    def push_commands (arr)
      arr.each {|x| push_command x}
      self
    end

    def send_command(cmd, temp_set_key, is_first=false)
      if [:zinterstore, :zunionstore].member? cmd[:command]
        if is_first
          @redis.send cmd[:command], temp_set_key, cmd[:key], :weights => cmd[:weight]
        else
          @redis.send cmd[:command], temp_set_key, (cmd[:key].kind_of?(Array) ? cmd[:key] : [cmd[:key]]) + [temp_set_key], :weights => (cmd[:weight].kind_of?(Array) ? cmd[:weight] : [cmd[:weight]]) + [0]
        end
      else
        @redis.send cmd[:command], temp_set_key, *cmd[:arg]
      end
    end
    
    def digest(value)
      #value
      Digest::SHA1.hexdigest value.to_s
    end
  end
end
