# encoding: utf-8
module Queris
  class Query
    attr_accessor :redis_prefix, :ttl, :created_at, :sort_queue, :sort_index_name
    def initialize(arg)
      @queue, @sort_queue = [], []
      @explanation = []
      @redis_prefix = (arg[:prefix] || arg[:redis_prefix]) + self.class.name + ":"
      @redis=arg[:redis] || Queris.redis
      @subquery = []
      @ttl ||= arg[:ttl] || 3.minutes
      @created_at = Time.now.utc
      self
    end
    
    def union(index, val)
      @results_key = nil
      push_commands index.build_query_part(:zunionstore, self, val, 1)
      push_explanation :union, index, val.to_s
    end
    
    def intersect(index, val)
      @results_key = nil
      push_commands index.build_query_part(:zinterstore, self, val, 1)
      push_explanation :intersect, index, val.to_s
    end
    
    def diff(index, val)
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
      @results_key = nil
      if index.nil?
        @sort_queue = []
        @sort_index_name = nil
      else
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
    
    def query(force=nil)
      #puts "QUERYING #{results_key}"
      if force || !@redis.exists(results_key)
        @subquery.each { |q| q.query force }
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
      end
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
    
    def results(*arg, &block)
      query
      if arg.last == :reverse
        reverse = true
        arg.shift
      end
      if arg.first && arg.first.kind_of?(Range)
        first, last = arg.first.begin, arg.first.end - (arg.first.exclude_end? ? 1 : 0)
      else
        first, last = arg.first.to_i, (arg.second || -1).to_i
      end
      res = reverse ? @redis.zrange(results_key, first, last) : @redis.zrevrange(results_key, first, last)
      if block_given?
        res.map!(&block)
      end
      res
    end
    
    def results_key
      @results_key ||= "#{@redis_prefix}results:" << digest(explain true) << ":subqueries:#{(@subquery.length > 0 ? @subquery.map{|q| q.id}.sort.join('&') : 'none')}" << ":sortby:#{@sort_index_name || 'nothing'}"
    end
    
    def digest(value)
      #value
      Digest::SHA1.hexdigest value.to_s
    end
    
    def id
      digest results_key
    end
    
    def length
      query
      @redis.zcard results_key
    end
    alias :size :length
    alias :count :length
    
    def push_command(*args)
      if args.first.respond_to? :to_sym
        cmd, arg =  args.first, args.second
      else
        cmd =  args.first[:command]
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
    
    def build_query_part(command, query, val=nil, multiplier = 1)
      query.subquery(self) unless query.subquery_id(self)
      [{ :command => command, :subquery => true, :subquery_id => query.subquery_id(self), :key => 'NOT_THE_REAL_KEY_AT_ALL', :weight => multiplier }]
    end
    
    def subquery arg={}
      @results_key = nil
      if arg.kind_of? Query
        subq = arg
      else
        subq = self.class.new(arg.merge :redis_prefix => redis_prefix, :ttl => @ttl)
      end
      @subquery << subq
      @subquery.last
    end
    def subquery_id(subquery)
      @subquery.index subquery
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
      @redis ||= redis
    end
  end
end