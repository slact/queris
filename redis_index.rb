require 'rubygems'
require 'digest/sha1'

module RedisIndex
  
  class Index
    attr_accessor :name, :redis, :model, :attribute
    def initialize(arg={})
      arg.each do |opt, val|
        instance_variable_set "@#{opt}".to_sym, val
      end
      @redis ||= $redis
      @name ||= @attribute
      @attribute ||= @name
      @attribute = @attribute.to_sym unless !@attribute
      @name = @name.to_sym
      @key ||= :id #object's key attribute (default is 'id')
      @keyf ||= "%s#{self.class.name.sub /^.*::/, ""}:#{@name}=%s"
      if block_given?
        yield self, arg
      end
      raise ArgumentError, "Index must have a name" unless @name
      raise ArgumentError, "Index must have a model" unless @model
      @model.add_redis_index self
    end
    def val(value)
      @value.nil? ? value : @value.call(value)
    end
    def digest(value)
      #value
      Digest::SHA1.hexdigest value.to_s
    end
    def value_is(obj)
      obj.send @attribute
    end
    def value_was(obj)
      obj.send "#{@attribute}_was"
    end
    def update(obj)
      val_is, val_was = value_is(obj), value_was(obj)
      if(val_is != val_was)
        remove(obj, val_was)
        add(obj)
      end
    end
    def create(obj)
      add(obj)
    end
    def delete(obj)
      remove(obj, value_was(obj))
    end    
  end
    
  class SearchIndex < Index
    def initialize(arg={})
      super arg
      @type ||= "string"
      raise Exception, "Model not passed to index." unless @model
    end
    
    def set_key(value, prefix=nil)
      @keyf %[prefix || @redis_prefix || @model.redis_prefix, digest(val value)]
    end
    def add(obj, val = nil)
      @redis.sadd set_key(val.nil? ? obj.send(@attribute) : val), obj.send(@key)
    end
    def remove(obj, val = nil)
      @redis.srem set_key(val.nil? ? obj.send(@attribute) : val), obj.send(@key)
    end
    
    def build_query_part(command, query, value, obj=nil)
      (value.kind_of?(Enumerable) ?  value : [ value ]).each do |a_value|
        query.push_command command, :key => set_key(a_value), :short_key => set_key(a_value, "")
      end
    end
  end
  
  class ForeignIndex < SearchIndex
    attr_accessor :real_index
    def initialize(arg)
      raise ArgumentError, "Missing required initialization attribute real_index for ForeignIndex." unless arg[:real_index]
      super arg
    end
    def create(*a) end
    alias :delete :create
    alias :update :create
    def set_key(*arg)
      @real_index.set_key *arg
    end
    def method_missing(method)
      @real_index.method
    end
  end
  
  
  
  class PresenceIndex < SearchIndex
    def initialize(arg)
      super arg
      @counter_keyf = "#{@model.redis_prefix}#{self.class.name.sub /^.*::/, ""}:#{@name}:#{@attribute}=%s:counter"
      @attribute = @key
      @threshold ||= 1
    end
    def digest(*arg)
      "present"
    end
    def counter_key(obj, val=nil)
      @counter_keyf % (val || value_is(obj))
    end
    def add(obj, value=nil)
      k = @redis.incr counter_key(obj)
      if k == @threshold
        super obj
      end
    end
    def remove(obj, value=nil)
      ckey = counter_key obj
      @redis.decr ckey
      if @redis.get(ckey).to_i. < @threshold
        @redis.del ckey
        super obj
      end
    end
  end
  
  class RangeIndex < SearchIndex
    def initialize(arg)
      @score ||= Proc.new { |x| x.to_f }
      super arg
    end
    
    def sorted_set_key(val=nil, prefix=nil)
      @keyf %[prefix || @model.redis_prefix, "(...)"]
    end
    
    def add(obj, value=nil)
      my_val = val(value || value_is(obj))
      @redis.zadd sorted_set_key(obj.send @attribute), score(obj, my_val), obj.send(@key)
    end
    
    def score(obj, val=nil)
      value = val || obj.send(:instance_variable_get, "@#{@attribute}")
      @score.call value
    end
    
    def remove(obj, value=nil)
      @redis.zrem sorted_set_key(obj.send @attribute), val(value || value_is(obj))
    end
    
    def build_query_part(command, query, value, multiplier=1)
      case value
      when Range
        range command, query, val(value.begin), val(value.end), false, value.exclude_end?, nil, multiplier
      when Enumerable
        raise ArgumentError, "RangeIndex doesn't accept non-Range Enumerables"
      else
        range command, query, '-inf', val(value) || 'inf', false, true, nil, multiplier
        range command, query, val(value), 'inf', true, false, true, multiplier if value
      end
      self
    end
    
    private
    def range(command, query, min, max, exclude_min, exclude_max, range_only=nil, multiplier=1)
      key = sorted_set_key
      min, max = "#{exclude_min ? "(" : nil}#{min.to_f}", "#{exclude_max ? "(" : nil}#{max.to_f}"
      query.push_command command, :key => key, :short_key => sorted_set_key(nil, ""), :weight => multiplier unless range_only
      query.push_command :zremrangebyscore, :arg => ['-inf', min]
      query.push_command :zremrangebyscore, :arg => [max, 'inf']
      self
    end
    
  end
  
  
    # be advised: this construction has little to no error-checking, so garbage in garbage out.
  class Query
    attr_accessor :redis_prefix
    def initialize(arg)
      @queue = []
      @redis_prefix = (arg[:prefix] || arg[:redis_prefix]) + self.class.name + ":"
      @redis=arg[:redis] || $redis
      @subquery = []
      self
    end
    
    def union(index, val)
      index.build_query_part :zunionstore, self, val
      self
    end
    
    def intersect(index, val)
      index.build_query_part :zinterstore, self, val
      self
    end
    
    def sort(index, direction=:asc)
      index.build_query_part :zinterstore, self, nil, (direction == :asc ? -1 : 1)
      self
    end
    
    def query(force=nil)
      @subquery.each { |q| q.query }
      if force || !@redis.exists(results_key)
        temp_set = "#{@redis_prefix}Query:temp_sorted_set:#{digest results_key}"
        @redis.del temp_set if force
        first = @queue.first
        @redis.multi do
          @queue.each do |cmd|
            Rails.logger.info cmd.inspect
            if [:zinterstore, :zunionstore].member? cmd[:command]
              if first == cmd
                @redis.send cmd[:command], temp_set, cmd[:key], :weights => cmd[:weight]
              else
                @redis.send cmd[:command], temp_set, cmd[:key] + [temp_set], :weights => (cmd[:weight] + [0])
              end
            else
              @redis.send cmd[:command], temp_set, *cmd[:arg]
            end
          end
        end
        @redis.rename temp_set, results_key #don't care if there's no temp_set, we're in a multi.
        @redis.expire results_key, 3.minutes
      end
      self
    end
    
    def results(*arg, &block)
      query
      if arg.first && arg.first.kind_of?(Range)
        first, last = arg.first.begin, arg.first.end - (arg.first.exclude_end? ? 1 : 0)
      else
        first, last = arg.first.to_i, (arg.second || -1).to_i
      end
      res = @redis.zrange(results_key, first, last)
      if block_given?
        res.map! &block
      end
      res
    end
    
    def results_key
      @results_key ||= @redis_prefix + "results:" + digest( @queue.map { |q| 
        key = "#{q[:command]}:"
        if !(q[:short_key].empty? && q[:key].empty?)
          key << (q[:short_key].empty? ? q[:key] : q[:short_key]).sort.join("&")
        else
          key << digest(q[:arg].to_json)
        end
        key
      }.join("&"))
    end
    
    def digest(value)
      #value
      Digest::SHA1.hexdigest value.to_s
    end
    
    def length
      query
      @redis.zcard results_key
    end
    alias :size :length
    alias :count :length
    
    def push_command(cmd, arg={})
      cmd ||= arg[:command]
      raise "command must be symbol-like" unless cmd.respond_to? :to_sym
      cmd = cmd.to_sym
      if (@queue.length == 0 || @queue.last[:command]!=cmd) || !([:zinterstore, :zunionstore].member? cmd)
        @queue.push :command => cmd, :key =>[], :weight => [], :short_key => []
      end
      last = @queue.last
      unless arg[:key].nil?
        last[:key] << arg[:key]
        last[:weight] << arg[:weight] || 0
      end
      last[:short_key] << arg[:short_key] unless arg[:short_key].nil?
      last[:arg] = arg[:arg]
      self
    end
    
    def build_query_part(command, query, *arg)
      query.push_command command, :key => results_key
    end
    
    def subquery
      @subquery << self.class.new(self.model)
      @subquery.last
    end
    
    def marshal_dump
      instance_values.merge "redis" => false
    end
    def marshal_load(arg)
      arg.each do |n,v|
        instance_variable_set "@#{n}", v
      end
      @redis ||= $redis
    end
  end
  
  class ActiveRecordQuery < RedisIndex::Query
    attr_accessor :model, :params
    def initialize(arg=nil)
      @params = {}
      @model = arg.kind_of?(Hash) ? arg[:model] : arg
      raise ArgumentError, ":model arg must be an ActiveRecord model, got #{arg.inspect} instead." unless @model.kind_of?(Class) && @model < ActiveRecord::Base
      super :prefix => @model.redis_prefix
    end

    def results(*arg)
      super *arg do |id|
        @model.find_cached id
      end
    end

    #retrieve query parameters, as fed through union and intersect
    def param(param_name)
      @params[param_name.to_sym]
    end
    def sorting_by
      @sort_by
    end

    def union(index_name, val=nil)
      #print "UNION ", index_name, " : ", val.inspect, "\r\n"
      super @model.redis_index(index_name, SearchIndex), val
      @params[index_name.to_sym]=val if index_name.respond_to? :to_sym
      self
    end
    def intersect(index_name, val=nil)
      #print "INTERSECT ", index_name, " : ", val.inspect, "\r\n"
      super @model.redis_index(index_name, SearchIndex), val
      @params[index_name.to_sym]=val if index_name.respond_to? :to_sym
      self
    end
    def sort(index_name, direction=:asc)
      super @model.redis_index(index_name, SearchIndex), direction
      @sort_by = index_name
      self
    end
  end
  
  def self.included(base)
    base.class_eval do
      class << self
        def redis_index (index_name=nil, index_class = Index)
          raise ArgumentError, "#{index_class} must be a subclass of RedisIndex::Index" unless index_class <= Index
          case index_name 
          when index_class
            return index_name 
          when NilClass
            return @redis_indices 
          when Query
            return index_name
          end
          @redis_indices||=[] #this line sucks.
          index = @redis_indices.find { |i| index_name.respond_to?(:to_sym) && i.name == index_name.to_sym && i.kind_of?(index_class)}
          raise Exception, "Index #{index_name} not found in #{name}" unless index
          index
        end
        def add_redis_index(index)
          @redis_indices||=[]
          @redis_indices.push index
        end
        def redis_prefix
          @redis_prefix||="Rails:#{Rails.application.class.parent.to_s}:#{self.name}:"
        end
      end
    end
    base.extend ClassMethods
    base.after_create :create_redis_indices
    base.before_save :update_redis_indices, :uncache
    base.before_destroy :delete_redis_indices, :uncache
    base.after_initialize do 
      
    end

  end

  module ClassMethods
    def index_attribute(arg={}, &block)
      index_class = arg[:index] || SearchIndex
      raise ArgumentError, "index argument must be in RedisIndex::Index if given" unless index_class <= Index
      index_class.new(arg.merge(:model => self), &block)
    end

    def index_attribute_for(arg)
      raise ArgumentError ,"index_attribute_for requires :model argument" unless arg[:model]
      index = index_attribute(arg) do |index|
        index.name = "foreign_index_#{index.name}"
      end
      arg[:model].send :index_attribute, arg.merge(:index=> ForeignIndex, :real_index => index)
    end

    def index_attribute_from(arg) #doesn't work yet.
      model = arg[:model]
      model.send(:include, RedisIndex) unless model.include? RedisIndex
      model.send(:index_attribute_for, arg.merge(:model => self))
    end

    def index_range_attribute(arg)
      index_attribute arg.merge :index => RangeIndex, :use_existing_index => true
    end
    alias index_sort_attribute index_range_attribute

    def index_attributes(*arg)
      arg.each do |attr|
        index_attribute :attribute => attr
      end
      self
    end

    def redis_query
      query = ActiveRecordQuery.new :model => self
      yield query if block_given?
      query
    end

    def build_redis_indices
      start_time = Time.now
      all = self.find(:all)
      sql_time = Time.now - start_time
      redis_start_time, printy, total =Time.now, 0, all.count - 1
      all.each_with_index do |row, i|
        if printy == i
          print "\rBuilding redis indices... #{((i.to_f/total) * 100).round.to_i}%"
          printy += (total * 0.05).round
        end
        row.create_redis_indices 
      end
      print "\rBuilt redis indices for #{total} rows in #{(Time.now - redis_start_time).round 3} sec. (#{sql_time.round 3} sec. for SQL).\r\n"
      #update all foreign indices
      foreign = 0
      redis_index.each do |index|
        if index.kind_of? ForeignIndex
          foreign+=1
          index.real_index.model.send :build_redis_indices 
        end
      end
      puts "Built #{redis_index.count} ind#{redis_index.index.count == 1 ? "ex" : "ices"} (#{foreign} foreign) for #{self.name} in #{(Time.now - start_time).round(3)} seconds."
      self
    end
      
    def cache_key(id)
      "#{redis_prefix}#{id}:cached"
    end
    
    def find_cached(id)
      key = cache_key id
      if marshaled = $redis.get(key)
        Marshal.load marshaled
      elsif (found = find id)
        $redis.set key, Marshal.dump(found)
        found
      end
    end
  end
  
  [:create, :update, :delete].each do |op|
    define_method "#{op}_redis_indices" do
      self.class.redis_index.each { |index| index.send op, self}
    end
  end
  
  def uncache
    $redis.del self.class.cache_key(id)
  end
end