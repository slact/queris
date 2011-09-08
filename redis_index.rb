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
        query.push_command :command => command, :key => set_key(a_value), :short_key => set_key(a_value, "")
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
    
    def build_query_part(command, query, val, multiplier=1)
      param = {:command => command}
      case val
      when Range
        range command, query, val.begin, val.end, false, val.exclude_end?, nil, multiplier
      when Enumerable
        raise ArgumentError, "RangeIndex doesn't accept non-Range Enumerables"
      else
        range command, query, '-inf', val || 'inf', false, true, nil, multiplier
        range command, query, val, 'inf', true, false, true, multiplier if val
      end
      self
    end
    
    private
    def range(command, query, min, max, exclude_min, exclude_max, range_only=nil, multiplier=1)
      key = sorted_set_key
      min, max = "#{exclude_min ? "(" : nil}#{min}", "#{exclude_max ? "(" : nil}#{max}"
      query.push_command :command => command, :key => key, :short_key => sorted_set_key(nil, ""), :weight => multiplier unless range_only
      query.push_command :command => :zremrangebyscore, :arg => ['-inf', min]
      query.push_command :command => :zremrangebyscore, :arg => [max, 'inf']
      self
    end
    
  end
  
  
    # be advised: this construction has little to no error-checking, so garbage in garbage out.
  class Query
    def initialize(arg)
      @queue = []
      @redis_prefix = (arg[:prefix] || arg[:redis_prefix]) + self.class.name + ":"
      @redis=arg[:redis] || $redis
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
      if force || !@redis.exists(results_key)
        temp_set = "#{@redis_prefix}Query:temp_sorted_set:#{digest results_key}"
        @redis.del temp_set if force
        first = @queue.first
        @queue.each do |cmd|
          @redis.multi do
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
        @redis.rename temp_set, results_key if @redis.exists temp_set
        @redis.expire results_key, 3*60
      end
      self
    end
    
    def results(first=0, last=-1, &block)
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
      @redis.zcard results_key
    end
    alias :size :length
    alias :count :length
    
    def push_command(arg={})
      cmd = arg[:command]  
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
      self.query
      query.push_command :command => command, :key => results_key
    end
  end
  
  class ActiveRecordQuery < RedisIndex::Query
    def initialize(arg)
      @model = arg[:model]
      super :prefix => @model.redis_prefix
    end
    
    def results(*arg)
      super *arg do |id| 
        @model.find_cached id
      end
    end
    
    def union(index_name, val=nil)
      super @model.redis_index(index_name, SearchIndex), val
    end
    def intersect(index_name, val=nil)
      super @model.redis_index(index_name, SearchIndex), val
    end
    def sort(index_name, direction=:asc)
      super @model.redis_index(index_name, SearchIndex), direction
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
    
    def index_sort_attribute(arg)
      index_attribute arg.merge :index => RangeIndex, :use_existing_index => true
    end
    
    
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
      self.find(:all).each { |row| row.create_redis_indices }
      #update all foreign indices
      redis_index.each do |index|
        if index.kind_of? ForeignIndex
          index.real_index.model.send :build_redis_indices 
        end
      end
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