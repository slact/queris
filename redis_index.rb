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
      raise Exception, "Index must have a name" unless @name
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
      @redis.sadd set_key(val || obj.send(@attribute)), obj.send(@key)
    end
    def remove(obj, val = nil)
      @redis.srem set_key(val || obj.send(@attribute)), obj.send(@key)
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
  
  class CustomIndex < SearchIndex
    def initialize(arg)
      raise ArgumentError, "Missing required initialization attribute real_index for ForeignIndex." unless arg[:real_index]
      super arg
    end
    [:add, :remove, :update].each do |method_name|
      define_method method_name, do |*arg|
        send "#{method_name}_block", *arg
      end
    end
  end
  
  class SortIndex < Index
    def initialize(arg)
      super arg
    end
    
    def hash_key(val, prefix=nil)
      @keyf % [prefix || @redis_prefix || @model.redis_prefix, val]
    end
    
    def add(obj, value=nil)
      @redis.hset hash_key(obj.send @key), @name, val(value || value_is(obj))
    end
    alias create add
    
    def remove(obj, value=nil)
      hkey = hash_key(obj.send @key)
      @redis.hdel hkey, @name
      @redis.del hkey unless @redis.hlen(hkey) > 0
    end
    alias delete remove
    
    def update(obj)
      add(obj) if value_is(obj) != value_was(obj)
    end
    
    def sort_by
      hash_key "*->#{@name}"
    end
  end
  
    # be advised: this construction has little to no error-checking, so garbage in garbage out.
  class Query
    def initialize(arg)
      @queue = []
      @redis_prefix = (arg[:prefix] || arg[:redis_prefix]) + self.class.name + ":"
      @sort_options = {}
      @redis=arg[:redis] || $redis
      self
    end
    
    def union(index, val)
      build_query :sunionstore, index, val
    end
    
    def intersect(index, val)
      build_query :sinterstore, index, val
    end
    
    def build_query(op, index, value)
      
      @results_key = nil
      if @queue.length == 0 || @queue.last[:operation]!=op
         @queue.push :operation => op, :key =>[], :results_key => []
      end
      last = @queue.last
      (value.kind_of?(Enumerable) ?  value : [ value ]).each do |a_value|
        last[:key].push index.set_key a_value
        last[:results_key].push index.set_key a_value, ""
      end
      self
    end
    
    def query(force=nil)
      temp_set = "#{@redis_prefix}temp_set:(#{results_key})"
      @sort_options[:store] ||= results_key
      if force || !@redis.exists(results_key)
        @redis.del temp_set if force
        first = @queue.first
        @queue.each do |q|
          if first!=q
            @redis.send q[:operation], temp_set, temp_set, *q[:key]
          else
            @redis.send q[:operation], temp_set, *q[:key]
          end
        end
        @redis.expire temp_set, 10 #10-second search set timeout
        @redis.sort temp_set, @sort_options
        @redis.expire @cache_key, 5*60    
      else
        @redis.sort results_key, @sort_options
      end
      self
    end
    
    def results(first=0, last=-1, &block)
      res = @redis.lrange(results_key, first, last)
      if block_given?
        res.map! &block
      end
      res
    end
    
    def sort(index_name, direction=:asc)
      index = @model.redis_index(index_name, SortIndex)
      @sort_options[:by] = index.sort_by
      @sort_options[:order] = direction.to_s
      self
    end
    
    def results_key
      @results_key ||= @redis_prefix + "results:" + ( @queue.map { |q| "#{q[:operation]}:" + q[:results_key].sort.join("&")}.join("&"))
      @results_key
    end
    
    def length
      @redis.llen results_key
    end
    alias :size :length
    alias :count :length
    
  end
  
  class ActiveRecordQuery < RedisIndex::Query
    def initialize(arg)
      @model = arg[:model]
      super :prefix => @model.redis_prefix
    end
    
    def results(*arg)
      super *arg do |id| 
        @model.find id
      end
    end
    def build_query(op, index_name, value)
      index = @model.redis_index index_name
      super op, index, value
    end
  end
  
  def self.included(base)
    base.class_eval do
      class << self
        def redis_index (index_name=nil, index_class = Index)
          raise ArgumentError, "#{index_class} must be a subclass of RedisIndex::Index" unless index_class <= Index
          if index_name.kind_of? index_class
            return index_name 
          elsif index_name.nil?
            return @redis_indices 
          end
          @redis_indices||=[] #this line sucks.
          index = @redis_indices.find { |i| i.name == index_name.to_sym && i.kind_of?(index_class)}
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
    base.before_save :update_redis_indices
    base.before_destroy :delete_redis_indices
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
    
    def index_foreign_attribute(arg) #doesn't work yet.
      raise Exception, "Not implemented"
      arg[:model].class_eval do
        include RedisIndex unless include? RedisIndex
      end unless arg[:model].nil?
      index_attribute_for
    end
    
    def index_sort_attribute(arg)
      index_attribute arg.merge :index => SortIndex
    end
    
    def indexing_attribute_from(*arg) #dummy method
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
  end
  
  [:create, :update, :delete].each do |op|
    define_method "#{op}_redis_indices" do
      self.class.redis_index.each { |index| index.send op, self}
    end
  end
end