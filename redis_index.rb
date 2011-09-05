require 'rubygems'
require 'digest/sha1'

module RedisIndex
  
  class Index
    attr_accessor :name, :type, :attribute, :redis, :model
    def initialize(arg={})
      arg.each do |opt, val|
        instance_variable_set "@#{opt}".to_sym, val
        self.class.send :attr_accessor, "#{opt}".to_sym ##might be a problem
      end
      @type ||= "string"
      @name ||= @attribute
      @attribute ||= @name
      @name = @name.to_sym unless !@name
      @attribute = @attribute.to_sym unless !@attribute
      @redis ||= $redis
      @key ||= :id #object's key attribute (default is 'id')
      @set_keyf ||= "#{@prefix || @redis_prefix || @model.redis_prefix}#{self.class.name}:#{@name}=%s"
      raise Exception, "Model not passed to index." unless @model
    end
    
    def digest(val)
      Digest::SHA1.hexdigest val.to_s
    end
    
    def value_is(obj)
      obj.send @attribute
    end
    def value_was(obj)
      obj.send "#{@attribute}_was"
    end
    def set_key(val, prefix=nil)
      @set_keyf % digest(val)
    end
    def add(val, obj)
      @redis.sadd set_key(val), obj.send(@key) 
    end
    def remove(val, obj)
      @redis.sremove set_key(val), obj.send(@key)
    end
    def update(obj)
      val_is, val_was = value_is(obj), value_was(obj)
      if(val_is != val_was)
        remove(val_was, obj)
        add(val_is, obj)
      end
    end
    def create(obj)
      add(value_is(obj), obj)
    end
    def delete(obj)
      remove(value_was(obj), obj)
    end
  end
  
  class ForeignIndex < Index
    attr_accessor :real_index
    def initialize(arg)
      raise ArgumentError, "Missing required initialization attribute real_index for ForeignIndex." unless arg[:real_index]
      super arg
    end
    def add(*arg) end
    def remove(*arg) end
    def value_is(*arg) end
    def value_was(*arg) end
    def set_key(*arg)
      @real_index.set_key *arg
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
    
    require 'digest/sha1'
    def digest(val)
      Digest::SHA1.hexdigest val.to_s
    end
    
    def union(index, val)
      build_query :sunionstore, index, val
    end
    
    def intersect(index, val)
      build_query :sinterstore, index, val
    end
    
    def query(force=nil)
      temp_set = "#{@redis_prefix}temp_set:#{digest results_key}"
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
      end
      @redis.expire @temp_set, 10 #10-second search set timeout
      
      @sort_options[:store] ||= results_key
      @redis.sort temp_set, @sort_options
      
      @redis.expire @cache_key, 5*60 
      self
    end
    
    def results(first=0, last="", &block)
      res = @redis.lrange(results_key, first, last)
      if block_given?
        res.map! &block
      end
      res
    end
    
    def sort(opts)
      @sort_options = opts
      self
    end

    def build_query(op, index, value)
      @results_key = nil
      if @queue.length == 0 || @queue.last[:operation]!=op
         @queue.push :operation => op, :index => [], :value => [], :key =>[], :results_key => []
      end
      last = @queue.last
      last[:index].push index
      last[:value].push value
      last[:key].push index.set_key value
      last[:results_key].push index.set_key value, ""
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
    def get_index(index_name)
      @model.redis_indices[index_name.to_sym]
    end
    def build_query(op, index_name, value)
       index = get_index index_name
       super op, index, value
    end
  end
  
  def self.included(base)
    base.class_eval do
      class << self
        def redis_indices
          @redis_indices||={}
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
    def index_attribute(arg={})
      arg[:model] ||= self unless arg.kind_of? RedisIndex::Index
      new_index = (arg.kind_of? RedisIndex::Index) ? arg : RedisIndex::Index.new(arg)
      redis_indices[new_index.name.to_sym] = new_index
    end
    
    def index_attribute_for(arg)
      raise ArgumentError, "index_attribute_for requires :model argument" unless arg[:model]
      
      index = RedisIndex::Index.new(arg.merge :model => self)
      index.name = "foreign_index_#{index.name}"
      
      foreign_index = RedisIndex::ForeignIndex.new(arg.merge :real_index => index)
      arg[:model].redis_indices[foreign_index.name]=foreign_index
      self.redis_indices[index.name]=index
    end
    
    def index_foreign_attribute(arg) #doesn't work yet.
      raise Exception, "Not implemented"
      arg[:model].class_eval do
        include RedisIndex unless include? RedisIndex
        index_attribute_for
      end unless arg[:model].nil?
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
      res = self.find(:all).each do |row|
          row.create_redis_indices
      end
      redis_indices.each do |k, index|
        if index.kind_of? RedisIndex::ForeignIndex
          index.real_index.model.build_redis_indices 
        end
      end
      self
    end
  end
  
  [:create, :update, :delete].each do |op|
    define_method "#{op}_redis_indices" do
      self.class.redis_indices.each do |index_name, index|
      end
    end
  end
end