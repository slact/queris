require 'rubygems'

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
      @name = @name.to_sym
      @attribute = @attribute.to_sym
      @redis ||= $redis
      raise Exception, "Model not passed to index." unless @model
    end
    
    require 'digest/sha1'
    def digest(val)
      Digest::SHA1.hexdigest val.to_s
    end
    
    def value_is(obj)
      obj.send @attribute
    end
    def value_was(obj)
      obj.send "#{@attribute}_was"
    end
    def key(val, prefix=nil)
      "#{prefix || @model.redis_prefix}#{self.class.name}:#{@name}=#{digest val}"      
    end
    def add(val, id)
      @redis.sadd(key(val), id)
    end
    def remove(val, id)
      @redis.sremove(key(val), id)
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
    
    def results(*arg, &block)
      res = @redis.lrange(results_key, *arg)
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
      last[:key].push index.key value
      last[:results_key].push index.key value, ""
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
    
    def results(limit, offset)
      super limit, offset do |id| 
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
    base.extend ClassMethods
    base.after_create :create_redis_indices
    base.before_save :update_redis_indices
    base.before_destroy :delete_redis_indices
    base.after_initialize do 
      
    end

  end

  module ClassMethods
    def index_attribute(arg={})
      @redis_prefix ||= "Rails:#{Rails.application.class.parent.to_s}:#{self.name}:"
      @redis_indices ||= {}
      arg[:model] ||= self
      new_index = RedisIndex::Index.new arg
      redis_indices[new_index.name] = new_index
    end
    def index_attrribute_for(arg)
      
      index_attribute(attr, attr_type, model.redis_prefix, index_name)
      
      model.index_attribute(attr, attr_type, model.redis_prefix, index_name)
      
    end
    def redis_indices
      @redis_indices
    end
    def redis_prefix
      @redis_prefix
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
      self
    end
  end
  
  def create_redis_indices
    self.class.redis_indices.each do |index_name, index|
      index.add index.value_is(self), id
    end
  end
  #after_create :create_redis_indices
  
  def update_redis_indices
    self.class.redis_indices.each do |index_name, index|
      attr_is, attr_was = index.value_is(self), index.value_was(self)
      if attr_is != attr_was
          index.add attr_is, id 
          index.remove attr_was, id
      end
    end
  end
  #before_save :update_redis_indices
  
  def delete_redis_indices
    self.class.redis_indices.each do |index_name, index|
      index.remove index.value_is(self), id
    end
  end
  #before_destroy :delete_redis_indices
  
end