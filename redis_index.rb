require 'rubygems'

module RedisIndex
  require 'digest/sha1'
  def self.digest(val)
    Digest::SHA1.hexdigest val.to_s
  end
  
    # be advised: this construction has little to no error-checking, so garbage in garbage out.
  class Query
    def initialize(key_prefix, *arg)
      @queue = []
      @prefix = key_prefix
      @cache_key = "#{@prefix}:query_cache"
      @temp_set = "#{@prefix}:temp_set"
      @sort_options = { :store => @cache_key }
      self
    end
    
    def union(*arg)
      build_query :sunionstore, *arg
    end
    
    def intersect(*arg)
      build_query :sinterstore, *arg
    end
    
    def query
      if !$redis.exists @cache_key
        @temp_set << ":#{RedisIndex.digest @cache_key}"
        @queue.each { |f| f.call }
      end
      $redis.expire @temp_set, 30 #30-second search set timeout
      $redis.sort @temp_set, {:store => @cache_key }
      $redis.expire @cache_key, 5*60 
      self
    end
    
    def results(start=0, finish=NaN, &block)
      res = $redis.lrange(@cache_key, start, finish)
      if block_given?
        res.map! &block
      end
      res
    end
    
    def sort(opts)
      opts.store = @cache_key
      @sort_options = opts
      self
    end

    def build_query(op, *arg)
      first = @queue.length==0
      @cache_key << ":#{op}=#{RedisIndex.digest arg.sort!.join('&')}"
      @queue << lambda do
        $redis.send(op, @temp_set, *arg)
      end
      self
    end
    
    def length
      $redis.llen @cache_key
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
      @redis_prefix ||= "Rails:" << Rails.application.class.parent.to_s << ":RedisIndex:"
      @redis_indices ||= {}
      @redis_indices[arg[:name] || arg[:attribute]]= {
        :type => arg[:type].to_s,
        :prefix => arg[:prefix] || @redis_prefix,
        :attribute => arg[:attribute].to_sym,
        :name => arg[:name] || arg[:attribute],
        :update => arg[:update].nil? ? true : arg[:update]
      }
    end
    def index_attr_for(model, attr, index_name, attr_type="string")
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
    
    def redis_index_key(index_name, val, prefix=nil)
      index = redis_indices[index_name.to_sym]
      raise ArgumentError, "No such index " << index_name.to_s << "." unless index
      "#{prefix || index[:prefix]}#{index[:name]}=#{RedisIndex.digest val}"
    end
      
    def redis_query
      query = ActiveRecordQuery.new self.redis_prefix, self
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
  
  def redis_index_key(index_name, val)
    self.class.redis_index_key(index_name, val)
  end
  
  def create_redis_indices
    self.class.redis_indices.each do |index_name, index|
      $redis.sadd(redis_index_key(index[:name], send(index[:attribute])), self.id) unless index[:dont_update]
    end
  end
  #after_create :create_redis_indices
  
  def update_redis_indices
    self.class.redis_indices.each do |index_name, index|
      attr_is, attr_was = send(index[:attribute]) , send("#{index[:attribute]}_was")
      if attr_is != attr_was && !index[:dont_update]
          $redis.srem(redis_index_key(index[:name], attr_was), self.id)
          $redis.sadd(redis_index_key(index[:name], attr_is), self.id)
      end
    end
  end
  #before_save :update_redis_indices
  
  def delete_redis_indices
    self.class.redis_indices.each do |index_name, index|
      $redis.srem(redis_index_key(index[:name], send(index[:attribute])), self.id) unless index[:dont_update]
    end
  end
  #before_destroy :delete_redis_indices
  
  class ActiveRecordQuery < RedisIndex::Query
    def initialize(prefix, model)
      @model = model
      super
    end
    
    def union(attr, val)
      super @model.redis_index_key(attr, val)
    end
    
    def intersect(attr, val)
       super @model.redis_index_key(attr, val)
    end
    
    def results(limit, offset)
      super limit, offset do |id| 
        @model.find id
      end
    end
  end
end