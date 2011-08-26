module RedisIndex
  
  class Query
    require 'digest/sha1'

    def initialize(key_prefix)
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
      if !redis.exists @cache_key
        @temp_set << ":#{Digest::SHA1.hexdigest @cache_key}"
        @queue.each { |f| f() }
      end
      redis.sort @temp_set, {:store => @cache_key }
      self
    end
    
    def results(limit, offset)
      redis.lrange(key, start_i, end_i)
      
    end
    
    def sort(opts)
      opts.store = @cache_key
      @sort_options = opts
      self
    end

    def build_query(op, *arg)
      first = @queue.length==0
      @cache_key << ":#{op}=#{arg.join('&')}"
      @queue << lambda do  
        arg.unshift(@temp_set) unless first
        redis[op](@temp_set, *arg)
      end
      self
  end
  
  
  module ActiveRecord 
    def self.included(base)
      base.extend ClassMethods
    end
    
    module ClassMethods
      def build_redis_indices
        
      end
      def ohm_index(prop, type)
        
      end
    end
    
    def 
    
    after_initialize do
      @redis_indices = {}
      @redis_prefix = "foobar:"
    end
    
    class AttributeNotRecognizedError < ActiveRecordError; end
    def index_attribute(attr, attr_type, *rest)
      raise AttributeNotRecognizedError if !attributeNames.include? attr
      @redis_indices[attr_type.to_sym] = attr_type.to_s
    end
    
    after_create :create_redis_indices
    before_save :update_redis_indices
    before_destroy :delete_redis_indices
    
    protected
    @@redis_prefix = "Rails:" << Rails.application.class.parent << ":RedisIndex"
    def redis_index_key(attr, val)
        "#{@@redis_prefix}:#{self.class.name}:#{attr}"
    end
    
    def create_redis_indices
      @redis.indices.each do |attr, type|
        redis.sadd(redis_index_key attr self[:attr], self.id)
      end
    end
    
    def update_redis_indices
      @redis.indices.each do |attr, type|
        attr_is, attr_was = self[:attr] , self["#{attr}_was".to_sym]
        if attr_is != attr_was
            redis.srem(redis_index_key attr attr_was, self.id)
            redis.sadd(redis_index_key attr attr_is, self.id)
        end
      end
    end
    
    def delete_redis_indices
      @redis.indices.each do |attr, type|
        redis.srem(redis_index_key attr self[:attr], self.id)
      end
    end
    
 
    def search_redis
        class ActiveRecordQuery < Query
          def build_query(*arg)
            arg.map do |item| 
              ":#{item}:#{}"
            end
            super(*arg)
          end
        end
      query = ActiveRecordQuery.new @@redis_prefix
      yield query if block_given?
      query
    end
    
  end
end