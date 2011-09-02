require 'rubygems'
module RedisIndex
  class Query
    require 'digest/sha1'

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
        @temp_set << ":#{Digest::SHA1.hexdigest @cache_key}"
        puts "queue", @queue
        @queue.each { |f| puts f; puts f.call }
      end
      $redis.sort @temp_set, {:store => @cache_key }
      self
    end
    
    def results(limit, offset, &block)
      res = $redis.lrange(@cache_key, offset || 0, limit && (offset || 0 + limit))
      if block_given?
        res.map! block
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
      @cache_key << ":#{op}=#{arg.join('&')}"
      @queue << lambda do
        puts op, first
        arg.unshift(@temp_set) unless first
        $redis.call(op, @temp_set, *arg)
      end
      self
    end
  end
  
  def self.included(base)
    base.extend ClassMethods
    base.after_create :create_redis_indices
    base.before_save :update_redis_indices
    base.before_destroy :delete_redis_indices
  end
  
  class ActiveRecord::Base
    def self.index_attribute(attr, attr_type="string", *rest)
      #raise AttributeNotRecognizedError if !attribute_names.include? attr    
      @@redis_indices[attr.to_sym]= attr_type.to_s
    end
    def self.index_attributes(*arg)
      arg.each do |arr|
          index_attribute arr
      end
      self
    end
  end
end