module Queris
  
  module ActiveRecordMixin
    def self.included base
      base.after_create :create_redis_indices
      base.before_save :update_redis_indices, :maybe_uncache_result
      base.before_destroy :delete_redis_indices, :uncache_result
      base.extend ActiveRecordMixin
    end
    def redis_query(arg={})
      query = ActiveRecordQuery.new self, arg
      yield query if block_given?
      query
    end
    def find_all
      find :all
    end
    def ignore_attributes_for_results_cache *arg
      @ignored_attributes_for_cache||=[]
      if arg.count == 0
        @ignored_attributes_for_cache
      else
        @ignored_attributes_for_cache += arg.map{|x| x.to_s}
        arg.each do |attr_name|
          @ignored_attributes_for_cache
        end
      end
    end
    def expire_result_cache ttl=nil
      if ttl.nil?
        @result_cache_ttl
      else
        @result_cache_ttl = ttl
      end
    end
    
    def cache_key(id)
    "#{self.redis_prefix}#{id}:cached"
    end
    
    def cache_result(id, res, expire=nil)
      key = self.cache_key id
      Queris.redis.set key, Marshal.dump(res)
      if expire = expire_result_cache
        Queris.redis.expire key, expire
      end
      res
    end
    def uncache_result
      Queris.redis.del self.class.cache_key(self.id)
    end
    
    def find_cached(id, cache_it=true)
      key = cache_key id
      if marshaled = Queris.redis.get(key)
        res = Marshal.load marshaled
      elsif cache_it
        begin
          res = find(id)
        rescue
          res = nil
        end
        cache_result id, res if res
      end
    end
    
    def maybe_uncache_result
      if (self.changed - (self.class.ignore_attributes_for_results_cache || [])).count > 0
        uncache_result
      end
    end
    
  end
  
  class ActiveRecordQuery < Query
    attr_accessor :params
    def initialize(model, arg=nil)
      if model.kind_of?(Hash) and arg.nil?
        arg, model = model, model[:model]
      elsif arg.nil?
        arg= {}
      end
      @params = {}
      unless model.kind_of?(Class) && model < ActiveRecord::Base
        raise ArgumentError, ":model arg must be an ActiveRecord model, got #{model.respond_to?(:superclass) ? model.superclass.name : model} instead."
      end
      super model, arg
    end

    def results(*arg)
      query_ids = {}
      res = super(*arg)
      res.each_with_index do |id, i|
        if(cached = @model.find_cached id, false).nil?
          query_ids[id.to_i]=i
        else
          res[i]=cached
        end
      end
      sql_res = @model.find(query_ids.keys)
      sql_res.each do |found|
        res[query_ids[found.id]]=found
        @model.cache_result found.id, found
      end
      res
    end

    def subquery(arg={})
      if arg.kind_of? Query #adopt a given query as subquery
        raise "Trying to use a subquery from a different model" unless arg.model == model
      else #create new subquery
        arg[:model]=model
      end
      super arg
    end

  end
end