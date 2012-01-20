module Queris
  
  module ActiveRecordMixin
    def self.included base
      base.after_create :create_redis_indices
      base.before_save :update_redis_indices
      base.before_destroy :delete_redis_indices
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
    
    def cache_all_attributes
      Queris.register_model self
      Queris::HashCache.new :model => self
    end
    
    def find_cached(id, cache_it=true)
      #POTENTIAL OPTIMIZATION: accept Enumerable id, pipeline redis commands
      cache = redis_index("all_attribute_hashcache", Queris::HashCache)
      if (obj = cache.fetch(id))
        return obj
      elsif cache_it
        begin
          obj = find(id)
        rescue
          obj = nil
        end
        cache.create obj if obj
        obj
      end
    end
    
    def all_cacheable_attributes
      attribute_names
    end
    
    def changed_cacheable_attributes
      changed
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
      res_ids = super(*arg)
      res = []
      res_ids.each_with_index do |id, i|
        unless (cached = @model.find_cached id).nil?
          res << cached
        end
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
