module Queris
  
  module ActiveRecordMixin
    def self.included base
      base.after_create :create_redis_indices
      base.before_save :update_redis_indices, :uncache
      base.before_destroy :delete_redis_indices, :uncache
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
      super model, arg.merge(:prefix => model.redis_prefix)
    end

    def results(*arg)
      query_ids = {}
      res = super(*arg)
      puts res.count
      res.each_with_index do |id, i|
        print "id:#{id},i:#{i}\r\n"
        if(cached = @model.find_cached id).nil?
          query_ids[id.to_i]=i
        else
          res[i]=cached
        end
      end
      sql_res = @model.find(query_ids.keys)
      puts sql_res.count
      sql_res.each do |found|
        res[query_ids[found.id]]=found
        @model.cache_result found.id, found
      end
      puts res.count
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