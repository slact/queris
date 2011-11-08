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
      super(*arg) do |id|
        @model.find_cached id
      end
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