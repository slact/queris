module Queris
  module OhmMixin
    def self.included(base)
      base.class_eval do
        include Ohm::Callbacks
        after :create, :create_redis_indices
        after :save, :update_redis_indices
        before :delete, :delete_redis_indices
      end
    end
    
    def redis_query(arg={})
      query = OhmQuery.new self, arg
      yield query if block_given?
      query
    end
    
    def find_cached(id, *arg)
      self[id]
    end
  end

  class OhmQuery < Query
    attr_accessor :params
    def initialize(model, arg=nil)
      if model.kind_of?(Hash) and arg.nil?
        arg, model = model, model[:model]
      elsif arg.nil?
        arg= {}
      end
      @params = {}
      unless model.kind_of?(Class) && model < Ohm::Model
        raise ArgumentError, ":model arg must be an Ohm model model, got #{model.respond_to?(:superclass) ? model.superclass.name : model} instead."
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
