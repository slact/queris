module Queris
  module QuerisModelMixin
    def self.included(base)
      base.extend QuerisModelMixin
    end
    def redis_query(arg={})
      query = QuerisModelQuery.new self, arg.merge(:redis => redis(true))
      yield query if block_given?
      query
    end
    
    def find(id)
      new.set_id(id).load
    end
    
    #don't save attributes, just index them. useful at times.
    def index_only
      @index_only = true
    end
    
    def index_attribute(arg={}, &block)
      if arg.kind_of? Symbol 
        arg = {:attribute => arg }
      end
      super arg.merge(:redis => redis), &block
    end
  end
  
  class QuerisModelQuery < Query
    #TODO
    attr_accessor :params
    def initialize(model, arg=nil)
      if model.kind_of?(Hash) and arg.nil?
        arg, model = model, model[:model]
      elsif arg.nil?
        arg= {}
      end
      @params = {}
      super model, arg
    end

    def results(*arg)
      #TODO
      res_ids = super(*arg)
      res = []
      res_ids.each_with_index do |id, i|
        unless (cached = @model.find id).nil?
          res << cached
        end
      end
      res
    end

    def subquery(arg={})
      #TODO
      if arg.kind_of? Query #adopt a given query as subquery
        raise "Trying to use a subquery from a different model" unless arg.model == model
      else #create new subquery
        arg[:model]=model
      end
      super arg
    end
  end
end
