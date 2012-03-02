module Queris
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
