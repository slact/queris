module Queris
  
  module ActiveRecordMixin
    def self.included base
      base.after_create :create_redis_indices
      base.before_save :update_redis_indices, :uncache
      base.before_destroy :delete_redis_indices, :uncache
      base.extend ActiveRecordMixin
    end
    def redis_query(arg={})
      query = ActiveRecordQuery.new arg.merge(:model => self)
      yield query if block_given?
      query
    end
    def find_all
      find :all
    end
  end
  
  class ActiveRecordQuery < Query
    attr_accessor :model, :params
    def initialize(arg=nil)
      @params = {}
      @model = arg.kind_of?(Hash) ? arg[:model] : arg
      raise ArgumentError, ":model arg must be an ActiveRecord model, got #{arg.inspect} instead." unless @model.kind_of?(Class) && @model < ActiveRecord::Base
      super arg.merge(:prefix => @model.redis_prefix)
    end

    def results(*arg)
      super(*arg) do |id|
        @model.find_cached id
      end
    end

    #retrieve query parameters, as fed through union and intersect
    def param(param_name)
      @params[param_name.to_sym]
    end
    def union(index_name, val=nil)
      #print "UNION ", index_name, " : ", val.inspect, "\r\n"
      super @model.redis_index(index_name, SearchIndex), val
      @params[index_name.to_sym]=val if index_name.respond_to? :to_sym
      self
    end
    def diff(index_name, val=nil)
      #print "UNION ", index_name, " : ", val.inspect, "\r\n"
      super @model.redis_index(index_name, SearchIndex), val
      @params[index_name.to_sym]=val if index_name.respond_to? :to_sym
      self
    end
    def intersect(index_name, val=nil)
      #print "INTERSECT ", index_name, " : ", val.inspect, "\r\n"
      super @model.redis_index(index_name, SearchIndex), val
      @params[index_name.to_sym]=val if index_name.respond_to? :to_sym
      self
    end
    def sort(index_name, reverse = nil)
      if index_name.kind_of?(Query)
        raise "sort can be extracted only from query using the same model..." unless index_name.model == model
        sort_query = index_name
        if sort_query.sorting_by.nil?
          index_name = nil
        else
          index_name = sort_query.sort_index_name
          sort_query.sort nil #unsort sorted query
        end
      end
      if index_name.respond_to?('[]') && index_name[0] == '-'
        reverse = true
        index_name = index_name[1..-1]
      end
      super @model.redis_index(index_name, RangeIndex), reverse
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