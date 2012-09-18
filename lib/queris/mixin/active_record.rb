module Queris
  
  module ActiveRecordMixin
    def self.included base
      base.after_create :create_redis_indices
      base.before_save :update_redis_indices
      base.before_destroy :delete_redis_indices

      def changed_cacheable_attributes
        changed
      end

      def all_cacheable_attributes
        attribute_names
      end

      base.extend ActiveRecordClassMixin
    end
    
    module ActiveRecordClassMixin
      def redis_query(arg={}, &block)
        ActiveRecordQuery.new self, arg, &block
      end
      def find_all
        find :all
      end
      def stored_in_redis?
        redis_index(:all_attribute_hashcache, Queris::HashCache)
      end
      def find_cached(id, opt={})
        cache = redis_index :all_attribute_hashcache, Queris::HashCache
        if (obj = cache.fetch(id, opt))
          return obj
        elsif !opt[:nofallback]
          begin
            obj = find(id)
          rescue
            obj = nil
          end
          cache.create obj if obj
          obj
        end
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
      if (hashcache_index = model.stored_in_redis?)
        arg.push({}) unless Hash === arg.last
        arg.last.merge! :replace_command => true
        ret = []
        super(*arg) do |cmd, key, first, last, rangeopt|
          raise "Results with_scores not yet implemented efficiently. Use raw_results if you must have scores." if rangeopt[:with_scores]
          res, failed_i = redis.evalsha(Queris::script_hash(:results_from_hash), [key], [cmd, first, last, hashcache_index.key('%s',nil,true)])
          res.each_with_index do |h, i|
            if failed_i.first == i
              failed_i.shift
              obj = model.find_cached(h)
            else
              hash = Hash[*h] if Array === h
              obj = hashcache_index.load_cached(hash)
            end
            ret << obj if obj
          end
        end
        ret
      else
        super(*arg) do |id|
          @model.find_cached id
        end
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
