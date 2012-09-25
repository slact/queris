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
        @hashcache ||= stored_in_redis?
        @hashkey ||= @hashcache.key '%s'
        ActiveRecordQuery.new self, arg.merge(from_hash: @hashkey), &block
      end
      def find_all
        find :all
      end
      def stored_in_redis?
        @hashcache ||= redis_index(:all_attribute_hashcache, Queris::HashCache)
      end
      def find_cached(id, opt={})
        @hashcache ||= stored_in_redis?
        if (obj = @hashcache.fetch(id, opt))
          return obj
        elsif !opt[:nofallback]
          begin
            obj = find(id)
          rescue
            obj = nil
          end
          @hashcache.create obj if obj
          obj
        end
      end
      def restore(hash)
        unless (@hashcache ||= stored_in_redis?)
          raise "Can't restore ActiveRecord model from hash -- there isn't a HashCache index present. (Don't forget to use cache_all_attributes on the model)"
        end
        @hashcache.load_cached hash
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
