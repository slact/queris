module Queris
  module QuerisModelMixin
    def self.included(base)
      base.extend QuerisModelClassMixin
    end
    
    module QuerisModelClassMixin
      def redis_query(arg={})
        @hash_keyf ||= new.key '%s'
        query = QuerisModelQuery.new self, arg.merge(redis: redis(true), from_hash: @hash_keyf, delete_missing: true)
        yield query if block_given?
        query
      end
      alias :query :redis_query
      def add_redis_index(index, opt={})
        #support for incremental attributes
        @incremental_attr ||= {}
        ret = super(index, opt)
        if @incremental_attr[index.attribute].nil?
          @incremental_attr[index.attribute] = index.incremental?
        else
          @incremental_attr[index.attribute] &&= index.incremental?
        end
        ret
      end
      def stored_in_redis?; true; end
      def can_increment_attribute?( attr_name )
        @incremental_attr[attr_name.to_sym]
      end

      private
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
  end
end
