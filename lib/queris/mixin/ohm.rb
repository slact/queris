module Queris
  module OhmMixin
    def self.included(base)
      begin
        require "ohm/contrib"
      rescue Exception => e
        raise LoadError, "ohm-contrib not found. Please ensure the ohm-contrib gem is available."
      end
    
      base.class_eval do
        include Ohm::Callbacks
        %w(create update delete).each do |action|
          hook="before_#{action}"
          alias_method hook "old_#{hook}" if respond_to?(hook)
          define_method "brefore_#{action}" do
            call("old_#{hook}") if respond_to? "old_#{hook}"
            call "#{action}_redis_indices"
          end
        end
      end
      
      base.extend OhmClassMixin
    end
    
    module OhmClassMixin
      def redis_query(arg={})
        query = OhmQuery.new self, arg
        yield query if block_given?
        query
      end
    
      def find_cached(id, *arg)
        self[id]
      end
      def restore(hash, arg)
        
      end
    end

    class OhmQuery < Query
      attr_accessor :params
      def ensure_same_redis(model)
        quer, ohmr = Queris.redis, self.db
        if !ohmr && quer
          Ohm.connect url: quer.id
        elsif !quer && ohmr
          Queris.add_redis Redis.new(url: ohmr.id)
        elsif quer && ohmr
          unless quer.id == ohmr.id
            raise Error, "Queris redis master server and Ohm redis server must be the same. There's just no reason to have them on separate servers."
          end
        end
        yield Queris.redis if block_given?
      end
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
    end
  end
end

