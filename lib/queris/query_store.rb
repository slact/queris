module Queris
  class QueryStore < Queris::Model
    attr_accessor :query
    attr_accessor :used_index
    attribute :id
    index_attribute name: :index, attribute: :all_live_indices, key: :marshaled, value: (proc do |index|
      case index
      when Enumerable
        index.map{|i| QueryStore.index_to_val i}
      when Index
        QueryStore.index_to_val index
      else
        index
      end
    end)
    index_only
    
    def self.redis(another_model = nil)
      roles = [ :metaquery ]
      roles.unshift ("#{another_model.name}:metaquery").to_sym if another_model
      Queris.redis *roles
    end
    
    def self.index_to_val(index)
      Index === index ? "#{index.model.name}:#{index.class.name.split('::').last}:#{index.name}" : index
    end
    
    def self.add(query)
      raise "Not a Queris query, can't add it as a Live query" unless Query === query
      (redis || query.model.redis).multi do
        redis_indices.each do |index|
          index.add query
        end
      end
    end
    def self.remove(query)
      (redis || query.model.redis).multi do
        redis_indices.each do |index|
          index.remove query
        end
      end
    end
    def self.query(model, arg={})
      Metaquery.new(self, arg.merge(:target_model => model))
    end

    def self.find(marshaled)
      Marshal.load(marshaled)
    end

    class Metaquery < QuerisModelQuery
      def initialize(model, arg={})
        raise "Metaqueries can't be live" if arg[:live]
        @target_model = arg[:target_model]
        arg[:profiler] = Queris::DummyProfiler.new
        super model, arg
      end
      def redis
        @redis || model.redis(@target_model)
      end
      %w( union diff intersect ).each do |op|
        define_method op do |index|
          index = @target_model.redis_index(index)
          super model.redis_indices.first, QueryStore.index_to_val(index)
        end
      end
    end
  end
end
