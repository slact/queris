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
    
    class << self
      def redis(another_model = nil)
        another_model = another_model.model if Query === another_model
        if another_model == self
          Queris.redis "#{another_model.name}:metaquery"
        else
          roles = [ :metaquery ]
          roles.unshift("#{another_model.name}:metaquery".to_sym) if another_model
          Queris.redis(*roles)
        end
        #raise "No appropriate redis connection found for QueryStore. Add a queris connection with the metaquery role (Queris.add_redis(r, :metaquery), or add live_queries to desired models." unless r
      end
      
      def index_to_val(index)
        Index === index ? "#{index.model.name}:#{index.class.name.split('::').last}:#{index.name}" : index
      end
      
      def add(query)
        pipelined_each_index(query) {|i| i.add query}
      end
      def remove(query)
        pipelined_each_index(query) {|i| i.remove query}
      end
      def pipelined_each_index(q)
        redis(q).multi do
          redis_indices.each do |index|
            yield index
          end
        end
      end
      private :pipelined_each_index
      
      def set_flag(query, *flags)
        if flags.count = 1
          redis(query).setex query.results_key(flags.first), 1, query.ttl
        else
          redis(query).multi do |r|
            flags.each {|flag| redis(query).setex query.results_key(flag), 1, query.ttl }
          end
        end
      end
      alias :set_flags :set_flag
      def refresh_flag(query, flag)
        redis(query).expire query.results_key(flag), query.ttl
      end
      def clear_flag(query, *flags)
        redis(query).multi do |r|
          flags.each {|flag| redis(query).del(query.results_key flag)}
        end
      end
      alias :clear_flags :clear_flag
      def get_flag(query, flag)
        redis(query).exists query.results_key(flag)
      end

      def query(model, arg={})
        Metaquery.new(self, arg.merge(:target_model => model))
      end

      def find(marshaled)
        Marshal.load(marshaled)
      end

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
