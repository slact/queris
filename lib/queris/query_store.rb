module Queris
  class QueryStore < Queris::Model
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
    index_range_attribute name: :expire, attribute: :ttl, key: :marshaled, value: (proc do |val|
      Time.now.to_f + val - 2 #this update doesn't happen atomically with query update, and we never want to update an expired query -- thus val - c (where c is small)
    end)
    index_only
    
    class << self
      @metaquery_ttl = 600
      attr_accessor :metaquery_ttl
      def redis(another_model = nil)
        another_model = another_model.model if Query === another_model
        r = Queris.redis(another_model == self ? "metaquery:metaquery" : :metaquery)
        raise "No appropriate redis connection found for QueryStore. Add a queris connection with the metaquery role (Queris.add_redis(r, :metaquery), or add live_queries to desired models." unless r
        r
      end
      
      def index_to_val(index)
        Index === index ? "#{index.model.name}:#{index.class.name.split('::').last}:#{index.name}" : index
      end

      def add(query)
        redis.multi do |r|
          redis_indices.each {|i| i.add query}
        end
        #puts "added #{query} to QueryStore"
      end
      def remove(query)
        redis.multi do |r|
          redis_indices.each { |i| i.remove query }
        end
        #puts "removed #{query} from QueryStore"
      end

      #NOT EFFICIENT!
      def all_metaqueries
        q=query(self, :ttl => 20).static!
        redis_indices(live: true).each { |i| q.union(i) }
        q.results
      end
      def all_queries(opt={})
        queries = []
        max_score = opt[:expired] ? Time.now.to_f : 'inf'
        res = redis.zrangebyscore redis_index(:expire).key, '-inf', max_score
        res.reverse_each do |marshaled_query|
          query = Marshal.load marshaled_query
          if query
            queries << query
          else
            failed << marshaled_query
          end
        end
        queries
      end

      def garbage_collect
        total_collected, failed = 0, []
        all_queries(:expired => true).each do |q|
          if Query === q
            remove q
            #puts "Garbage-Collected #{q}"
            total_collected += 1
          else
            failed << q
          end
        end
        puts "Garbage collection cleaned #{total_collected} quer#{total_collected == 1 ? 'y' : 'ies'}."
        puts "Failed to cleanup #{failed.count} non-deserializable queries" if failed.count > 0
        return total_collected, failed
      end
      alias :gc :garbage_collect

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
        @target_model = arg[:target_model]
        arg[:profiler] = Queris::DummyProfiler.new
        super model, arg
      end
      def redis_master
        Queris::redis :metaquery
      end
      def redis
        @redis || model.redis(@target_model) || redis_master
      end
      def static!; @live=false; @realtime=false; self; end
      def realtime?; @realtime; end
      def realtime!
        live!
        @realtime=true
        self
      end
      def results_exist?
        super(redis)
      end
      def set_param_from_index(*arg); self; end
      %w( union diff intersect ).each do |op|
        define_method op do |index|
          index = @target_model.redis_index(index)
          super model.redis_index(:index), QueryStore.index_to_val(index)
        end
      end
    end
  end
end
