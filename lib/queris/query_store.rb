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
    index_only
    live_queries
    
    class << self
      @metaquery_ttl = 600
      attr_accessor :metaquery_ttl
      def redis(another_model = nil)
        another_model = another_model.model if Query === another_model
        if another_model == self
          r = Queris.redis "metaquery:metaquery"
        else
          r= Queris.redis :'metaquery:slave', :metaquery
        end
        raise "No appropriate redis connection found for QueryStore. Add a queris connection with the metaquery role (Queris.add_redis(r, :metaquery), or add live_queries to desired models." unless r
        r
      end
      
      def index_to_val(index)
        Index === index ? "#{index.model.name}:#{index.class.name.split('::').last}:#{index.name}" : index
      end

      def add(query)
        redis.pipelined do
          redis_indices.each {|i| i.add query}
          update query
        end
        #puts "added #{query} to QueryStore"
      end
      def remove(query)
        redis.pipelined do
          redis_indices.each { |i| i.remove query }
        end
        #puts "removed #{query} from QueryStore"
      end
      def update(query)
        redis.pipelined do
          redis.setex, "Queris:Metaquery:expire:#{query.marshaled}", query.ttl
        end
      end

      #NOT EFFICIENT!
      def all_metaqueries
        q=query(self, :ttl => 20).static!
        redis_indices(live: true).each { |i| q.union(i) }
        q.results
      end
      
      def query(model, arg={})
        Metaquery.new(self, arg.merge(:target_model => model, :realtime => true))
      end
      def metaquery(arg={})
        query self, arg
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
        @redis || model.redis(@target_model) || Queris::redis(:'metaquery:slave') || redis_master
      end
      def static!; @live=false; @realtime=false; self; end
      def realtime?; @realtime; end
      def realtime!
        live!
        @realtime=true
        self
      end
      def results_with_gc
        res = results(:replace_command => true) do |cmd, key, first, last, rangeopt|
          redis.evalsha(Queris.script_hash(:results_with_ttl), [key], ["Queris:Metaquery:expire:%s"])
        end
        res = [[],[]] if res.empty?
        res.first.map! do |marshaled|
          QueryStore.find marshaled
        end
        #garbage-collect the expired stuff
        res.last.each do |marshaled|
          QueryStore.remove QueryStore.find(marshaled)
        end
        res.first
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
