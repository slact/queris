module Queris
  
  #black hole, does nothing, is nil.
  class DummyProfiler
    def method_missing(*args)
      self
    end
    def initialize(*args); end
    def nil?; true; end
  end
  
  module ObjectMixin
    def self.included base
      base.extend ClassMixin
    end

    module ClassMixin
      def redis_index(index_match=nil, index_class = Index)
        raise ArgumentError, "#{index_class} must be a subclass of Queris::Index" unless index_class <= Index
        case index_match
        when index_class, NilClass, Query
          return index_match
        when String
          index_match = index_match.to_sym
        end
        index = redis_index_hash[index_match]
        raise "Index #{index_match} not found in #{name}" unless index
        raise "Found wrong index class: expected #{index_class.name}, found #{index.class.name}" unless index.kind_of? index_class
        index
      end
      def redis_indices
        @redis_indices ||= superclass.respond_to?(:redis_indices) ? self.superclass.redis_indices.clone : []
        #BUG: redis_indices is very static. superclass modifications after class declaration will not count.
      end
      def query_profiler; @profiler || DummyProfiler; end
      def profile_queries?; query_profiler.nil?; end

      def redis_prefix (app_name=nil)
        if @redis_prefix.nil? || !app_name.nil?
          @redis_prefix = "#{Queris.redis_prefix(app_name)}#{self.name}:"
        else
          @redis_prefix
        end
      end
      def query(arg={}, &block)
        redis_query arg, &block
      end
      def redis_query(arg={})
        query = Queris::Query.new self, arg
      end

      def redis
        Queris.redis
      end
      def build_redis_indices(build_foreign = true, indices=nil)
        start_time = Time.now
        all = self.find_all
        fetch_time = Time.now - start_time
        redis_start_time, printy, total =Time.now, 0, all.count - 1
        index_keys = []
        indices ||= redis_indices
        print "\rDeleting existing indices..."
        indices.each do |index|
          index_keys.concat(redis.keys index.key "*", nil, true) #BUG - race condition may prevent all index values from being deleted
        end
        redis.multi do |redis|
          redis.del *index_keys
          all.each_with_index do |row, i|
            if printy == i
              print "\rBuilding redis indices... #{((i.to_f/total) * 100).round.to_i}%" unless total == 0
              printy += (total * 0.05).round
            end
            row.create_redis_indices indices
          end
          print "\rBuilding redis indices... ok. Committing to redis..."
        end
        print "\rBuilt redis indices for #{total} rows in #{(Time.now - redis_start_time).round 3} sec. (#{fetch_time.round 3} sec. to fetch all data).\r\n"
        #update all foreign indices
        foreign = 0
        indices.each do |index|
          if index.kind_of? Queris::ForeignIndex
            foreign+=1
            index.real_index.model.send :build_redis_indices 
          end
        end if build_foreign
        puts "Built #{indices.count} ind#{indices.count == 1 ? "ex" : "ices"} (#{build_foreign ? foreign : 'skipped'} foreign) for #{self.name} in #{(Time.now - start_time).round(3)} seconds."
        self
      end
      def build_redis_index(index_name)
        build_redis_indices(true, [ redis_index(index_name) ])
      end

      def add_redis_index(index, opt={})
        raise "Not an index" unless index.kind_of? Index
        if (found = redis_index_hash[index.name])
          #todo: same-name indices are allowed, but with some caveats.
          #define and check them here.
          #raise "Index #{index.name} (#{found.class.name}) already exists. Trying to add #{index.class.name}"
        end
        
        redis_indices.push index
        redis_index_hash[index.name.to_sym]=index
        redis_index_hash[index.class]=index
        index
      end
      
      private
      def redis_index_hash
        @redis_index_hash ||= superclass.respond_to?(:redis_index_hash) ? superclass.redis_index_hash.clone : {}
      end

      def profile_queries(another_profiler=nil)
        require "queris/profiler"
        @profiler = case another_profiler
        when :lite
          Queris::QueryProfilerLite
        else
          Queris::QueryProfiler
        end
      end
      def index_attribute(arg={}, &block)
        if arg.kind_of? Symbol 
          arg = {:attribute => arg }
        end
        index_class = arg[:index] || Queris::SearchIndex
        raise ArgumentError, "index argument must be in Queris::Index if given" unless index_class <= Queris::Index
        Queris.register_model self
        index_class.new(arg.merge(:model => self), &block)
      end
      def index_attribute_for(arg)
        raise ArgumentError ,"index_attribute_for requires :model argument" unless arg[:model]
        index = index_attribute(arg) do |index|
          index.name = "foreign_index_#{index.name}".to_sym
        end
        arg[:model].send :index_attribute, arg.merge(:index=> Queris::ForeignIndex, :real_index => index)
      end
      def index_attribute_from(arg) 
        model = arg[:model]
        model.send(:include, Queris) unless model.include? Queris
        model.send(:index_attribute_for, arg.merge(:model => self))
      end
      def index_range_attribute(arg)
        if arg.kind_of? Symbol 
          arg = {:attribute => arg }
        end
        index_attribute arg.merge :index => Queris::RangeIndex
      end
      def index_range_attributes(*arg)
        base_param = arg.last.kind_of?(Hash) ? arg.pop : {}
        arg.each do |attr|
          index_range_attribute base_param.merge :attribute => attr
        end
      end
      alias index_sort_attribute index_range_attribute
      def index_attributes(*arg)
        base_param = arg.last.kind_of?(Hash) ? arg.pop : {}
        arg.each do |attr|
          index_attribute base_param.merge :attribute => attr
        end
        self
      end
      def cache_attribute(attribute_name)
        Queris.register_model self
        Queris::HashCache.new :model => self, :attribute => attribute_name
      end
      def cache_all_attributes
        Queris.register_model self
        Queris::HashCache.new :model => self
      end
      alias :cache_object :cache_all_attributes
      def cache_attribute_from(arg)
        arg[:index]=Queris::HashCache
        index_attribute_from arg
      end
    end

    def get_cached_attribute(attr_name)
      redis_index(attr_name, Queris::HashCache).fetch id
    end
    
    #in lieu of code cleanup, instance method pollution
    %w(redis_index redis_indices query_profiler profile_queries redis).each do |method_name|
      define_method method_name do |*arg|
        self.class.send method_name, *arg
      end
    end
    
    %w(create update delete).each do |op|
      define_method "#{op}_redis_indices" do |indices=nil, redis=nil|
        (indices || self.class.redis_indices).each do |index|
          index.send op, self unless index.respond_to?("skip_#{op}?") and index.send("skip_#{op}?")
        end
      end
    end
    
  end
end
