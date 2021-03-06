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
      def redis_index(index_match=nil, index_class = Index, strict=true)
        raise ArgumentError, "#{index_class} must be a subclass of Queris::Index" unless index_class <= Index
        case index_match
        when index_class, NilClass, Query
          return index_match
        when String
          index_match = index_match.to_sym
        end
        index = redis_index_hash[index_match]
        if strict
          raise SchemaError, "Index #{index_match} not found in #{name}" unless index
          raise SchemaError, "Found wrong index class: expected #{index_class.name}, found #{index.class.name}" unless index.kind_of? index_class
        end
        index
      end

      #get all redis indices
      #options:
      # :foreign => true - look in foreign indices (other models' indices indexing from this model)
      # :live => true - look in live indices
      # ONLY ONE of the following will be respected
      # :except => [index_name, ...] - exclude these
      # :attributes => [...] - indices matching any of the given attribute names
      # :names => [...] - indices with any of the given names
      # :class => Queris::IndexClass - indices that are descendants of given class
      def redis_indices(opt={})
        unless Hash === opt
          tmp, opt = opt, {}
          opt[tmp]=true
        end
        if opt[:foreign]
          indices = @foreign_redis_indices || []
        else
          indices = @redis_indices || (superclass.respond_to?(:redis_indices) ? superclass.redis_indices.clone : [])
        end
        if !opt[:attributes].nil?
          attrs = opt[:attributes].map{|v| v.to_sym}.to_set
          indices.select { |index| attrs.member? index.attribute }
        elsif opt[:except]
          except = Array === opt[:except] ? opt[:except] : [ opt[:except] ]
          indices.select { |index| !except.member? index.name }
        elsif opt[:live]
          indices.select { |index| index.live? }
        elsif !opt[:names].nil?
          names = opt[:names].map{|v| v.to_sym}.to_set
          indices.select { |index| names.member? index.name }
        elsif !opt[:class].nil?
          indices.select { |index| opt[:class] === index }
        else
          indices
        end
        #BUG: redis_indices is very static. superclass modifications after class declaration will not count.
      end
      def live_index?(index)
        redis_indices(:live).member? redis_index(index)
      end
      def foreign_index?(index)
        redis_indices(:foreign).member? redis_index(index)
      end
        
      def query_profiler; @profiler || DummyProfiler; end
      def profile_queries?; query_profiler.nil?; end


      def query(arg={}, &block)
        redis_query arg, &block
      end

      def page_size(n=nil)
        if n.nil?
          @page_size || 1000
        else
          @page_size=n
        end
      end
      
      def info(arg={})
        puts "#{self.name} model info:"
        redis_indices.each do |i|
          next if arg[:live] && !i.live?
          next if arg[:class] && !(arg[:class] === i)
          puts "  #{i.info}"
        end
        querykeys = arg[:query_keys] || ((redis || Queris.redis).keys(query.results_key(nil, "*")))
        puts "  #{querykeys.count} query-related keys"
      end

      def clear_queries!
        q = query
        querykeys = redis.keys "#{q.redis_prefix}*"
        #old keys, too
        querykeys.concat redis.keys("#{self.prefix}#{q.class.name}*")
        querykeys.uniq!
        print "Deleting #{querykeys.count} query keys for #{name}..."
        redis.multi do |r|
          querykeys.each {|k| r.del k}
        end
        puts "ok"
        querykeys.count
      end
      def clear_cache!
        indices = redis_indices(:class => HashCache)
        total = 0
        indices.each do |i|
          keymatch = i.key("*", nil, true)
          allkeys = redis.keys(keymatch)
          print "Clearing #{allkeys.count} cache keys for #{name} ..."
          redis.multi do |r|
            allkeys.each { |key| r.del key }
          end
          total += allkeys.count
          puts "ok"
        end
        total
      end
      def redis_query(arg={})
        Queris::Query.new self, arg
      end

      def redis
        Queris.redis
      end
      
      def build_missing_redis_indices
        missing_indices = redis_indices.select do |index|
          
          unless index.skip_create?
            found = false
            cursor = "0"
            loop do
              cursor, res = redis.scan [cursor, "match", index.keypattern]
              if res.count > 0
                found = true
                break
              end
              break if cursor == "0"
            end
            
            if not found then
              puts "index #{name}:#{index.name} missing (#{index.keypattern})"
              true
            else
              false
            end
          end
        end
        if missing_indices .count > 0
          build_redis_indices missing_indices 
        else 
          puts "No missing indices for model #{name}."
        end
      end
      
      def build_redis_indices(indices=nil, build_foreign = true, incremental_delete=false)
        indices ||= redis_indices
        foreign_indices = []
        indices.select! do |index|
          if index.skip_create?
            false
          elsif Queris::ForeignIndex === index
            foreign_indices << index
            false
          else
            true
          end
        end
        start_time = Time.now
        if indices.count > 0
          print "Loading #{name} data..."
          all = self.find_all
          fetch_time = Time.now - start_time
          puts "\rLoaded #{all.count} entries for #{name} (#{fetch_time.to_f.round} sec)"
          redis_start_time, printy, total =Time.now, 0, all.count - 1
          index_keys = []

          print "\rDeleting existing indices..." unless incremental_delete
          indices.each {|index| index_keys.concat(redis.keys index.keypattern)} #BUG - race condition may prevent all index values from being deleted
          no_indexing_pipeline do #prevent create_redis_index from running in its own personal multi
            unless incremental_delete
              redis.multi 
              index_keys.each { |k| redis.del k }
            end
            next_print, last_i, gap=Time.now, 0, 2
            all.each_with_index do |row, i|
              now = Time.now
              if next_print <= now
                rate = (i - last_i)/(now.to_f - (next_print-gap).to_f)
                last_i=i
                print "\rBuilding redis indices... #{((i.to_f/total) * 100).round.to_i}% (#{i}/#{all.count}) (#{rate.round}/sec)" unless total == 0
                next_print = now + gap
              end
              if incremental_delete
                redis.multi
                row.eliminate_redis_indices indices
              end
              row.create_redis_indices indices
              redis.exec if incremental_delete
            end
            print "\rBuilding redis indices... ok. Committing to redis..."
            redis.exec unless incremental_delete
            print "\rBuilt #{name} redis indices for #{total} rows in #{(Time.now - redis_start_time).round 3} sec. (#{fetch_time.round 3} sec. to fetch all data).\r\n"
          end
        end
        #update all foreign indices
        foreign_by_model={}
        if build_foreign
          foreign_indices.each do |index|
            m = index.real_index.model
            foreign_by_model[m] ||= []
            foreign_by_model[m] << index.real_index
          end
          foreign_by_model.each { |model, indices| model.build_redis_indices indices, true, incremental_delete }
        end
        puts "Built #{indices.count} ind#{indices.count == 1 ? "ex" : "ices"} (#{build_foreign ? foreign_indices.count : 'skipped'} foreign) for #{self.name} in #{(Time.now - start_time).round(3)} seconds."
        self
      end
      def build_redis_index(index_name, incremental_delete=false)
        build_redis_indices [ redis_index(index_name) ], true, incremental_delete
      end

      def before_query(&block)
        @before_query_callbacks ||= []
        @before_query_callbacks << block
      end
      def before_query_flush(&block)
        @before_query_flush_callbacks ||= []
        @before_query_flush_callbacks << block
      end
      
      def run_query_callbacks(which_ones, query)
        callbacks = case which_ones
        when :before, :before_run, :run
          @before_query_callbacks
        when :before_flush, :flush
          @before_query_flush_callbacks
        end
        if callbacks
          callbacks.each {|block| block.call(query)}
          callbacks.count
        else
          0
        end
      end
      
      def add_redis_index(index, opt={})
        raise SchemaError, "Not an index" unless index.kind_of? Index
        #if (found = redis_index_hash[index.name])
          #todo: same-name indices are allowed, but with some caveats.
          #define and check them here.
          #raise "Index #{index.name} (#{found.class.name}) already exists. Trying to add #{index.class.name}"
        #end
        @redis_indices ||= []
        redis_indices.push index
        redis_index_hash[index.name.to_sym]=index
        redis_index_hash[index.class]=index
        index
      end
      def live_queries?
        @live_redis_indices && @live_redis_indices.count
      end
      
      def no_indexing_pipeline
        if block_given?
          @no_indexing_pipeline = true
          res = yield
          @no_indexing_pipeline = nil
          return res
        else
          @no_indexing_pipeline
        end
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
      attr_accessor :live_redis_indices

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
        index = index_attribute(arg) do |i|
          i.name = "foreign_index_#{i.name}".to_sym
        end

        foreigner = arg[:model].send :index_attribute, arg.merge(:index=> Queris::ForeignIndex, :real_index => index)
        @foreign_redis_indices ||= []
        @foreign_redis_indices.push foreigner
        foreigner
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
      def stored_in_redis?
        nil
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
    
    %w(create update delete eliminate).each do |op|
      define_method "#{op}_redis_indices" do |indices=nil, redis=nil|
        if self.class.no_indexing_pipeline ||  Redis::Pipeline === Queris.redis.client 
          (indices || self.class.redis_indices).each do |index|
            index.send op, self unless index.respond_to?("skip_#{op}?") and index.send("skip_#{op}?")
          end
        else
          Queris.redis.multi do #hacky, might bug out on weird configs
            (indices || self.class.redis_indices).each do |index|
              index.send op, self unless index.respond_to?("skip_#{op}?") and index.send("skip_#{op}?")
            end
          end
        end
      end
    end
    
  end
end
