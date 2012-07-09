module Queris
  class Profiler < Queris::Model
    
    class Sum
      attr_reader :key
      def self.index
        RangeIndex
      end
      
      def initialize(*arg)
        @key = "#{self.class.name.split('::').last}"
      end
      def hkey(attr)
        "#{attr}:#{@key}"
      end
      alias :attribute_name :hkey
      
      def current_value(val)
        val
      end
      def increment_value(val)
        val
      end
      
      def average(val, num_samples)
        current_value(val)/current_value(num_samples)
      end
    end
    
    class DecayingSum < Sum
      attr_reader :half_life
      def initialize(halflife=1209600) # 2 weeks default
        super
        @half_life = (halflife).to_f
        @key << ":half-life=#{@half_life}"
      end

      def current_value(val)
        decay val
      end

      def increment_value(val)
        grow val #rewind in time
      end

      private
      TIME_OFFSET=Time.new(2020,1,1).to_f
      def t(seconds)
        seconds - TIME_OFFSET
      end
      def exp_func(val, sign=1, time=Time.now.to_f)
        val * 2.0 **(sign * t(time)/@half_life)
      end
      def grow(val)
        exp_func val
      end
      def decay(val)
        exp_func val, -1
      end
    end
 
    attr_reader :last_sample
    
    class << self
      def samples(*attrs)
        @samples ||= {}
        return @samples if attrs.empty?
        opt = attrs.last.kind_of?(Hash) ? attrs.pop : {}
        attrs.each do |sample_name|
          sample_name = sample_name.to_sym #just in case?...
          @samples[sample_name] = opt[:unit] || opt[:units] || ""
          stats.each { |_, statistic| initialize_sample_average sample_name, statistic }
          
          define_method "#{sample_name}=" do |val|
            val = val.to_f
            self.class.stats.each do |_, statistic|
              increment sample_name, val
            end
            if @sample_saved
              @last_sample.clear
              @sample_saved = nil
            end
            @last_sample[sample_name]=val
          end
          
          define_method sample_name do |stat_name|
            if (statistic = self.class.named_stat(stat_name.to_sym))
              statistic.average send(statistic.hkey(sample_name)), send(statistic.hkey(:n)) 
            end
          end
        end
      end
      alias :sample :samples
      
      def unit(unit)
        samples[:n]=unit
      end
      
      #getter
      def stats
        @stats ||= {}
      end
      
      def average(method=:geometric, opt={}){}
        @named_stats ||= {}
        sample :n if samples[:n].nil?
        method = method.to_sym
        stat_name = opt[:name]
        stat = case method
          when :geometric, :arithmetic, :mean
            Sum.new
          when :decaying, :exponential
            DecayingSum.new(opt[:half_life] || opt[:hl] || opt[:halflife] || 1209600)
          else
            raise ArgumentError, "Average weighing must be one of [ geometric, decaying ]"
          end
        if @stats[stat.key].nil?
          @stats[stat.key] = stat
          raise ArgumentError, "Different statistic with same name already declared." unless @named_stats[stat_name].nil?
        end
        if @named_stats[stat_name].nil?
          @named_stats[stat_name] = @stats[stat.key]
        end
        samples.each do |sample_name, unit|
          initialize_sample_statistic sample_name, stat
        end
      end

      def default_statistic(stat_name)
        #TODO
      end
      def named_stat(name)
        @named_stats[name]
      end
      private
      def initialize_sample_statistic(sample_name, statistic)
        @initialized_samples ||= {}
        key = statistic.hkey sample_name
        if @initialized_samples[key].nil?
          attribute key
          index_range_attribute :attribute => key
          @initialized_samples[key] = true
        end
      end
    end

    redis Queris.redis(:profiling, :sampling, :master)
      
    def initialize(*arg)
      @time_start={}
      @last_sample={}
      super *arg
    end
    
    def samples
      self.class.samples.keys
    end
    def sample_unit(sample_name, default=nil)
      self.class.samples[sample_name] || default
    end
    
    def save(*arg)
      increment :n, 1 unless self.class.sample[:n].nil?
      if (result = super)
        @sample_saved = true
      end
      result
    end
    
    def load
      loaded = redis.hgetall(hash_key)
      loaded.each { |key, val| loaded[key]=val.to_f }
      super loaded
    end
    
    def record(attr, val)
      if respond_to? "#{attr}="
        send("#{attr}=", val)
      else
        raise "Attempting to time an undeclared sampling attribute #{attr}"
      end
      self
    end
    
    def attribute_diff(attr)
      super(attr) || 0
    end
    
    def increment(sample_name, delta_val)
      self.class.stats.each do |_, statistic|
        super statistic.hkey(sample_name), statistic.increment_value(delta_val)
      end
      self
    end
    
    def start(attr)
      @time_start[attr]=Time.now.to_f
    end
    def finish(attr)
      start_time = @time_start[attr]
      raise "Query Profiling timing attribute #{attr} was never started." if start_time.nil?
      t = Time.now.to_f - start_time
      @time_start[attr]=nil

      record attr, (Time.now.to_f - start_time)
    end
    
  end
  
  class QueryProfilerBase < Profiler
    def self.find(query, opt={})
      if redis.nil?
        opt[:redis]=query.model.redis
      end
      super query_profile_id(query), opt
    end
    
    def self.query_profile_id(query)
      "#{query.model.name}:#{query.structure}"
    end
    def set_id(query, *rest)
      @query = query
      set_id self.class.query_profile_id(query, *rest), true
    end
  end
  
  class QueryProfiler < QueryProfilerBase
    sample :cache_miss
    sample :time, :own_time, unit: :msec

    average :geometric, :name => :avg
    average :decaying, :name => :ema
    
    def self.profile(query, opt={})
      tab = "  "
      unless query.subqueries.empty?
        query.explain :terse_subquery_ids => true
      end
    end
    
    
    def profile
      self.cass.profile @query
    end
  end
  
  # does not store properties in a hash, and writes only to indices.
  # useful for Redises with crappy or no HINCRBYFLOAT implementations (<=2.6)
  # as a result, loading stats is suboptimal, but still O(1)
  class QueryProfilerLite < QueryProfilerBase
    sample :cache_miss
    sample :time, :own_time, unit: :msec

    average :decaying, :name => :ema, :half_life => 2629743 #1 month

    index_only

    def load(query=nil) #load from sorted sets through a pipeline
      stats = self.class.stats
      #The following code SHOULD, in some future, load attributes
      # through self.class.stats. For now loading all zsets will do.
      res = (redis || query.model.redis).multi do |r|
        stats.each do |_, statistic|
          r.zscore index.sorted_set_key, id
        end
      end
      indices.each do |index|
        unless (val = res.shift).nil?
          self.import index.name => val.to_f
        end
      end
      self
    end
  end
end
