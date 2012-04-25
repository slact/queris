module Queris
  #This is a suboptimal profiler implementation, as it requires current profile data to be loaded before it can be updated.
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
            self.class.stats.each do |_, statistic|
              increment sample_name, val.to_f
            end
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
            DecayingSum.new(opt[:half_life] || opt[:hl] || 1209600)
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
      super
    end
    
    def load
      loaded = redis.hgetall(hash_key)
      loaded.each { |key, val| loaded[key]=val.to_f }
      super loaded
    end
    
    def record(attr, val)
      send "#{attr}=", val
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
      puts "started #{attr}"#, caller
    end
    def finish(attr)
      start_time = @time_start[attr]
      raise "Query Profiling timing attribute #{attr} was never started." if start_time.nil?
      record attr, (Time.now.to_f - start_time)
      puts "finished #{attr}"#, caller
    end
  end
  
  class QueryProfiler < Profiler
    sample :cache_hit, :cache_miss
    sample :run_time, :results_time, unit: :msec

    average :geometric, :name => :avg
    average :decaying, :name => :ema
    
    def self.find(query, opt={})
      if redis.nil?
        opt[:redis]=query.model.redis
      end
      super query, opt
    end
    
    def self.query_profile_id(query)
      "#{query.model.name}:#{query.structure}"
    end
    def query_profile_id(query)
      self.class.query_profile_id(query)
    end
    def set_profile_id(query)
      set_id query_profile_id(query), true
    end
  end
end
