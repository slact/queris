module Queris
  #This is a suboptimal profiler implementation, as it requires current profile data to be loaded before it can be updated.
  class Profiler < Queris::Model
    decaying_attrs =[:cache_hit, 
        :cache_miss, 
        :run_time,
        :results_time, 
        :results_count,
        :times_run ]
    attrs *decaying_attrs
    redis Queris.redis(:profiling, :master) 
    index_only
    index_attributes *decaying_attrs, :index => Queris::DecayingAccumulatorIndex, :half_life => 604800
    
    def self.find(query)
      #inefficient, as it's supposed to be. Would be better if HINCRBY could do floats
      #NOTE: Redis 2.6 will have HINCRBYFLOATs
      found = new query_profile_id(query)
      found.load query
    end
      
    def load(query=nil) #load from sorted sets through a pipeline
      indices = self.class.redis_indices
      res = (redis || query.model.redis).multi do |r|
        indices.each do |index|
          r.zscore index.sorted_set_key, id
        end
      end
      indices.each do |index|
        unless (n_0 = res.shift).nil?
          n_t = n_0.to_f * 2 ** (-index.t(Time.now.to_f)/index.half_life)
          self.import index.name => n_t
        end
      end
      self
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
    
    def initialize(*arg)
      @time_start={}
      super *arg
    end
    
    def record(attr, val)
      send "#{attr}=", val
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
      if attr == :run_time
        record :times_run, 1
        self.times_run_was= 0 # force update of n
      end
    end
  end
end
