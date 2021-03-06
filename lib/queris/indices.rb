require "securerandom"
module Queris
  # These here are various queries 'indices'. All indices must subclass 
  # Queris::Index. Some are straight-up indices, mapping values 
  # (hashed when possible) to object ids.
  # others perform caching functions, yet others maintain some state 
  # (and cannot be rebuilt)
  
  class Index
    
    class Error < StandardError
    end
    
    attr_accessor :name, :redis, :model, :attribute, :live, :delta_ttl
    #live queries is implemented through a time-indexed sorted set of changed objects as they relate to a given live index.
    alias :live? :live
    DELTA_TTL = 172800 #max time to keep old live query changeset elements around
    def initialize(arg={})
      arg.each do |opt, val|
        instance_variable_set "@#{opt}".to_sym, val
      end
      @redis = arg[:redis]
      @name ||= @attribute #user-facing index name
      @attribute ||= @name #attribute or method, as long as it exists
      @attribute = @attribute.to_sym unless !@attribute
      @name = @name.to_sym
      @key ||= :id #object's key attribute (default is 'id') used to generate redis key
      @keyf ||= "%s#{self.class.name.sub(/^.*::/, "")}:#{@name}=%s"
      @delta_ttl = (arg[:delta_ttl] || arg[:delta_element_ttl] || arg[:changeset_ttl] || self.class::DELTA_TTL).to_i
      live_delta_key
      if block_given?
        yield self, arg
      end
      raise ArgumentError, "Index must have a name" unless @name
      raise ArgumentError, "Index must have a model" unless @model
      @model.add_redis_index self
    end
    def key_attr #object's key (usually 'id')
      @key
    end

    def poke(rds=nil, arg=nil)
      self
    end
    
    #needed for server-side query storage
    def json_redis_dump(hash={})
      hash
    end
    def redis(obj=nil)
      r=@redis || @model.redis || Queris.redis(:index, :slave, :master) || (!obj.nil? && obj.redis)
      raise ClientError, "No redis connection found for Queris Index #{name}" unless r
      r
    end

    #MAINTENANCE OPERATION -- DO NOT USE IN PRODUCTION CODE
    #get all keys associated with an index
    def keys
      if keypattern
        mykeys = (redis || model.redis).keys keypattern
        mykeys << live_delta_key if live
        mykeys
      else
        []
      end
    end
    def keypattern
      @keypattern ||= key('*', nil, true) if respond_to? :key
    end
    
    def temp_keys(val) #any temporary keys for index at given value?
      []
    end
    
    #info about data distribution in an index
    def distribution
      k = keys
      counts = (redis || model.redis).multi do |r|
        k.each do |thiskey|
          r.evalsha Queris.script_hash(:multisize), [thiskey]
        end
      end
      Hash[k.zip counts]
    end
    def info
      keycounts = distribution.values
      ret="#{name}: #{keycounts.reduce(0){|a,b| a+b if Numeric === a && Numeric === b}} ids in #{keycounts.count} redis keys."
      if live?
        #get delta set size
        delta_size = (redis || model.redis).zcard live_delta_key
        ret << " |live delta set|=#{delta_size}"
        if delta_size > 0
          last_el = (redis || model.redis).zrevrange(live_delta_key, 0, 0, :with_scores => true).first
          ret << " updated at: #{last_el.last}"
        end
      end
      ret
    end

    def exists?; keys.count > 0 ? keys.count : nil; end
    def erase!
      mykeys = keys
      model.redis.multi do |r|
        mykeys.each {|k| r.del k}
      end
      mykeys.count
    end
    def self.skip_create?; false; end
    def skip_create?
      @skip_create || self.class.skip_create?
    end
    def skip_update?
      @skip_update
    end
    def skip_delete?
      @skip_delete
    end
    #is it possible to update this index incrementally?
    def incremental?
      false
    end
    def stateless?
      true
    end
    def usable_as_results?(val=nil)
      not (Enumerable === val)
    end
    #can the index correctly be ranged over many values for q query?
    def handle_range?
      false
    end
    #processed index value
    def val(value)
      @value.nil? ? value : @value.call(value)
    end
    #processed indexed value, applied only when indexing objects and never applied to query values
    def index_val(value, obj=nil)
      (@index_value || @value).nil? ? value : (@index_value || @value).call(value, obj)
    end
    def digest(value)
      Queris.digest(value.to_s)
    end
    def value_is(obj)
      obj.send @attribute
    end
    def value_was(obj)
      msg = "#{@attribute}_was"
      obj.send msg if obj.respond_to? msg
    end
    def value_diff(obj)
      obj.attribute_diff @attribute if obj.respond_to? :attribute_diff
    end
    def live_delta_key
      @live_delta_key||="#{@redis_prefix||@model.prefix}#{self.class.name.split('::').last}:#{@name}:live_delta"
    end
    def no_live_update
      @skip_live_update = true
      ret = yield
      @skip_live_update = nil
      ret
    end
    def update_live_delta(obj, r=nil)
      r ||= redis
      if live? && !@skip_live_update
        okey=obj.send @key
        r.zadd live_delta_key, Time.now.utc.to_f, obj.send(@key)
        r.expire live_delta_key, @delta_ttl
        Queris.run_script :periodic_zremrangebyscore, r, [live_delta_key], [(@delta_ttl/2), '-inf', (Time.now.utc.to_f - @delta_ttl)]
      end
    end
    def update(obj)
      val_is, val_was = value_is(obj), value_was(obj)
      if(val_is != val_was)
        no_live_update do
          remove(obj, val_was) unless val_was.nil?
          add(obj) unless val_is.nil?
        end
        update_live_delta obj
      end
    end
    def create(obj)
      add(obj)
    end
    def delete(obj)
      remove(obj, value_was(obj))
    end
    #remove from all possible index keys, instead of relying on current value. uses KEYS command, is slow.
    def eliminate(obj)
      redis.evalsha Queris.script_hash(:remove_from_keyspace), [], [obj.send(@key), keypattern, 480]
    end
  end
  
  class HashCache < Index
    #maintains a cached copy of an object with separetely marshaled attributes in redis
    def initialize(arg={})
      @name= "#{arg[:attribute] || "all_attribute"}_hashcache"
      super arg
      @attribute= arg[:attribute]
      raise ClientError, "Model not passed to index." unless @model
      @name=@model.to_s #whatever, name's not important.
    end
    
    #don't add this index to the list of indices to be built when calling Queris.rebuild!
    def self.skip_create?; true; end
    
    def hash_key(obj, prefix=nil, raw_val=false)
      if raw_val
        id = obj
      else
        id = obj.kind_of?(@model) ? obj.send(@key) : obj
      end
      (@keyf) %[prefix || @redis_prefix || @model.prefix, id]
    end
    alias :key :hash_key
    def update(obj)
      changed_attrs = obj.changed_cacheable_attributes
      if @attribute.nil?
        cache_attributes obj, changed_attrs unless changed_attrs.length == 0
      elsif changed_attrs.member? @attribute
        cache_attributes obj, @attribute => send(@attribute)
      end
    end
    def create(obj)
      if @attriute.nil?
        cache_attributes obj, obj.all_cacheable_attributes
      elsif not obj.call(@attribute).nil?
        cache_attributes obj, @attribute => send(@attribute)
      end
      
    end
    
    def delete(obj)
      redis(obj).del hash_key obj
    end
    
    def fetch(id, opt={})
      if @attribute.nil?
        hash = (opt[:redis] || Queris.redis(:slave, :master)).hgetall hash_key id

        loaded = load_cached hash
        if hash && loaded.nil? #cache data invalid. delete.
          Queris.redis(:master).del(hash_key id)
        end
        loaded
      else
        return (opt[:redis] || Queris.redis(:slave, :master)).hget hash_key(id), @attribute
      end
    end
    
    def load_cached(marshaled_hash)
      @cached_attr_count ||= (not @attribute.nil?) ? 1 : @model.new.all_cacheable_attributes.length #this line could be a problem if more cacheable attributes are added after the first fetch.
      if marshaled_hash.length >= @cached_attr_count
        unmarshaled = {}
        marshaled_hash.each_with_index do |v|
          unmarshaled[v.first.to_sym]=Marshal.load v.last
        end
        obj= @model.new
        begin
          obj.assign_attributes(unmarshaled, :without_protection => true)
        rescue Exception => e
          #cache load failed because the data was invalid.
          return nil
        end
        obj.instance_eval do
          @new_record= false 
          @changed_attributes={}
        end
        obj
      else
        nil
      end
    end
    
    alias :load :fetch
      
    def info
      keycounts = distribution.values
      "HashCache: #{keycounts.count} objects cached."
    end
    
    private 
    def cache_attributes(obj, attrs)
      key = hash_key obj
      marshaled = {}
      attrs.each do |v|
        marshaled[v]=Marshal.dump obj.send(v)
      end
      redis.mapped_hmset key, marshaled
    end
  end

  class SearchIndex < Index
    #basic set index
    def initialize(arg={})
      super arg
      @type ||= "string"
      raise ClientError, "Model not passed to index." unless @model
    end
    
    def set_key(value, prefix=nil, raw_val=false)
      if Enumerable === value
        value.map { |val| set_key val, prefix, raw_val }
      else
        (@keyf) %[prefix || @redis_prefix || @model.prefix, raw_val ? value : digest(val value)]
      end
    end
    alias :key :set_key
    alias :key_for_query :key
    def add(obj, value = nil)
      value = index_val( value || obj.send(@attribute), obj)
      #obj_id = obj.send(@key)
      #raise "val too short" if !obj_id || (obj.respond_to?(:empty?) && obj.empty?)
      if value.kind_of?(Enumerable)
        value.each{|val| redis(obj).sadd set_key(val), obj.send(@key)}
      else
        redis(obj).sadd set_key(value), obj.send(@key)
      end
      update_live_delta obj
      #redis(obj).eval "redis.log(redis.LOG_WARNING, 'added #{obj.id} to #{name} at #{value}, key #{key(value)}')"
    end
    def remove(obj, value = nil)
      value = index_val( value || obj.send(@attribute), obj)
      (value.kind_of?(Enumerable) ? value : [ value ]).each do |val|
        redis(obj).srem set_key(val.nil? ? obj.send(@attribute) : val), obj.send(@key)
        #redis(obj).eval "redis.log(redis.LOG_WARNING, 'removed #{obj.id} from #{name} at #{value},  key #{set_key(val.nil? ? obj.send(@attribute) : val)}')"
      end
      update_live_delta obj
    end
    def key_size(redis_key, r=nil)
      (r || redis).scard redis_key
    end
  end

  class ForeignIndex < SearchIndex
    #this foreign index wrapper has caused some problems in the past.
    #it's annoying to work with and should be retired in favor of a transparent
    #foreign index proxy object
    attr_accessor :real_index
    def initialize(arg)
      raise ArgumentError, "Missing required initialization attribute real_index for ForeignIndex." unless arg[:real_index]
      super arg
    end
    def create(*a) end
    alias :delete :create
    alias :update :create
    alias :eliminate :create
    %w(set_key key key_for_query live_delta_key skip_create? exists? keys update_live_delta key_size erase!).each do |methname|
      define_method methname do |*arg|
        @real_index.send methname, *arg
      end
    end
    def foreign_id(obj)
      obj.send(@key)
    end
  end
  
  class PresenceIndex < SearchIndex
    def initialize(arg)
      super arg
      @counter_keyf = "#{@model.prefix}#{self.class.name.sub(/^.*::/, "")}:#{@name}:#{@attribute}=%s:counter"
      @attribute = @key
      @threshold ||= 1
    end
    def digest(*arg)
      "present"
    end
    def counter_key(obj, val=nil)
      @counter_keyf % (val || value_is(obj))
    end
    def add(obj, value=nil)
      k = redis(obj).incr counter_key(obj)
      if k == @threshold
        super obj
      end
    end
    def remove(obj, value=nil)
      ckey = counter_key obj
      r = redis(obj)
      r.decr ckey
      if r.get(ckey).to_i. < @threshold
        r.del ckey
        super obj
      end
    end
  end
  
  # The power, and occasional awkwardness, of sorted sets
  class RangeIndex < SearchIndex
    def initialize(arg)
      @value ||= proc { |x| x.to_f }
      super arg
    end
    def val(val=nil, obj=nil)
      @value.call val, obj
    end
    def sorted_set_key(val=nil, prefix=nil, raw_val=false)
      @keyf %[prefix || @model.prefix, "(...)"]
    end
    alias :key :sorted_set_key
    def key_for_query(val=nil)
      if val.nil? 
        key
      else
        "#{key}:rangehack:#{val.to_s}"
      end
    end
    
    def rangehack_set_key
      "#{sorted_set_key}:rangehacks"
    end
    
    def update(obj)
      if !(diff = value_diff(obj)).nil?
        increment(obj, diff) unless diff == 0
      else
        val_is, val_was = value_is(obj), value_was(obj)
        add(obj, val_is) unless val_is == val_was
        #removal is implicit with the way we're using sorted sets
      end
      #redis(obj).eval "redis.log(redis.LOG_WARNING, 'updated #{obj.id} for #{name}')"
    end
    
    def incremental?; true; end
    def handle_range?; true; end
    def usable_as_results?(val)
      val.nil?
    end
    
    def zcommand(cmd, obj, value)
      id= obj.send(@key)
      my_val= val(value || value_is(obj), obj)
      if cmd==:zrem
        redis(obj).send cmd, sorted_set_key, id
      else
        redis(obj).send cmd, sorted_set_key, my_val, id
      end
      update_rangehacks cmd, id, my_val
      update_live_delta obj
    end
    
    def add(obj, value=nil)
      zcommand :zadd, obj, value
    end
    
    def increment(obj, value=nil)
      zcommand :zincrby, obj, value
    end

    def remove(obj, value=nil)
      zcommand :zrem, obj, value
    end
    
    #double hack
    def update_rangehacks(action, id, val=nil)
      case action
      when :zrem
        action=:del
      when :zincrby
        action=:incr
      when :zadd
        action=:add
      end
      Queris.run_script :update_rangehacks, redis, [rangehack_set_key, sorted_set_key], [action, id, val]
    end
    
    #there's no ZRANGESTORE, so we need to extract the desired range
    #to a temporary zset first
    attr_accessor :rangehack_keys
    def ensure_rangehack_exists(redis, val, query)
      #copy to temp key if needed
      rangehack_keys||={}
      unless val.nil?
        rangehack_key = key_for_query val
        return if rangehack_keys[rangehack_key]
        val = (val...val) unless Enumerable === val
        Queris.run_script :make_rangehack_if_needed, redis, [rangehack_key, key, rangehack_set_key, query.runstate_key(:ready)], [self.val(val.begin), self.val(val.end), val.exclude_end?]
        #can be spiky-shit slow if whole zset must be copied
        query.add_temp_key(rangehack_key)
      end
    end
    def rangehack?(val)
      not val.nil?
    end
    def clear_rangehack_keys
      rangehack_keys={}
    end

    def temp_keys(val=nil)
      val.nil? ? [] : [ key_for_query(val) ]
    end
    def distribution_summary
      keycounts = distribution.values
      "#{name}: #{keycounts.reduce(0){|a,b| a+b if Numeric === a && Numeric === b}} ids in #{keycounts.count} redis key."
    end
    def key_size(redis_key, r=nil)
      (r || redis).zcard redis_key
    end
    private
    def remove_inverse_range(redis, key, val)
      first, last = val.begin.to_f, val.end.to_f
      range_end = "#{!val.exclude_end? ? '(' : nil}#{last}"   #)
      if (first <= last)
        redis.zremrangebyscore key, '-inf', "(#{first}" unless first == -Float::INFINITY   #)
        redis.zremrangebyscore key, range_end, 'inf' unless last == Float::INFINITY
      else
        redis.zremrangebyscore key, range_end, "(#{first}"        #)
      end
    end
  end

  class ScoredSearchIndex < RangeIndex
    def initialize(arg)
      @score_attr=arg[:score_attr] || arg[:score_attribute] || arg[:score]
      @score_val=arg[:score_val] || arg[:score_value]
      @lazy_score_update=arg[:lazy_score_update] || arg[:lazy_score] || arg[:lazy_update] || arg[:lazy]
      @value ||= proc{|x| x}
      raise Index::Error, "ScoredSearchIndex needs :score, :score_attr or :score_val parameter" if @score_attr.nil? && @score_val.nil?
      @score_val ||= proc {|x, obj| x.to_f}
      super
    end
    
    def update_rangehacks(*arg); end
    def handle_range?; false; end
    def ensure_rangehack_exists(*arg); end
    
    def usable_as_results?(val)
      true
    end
    
    def score_is(obj)
      score_attr_val=@score_attr.nil? ? nil : obj.send(@score_attr)
      @score_val.call(score_attr_val, obj)
    end
    
    def score_was(obj)
      if @score_attr.nil?
        raise Index::Error, "score_was impossile without a score_attr"
      else
        score_attr_val=@score_attr.nil? ? nil : obj.send("#{@score_attr}_was")
        @score_val.call(score_attr_val, obj)
      end
    end
      
    
    def zcommand(cmd, obj, value)
      id= obj.send(@key)
      if id.nil?
        if obj.respond_to? @key
          raise Index::Error, "nil key attribute (#{@key}) for #{self.class.name} of model #{obj.class}"
        else
          raise Index::Error, "missing key attribute (#{@key}) for #{self.class.name} of model #{obj.class}"
        end
      end
      my_val= val(value || value_is(obj), obj)
      if cmd==:zrem
        redis(obj).send cmd, sorted_set_key(my_val), id
      else
        redis(obj).send cmd, sorted_set_key(my_val), score_is(obj), id
      end
    end

    def sorted_set_key(val=nil, prefix=nil, raw_val=false)
      @keyf %[prefix || @model.prefix, val] #alternately, digest(val)
    end
    alias :key :sorted_set_key
    
    def key_for_query(val=nil)
      key val
    end
    
    def update_score(obj, val)
      zcommand :zadd, obj, val
    end
    
    def update(obj)
      val_is, val_was = val(value_is(obj)), val(value_was(obj))
      if val_is == val_was
        #should we update the score anyway? it's a O(log(N)) operation...
        if @score_attr
          sc_is, sc_was = score_is(obj), score_was(obj)
          if sc_is != sc_was
            update_score(obj, val_is)
          end
        elsif @lazy_score_update.nil?
          add(obj, val_is)
        end
      else
        remove(obj, val_was) unless val_was.nil?
        add(obj, val_is)
      end
    end
    
  end

  class ExpiringPresenceIndex < RangeIndex
    def initialize(arg={})
      raise ArgumentError, "Expiring Presence index must have its time-to-live (:ttl) set." unless arg[:ttl]
      novalue = !arg.key?(:attribute)
      arg[:value] = proc{|v,o| Time.now.utc.to_f} if novalue
      super arg
      @attribute = @key if novalue #don't care what attribute we use.
    end
    def json_redis_dump(hash={})
      hash[:index]=self.class.name.split('::').last
      hash[:nocompare]=true
      hash[:ttl]=@ttl
    end
    def incremental?; false; end
    def update(obj)
      add(obj)
    end
    def update_live_delta(*arg)
      self
    end
    def add(*arg)
      poke(nil, true) if live?
      super(*arg)
    end
    def key_for_query(val=nil)
      key
    end
    def ensure_rangehack_exists(*arg); end #nothing
    def usable_as_results?(val)
      false #because we always need to run stuff before query
    end

    def poke(rds=nil, schedule=false)
      rds ||= Queris.redis :master #this index costs a roundtrip to master 
      if live?
        Queris.run_script :update_live_expiring_presence_index, rds, [key, live_delta_key], [Time.now.utc.to_f, @ttl, schedule]
        Queris.run_script :periodic_zremrangebyscore, rds, [live_delta_key], [(@delta_ttl/2), '-inf', (Time.now.utc.to_f - @delta_ttl)]
      else
        r.zremrangebyscore key, '-inf', Time.now.utc.to_f - @ttl
      end
      self
    end
    def count
      redis.zrangebyscore(key, Time.now.utc.to_f - @ttl, 'inf').count
    end
    def wait_time
      Queris.redis.ttl live_queries_key + ":wait"
    end
    
    def before_query(redis, query=nil)
      poke(redis) #this is gonna cost me a roundtrip to master
    end
  end
  
  #a stateful index that cannot be rebuilt without losing data.
  class AccumulatorIndex < RangeIndex
    def stateless?
      false
    end
    def ensure_rangehack_exists(*arg); end #nothing
    def update_rangehacks(*arg); end #also nothing
    def add(obj, value=nil)
      increment(obj, value)
    end
  end
  
  class AccumulatorSearchIndex < ScoredSearchIndex
    #attr_accessor :score_val
    def stateless?
      false
    end
    def ensure_rangehack_exists(*arg); end #nothing
    def update_rangehacks(*arg); end #also nothing
    def add(obj, value=nil)
      increment(obj, value)
    end
    def update_score(obj, val)
      zcommand :zincrby, obj, val
    end
  end
  
  
  class DecayingAccumulatorIndex < AccumulatorIndex
    TIME_OFFSET=Time.new(2015,1,1).to_f #change this every few years to current date to maintain decent index resolution
    attr_reader :half_life
    def initialize(arg)
      @half_life = (arg[:half_life] || arg[:hl]).to_f
      @value = Proc.new do |val|
        val = Float(val)
        val * 2.0 **(t(Time.now.to_f)/@half_life)
      end
      super arg
    end
    def t(seconds)
      seconds - TIME_OFFSET
    end
  end
  
  class DecayingAccumulatorSearchIndex < AccumulatorSearchIndex
    TIME_OFFSET=Time.new(2015,9,4).to_f #change this every few years to current date to maintain decent index resolution
    attr_reader :half_life
    def initialize(arg)
      @half_life = (arg[:half_life] || arg[:hl]).to_f
      super arg
      @real_score_val=@score_val
      binding.pry if @real_score_val.nil?
      1+1.1
      @score_val= Proc.new do |val, obj|
        val = Float(val)
        score_attr_val=@score_attr.nil? ? nil : obj.send(@score_attr)
        val * 2.0 **(t(@real_score_val.call(score_attr_val, obj))/@half_life)
      end
      
    end
    def t(seconds)
      seconds - TIME_OFFSET
    end
  end
  
  class CountIndex < RangeIndex
    def initialize(*arg)
      raise "CountIndex is currently broken. Fix it or use something else"
    end
    def incrby(obj, val)
      redis(obj).zincrby sorted_set_key, val, obj.send(@key)
      if val<0 
        redis(obj).zremrangebyscore sorted_set_key, 0, '-inf'
        #WHOA THERE. We just went O(log(N)) on this simple and presumably O(1) index update. That's bad. 
        #TODO: probabilistically run every 1/log(N) times or less. Average linear complexity for a modicum of win.
      end
    end
    def add(obj)
      incrby obj, 1
    end
    def remove(obj)
      incrby obj, -1
    end
    def update(*arg)
    end
  end
end
