module Queris
  class Index
    attr_accessor :name, :redis, :model, :attribute
    def initialize(arg={})
      arg.each do |opt, val|
        instance_variable_set "@#{opt}".to_sym, val
      end
      @redis ||= Queris.redis
      @name ||= @attribute
      @attribute ||= @name
      @attribute = @attribute.to_sym unless !@attribute
      @name = @name.to_sym
      @key ||= :id #object's key attribute (default is 'id')
      @keyf ||= "%s#{self.class.name.sub(/^.*::/, "")}:#{@name}=%s"
      if block_given?
        yield self, arg
      end
      raise ArgumentError, "Index must have a name" unless @name
      raise ArgumentError, "Index must have a model" unless @model
      @model.add_redis_index self
    end
    def val(value)
      @value.nil? ? value : @value.call(value)
    end
    def index_val(value, obj=nil)
      (@index_value || @value).nil? ? value : (@index_value || @value).call(value, obj)
    end
    def digest(value)
      #value
      Digest::SHA1.hexdigest value.to_s
    end
    def value_is(obj)
      obj.send @attribute
    end
    def value_was(obj)
      obj.send "#{@attribute}_was"
    end
    def update(obj)
      val_is, val_was = value_is(obj), value_was(obj)
      if(val_is != val_was)
        remove(obj, val_was)
        add(obj)
      end
    end
    def create(obj)
      add(obj)
    end
    def delete(obj)
      remove(obj, value_was(obj))
    end    
  end
    
  class SearchIndex < Index
    def initialize(arg={})
      super arg
      @type ||= "string"
      raise Exception, "Model not passed to index." unless @model
    end
    
    def set_key(value, prefix=nil)
      (@keyf) %[prefix || @redis_prefix || @model.redis_prefix, digest(val value)]
    end
    def add(obj, value = nil)
      i=0
      value = index_val( value || obj.send(@attribute), obj)
      (value.kind_of?(Enumerable) ? value : [ value ]).each do |val|
        i +=1
        @redis.sadd set_key(val), obj.send(@key)
      end
    end
    def remove(obj, value = nil)
      value = index_val( value || obj.send(@attribute), obj)
      (value.kind_of?(Enumerable) ? value : [ value ]).each do |val|
        @redis.srem set_key(val.nil? ? obj.send(@attribute) : val), obj.send(@key)
      end
    end

    def build_query_part(command, query, value, multiplier=nil)
      ret = []
      if value.kind_of? Enumerable
        sub = query.subquery
        value.to_a.uniq.each {|val| sub.union self, val }
        set_key = sub.results_key
      else
        set_key = set_key(value)
      end
      ret.push :command => command, :key => set_key, :weight => multiplier
      ret
    end
  end
  
  class ForeignIndex < SearchIndex
    attr_accessor :real_index
    def initialize(arg)
      raise ArgumentError, "Missing required initialization attribute real_index for ForeignIndex." unless arg[:real_index]
      super arg
    end
    def create(*a) end
    alias :delete :create
    alias :update :create
    def set_key(*arg)
      @real_index.set_key(*arg)
    end
    def method_missing(method)
      @real_index.method
    end
  end
  
  
  
  class PresenceIndex < SearchIndex
    def initialize(arg)
      super arg
      @counter_keyf = "#{@model.redis_prefix}#{self.class.name.sub(/^.*::/, "")}:#{@name}:#{@attribute}=%s:counter"
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
      k = @redis.incr counter_key(obj)
      if k == @threshold
        super obj
      end
    end
    def remove(obj, value=nil)
      ckey = counter_key obj
      @redis.decr ckey
      if @redis.get(ckey).to_i. < @threshold
        @redis.del ckey
        super obj
      end
    end
  end
  
  class RangeIndex < SearchIndex
    def initialize(arg)
      @value ||= proc { |x| x.to_f }
      super arg
    end
    def val(val=nil, obj=nil)
      @value.call val, obj
    end
    def sorted_set_key(val=nil, prefix=nil)
      (@keyf) %[prefix || @model.redis_prefix, "(...)"]
    end
    
    def add(obj, value=nil)
      my_val = val(value || value_is(obj), obj)
      @redis.zadd sorted_set_key(obj.send @attribute), my_val, obj.send(@key)
    end
    
    def remove(obj, value=nil)
      @redis.zrem sorted_set_key(obj.send @attribute), obj.send(@key)
    end

    private
    def range(command, query, min=nil, max=nil, exclude_min=nil, exclude_max=nil, range_only=nil, multiplier=1)
      key, ret = sorted_set_key, []
      min_param, max_param = "#{exclude_min ? "(" : nil}#{min.to_f}", "#{exclude_max ? "(" : nil}#{max.to_f}"
      ret << {:command => command, :key => key, :weight => multiplier} unless range_only
      ret <<  {:command => :zremrangebyscore, :arg => ['-inf', min_param]} unless min.nil?
      ret << {:command => :zremrangebyscore, :arg => [max_param, 'inf']} unless max.nil?
      ret
    end

    def build_query_part(command, query, value, multiplier=1)
      case value
      when Range
        range command, query, val(value.begin), val(value.end), true, value.exclude_end?, nil, multiplier
      when Enumerable
        raise ArgumentError, "RangeIndex doesn't accept non-Range Enumerables"
      when NilClass
        range command, query, nil, nil, false, false, false, multiplier
      else
        ret = range command, query, '-inf', value || 'inf', false, true, nil, multiplier
        if value
          ret += range command, query, val(value), 'inf', true, false, true, multiplier
        end
        ret
      end
    end

  end
end