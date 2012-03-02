module Queris
  class Model
    attr_reader :id
    include Queris
    include ObjectMixin
    require "queris/mixin/queris_model"
    
    #get/setter
    def self.redis(redis_client=nil)
      if redis_client.kind_of? Redis
        @redis = redis_client
      end
      @redis
    end

    def self.prefix
      @prefix ||= "#{Queris.redis_prefix}#{self.superclass.name}:#{self.name}:"
    end
    
    def initialize(id=nil)
      set_id id unless id.nil?
      @attributes_were = {}
    end
    
    def self.attribute(attr_name)
      attr_accessor attr_name
      @attributes ||= []
      define_method "#{attr_name}_was" do 
        @attributes_were[attr_name]
      end
      @attributes << attr_name.to_sym
    end

    #get/setter
    def self.attributes(*attributes)
      unless attributes.nil?
        attributes.each do |attr|
          attribute attr
        end
      end
      @attributes
    end
    class << self
      alias :attr :attribute
      alias :attrs :attributes
    end

    def self.find(id)
      new.set_id(id).load
    end
    
    def self.index_attribute(arg={}, &block)
      if arg.kind_of? Symbol 
        arg = {:attribute => arg }
      end
      super arg.merge(:redis => redis), &block
    end
    
    def self.redis_query(arg={})
      query = QuerisModelQuery.new self, arg.merge(:redis => redis(true))
      yield query if block_given?
      query
    end

    #get/setter
    def self.expire(seconds=nil)
      #note that using expire will not update indices, leading to some stale indices
      unless seconds.nil?
        @expire = seconds
      else
        @expire
      end
    end
    
    def set_id(id)
      raise "id already exists and is #{self.id}" unless self.id.nil?
      @id = id
      self
    end

    def save
      key = hash_key #before multi
      redis.multi do
        redis.mapped_hmset key, attr_hash
        expire_sec = self.class.expire
        update_redis_indices if defined? :update_redis_indices
        attributes.each do |attr|
          @attributes_were[attr] = send attr
        end
        redis.expire key, expire_sec unless expire_sec.nil?
      end
      self
    end

    def delete
      key = hash_key
      redis.multi do
        redis.del key
        delete_redis_indices if defined? :delete_redis_indices
      end
      self
    end

    def load
      h = redis.hgetall hash_key
      attributes.each do |attr_name|
        val = h[attr_name.to_s]
        send "#{attr_name}=", val
        @attributes_were[attr_name] = val
      end
      self
    end

    def self.find_all
      keys = redis.keys "#{prefix}*"
      keys.map! do |key|
        self.find key[prefix.length..-1]
      end
      keys
      
    end

    private

    def prefix
      self.class.prefix
    end
    def redis(no_fallback=false)
      if no_fallback
        self.class.redis
      else
        self.class.redis || Queris.redis
      end
    end
    def attributes
      self.class.attributes
    end
    def attr_hash
      @attr_hash ||= {}
      attributes.each do |attr_name|
        @attr_hash[attr_name]=send attr_name
      end
      @attr_hash
    end
    def hash_key(custom_id=nil)
      if id.nil?
        @id = new_id
      end
      @hash_key ||= "#{prefix}#{id}"
    end
    def new_id
      @last_id_key ||= "#{Queris.redis_prefix}#{self.class.superclass.name}:last_id:#{self.class.name}"
      redis.incr @last_id_key
    end
  end
end
