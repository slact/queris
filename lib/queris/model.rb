module Queris
  class Model
    attr_reader :id
    include Queris #this doesn't trigger Queris::included as it seems it ought to...
    require "queris/mixin/queris_model"
    include ObjectMixin
    include QuerisModelMixin
    
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
    
    def initialize(id=nil, arg={})
      set_id id unless id.nil?
      @attributes_were = {}
      @redis = arg[:redis]
    end
    
    def self.attribute(attr_name)
      attr_accessor attr_name
      @attributes ||= []
      define_method "#{attr_name}_was" do 
        @attributes_were[attr_name]
      end
      define_method "#{attr_name}_was=" do |val|
        puts "set prev value of #{attr_name} to #{val}"
        @attributes_were[attr_name]=val
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
   

    #get/setter
    def self.expire(seconds=nil)
      #note that using expire will not update indices, leading to some serious staleness
      unless seconds.nil?
        @expire = seconds
      else
        @expire
      end
    end
    
    def set_id(id, overwrite=false)
      raise "id already exists and is #{self.id}" unless overwrite || self.id.nil?
      @id = id
      self
    end
    def id= (new_id)
      set_id new_id
    end

    def save
      key = hash_key #before multi
      redis.multi do
        unless index_only
          redis.mapped_hmset key, attr_hash 
          expire_sec = self.class.expire
        end
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
      raise "Can't load #{self.class.name} with id #{id} -- model was specified as index_only, so it was never saved." if index_only
      h = redis.hgetall hash_key
      attributes.each do |attr_name|
        val = h[attr_name.to_s]
        send "#{attr_name}=", val
        @attributes_were[attr_name] = val
      end
      self
    end

    def import(attrs={})
      attrs.each do |attr_name, val|
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

    def redis(no_fallback=false)
      if no_fallback
        @redis || self.class.redis
      else
        @redis || self.class.redis || Queris.redis
      end
    end
    
    private

    def prefix
      self.class.prefix
    end
    def index_only
      @index_only ||= self.class.class_eval do @index_only end #ugly
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
