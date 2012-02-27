module Queris
  class Model
    attr_reader :id
    def initialize(id=nil)
      set_id id unless id.nil?
    end
    def self.attribute(attr_name)
      attr_accessor attr_name
      @attributes ||= []
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
    
    def save(redis=nil)
      redis ||= Queris.redis :master
      redis.mapped_hmset hash_key, attr_hash
      expire_sec = self.class.expire
      redis.expire hash_key, expire_sec unless expire_sec.nil?
      self
    end
    
    def delete(redis=nil)
      redis ||= Queris.redis :master
      redis.del hash_key
      self
    end
    
    def load(redis=nil)
      redis ||= Queris.redis :master
      h = redis.hgetall hash_key
      self.class.attributes.each do |attr_name|
        send "#{attr_name}=", h[attr_name.to_s]
      end
      self
    end
    
    private
    def prefix
      @@prefix ||= "#{Queris.redis_prefix}#{self.class.superclass.name}:#{self.class.name}:"
    end
    def attr_hash
      @attr_hash ||= {}
      self.class.attributes.each do |attr_name|
        @attr_hash[attr_name]=send attr_name
      end
      @attr_hash
    end
    def hash_key
      if id.nil?
        @id = new_id
      end
      @hash_key ||= "#{prefix}#{id}"
    end
    def new_id(redis=nil)
      redis ||= Queris.redis :master
      redis.incr "#{prefix}last_id"
    end
  end
end
