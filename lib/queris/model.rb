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
      @attributes = {}
      @attributes_to_save = {}
      @attributes_to_incr = {}
      @attributes_were = {}
      @redis = arg[:redis]
    end
    
    def self.attribute(attr_name)
      @attributes ||= [] #Class instance var
      attr_name = attr_name.to_sym
      raise ArgumentError, "Attribute #{attr_name} already exists in Queris model #{self.name}." if @attributes.member? attr_name
      
      define_method "#{attr_name}" do |noload=false|
        if (val = @attributes[attr_name]).nil? && !@loaded && !noload && !@noload
          load
          send attr_name, true
        else
          val
        end
      end
      
      define_method "#{attr_name}=" do |val| #setter
        if @attributes_were[attr_name].nil?
          @attributes_were[attr_name] = @attributes[attr_name] 
        end
        @attributes_to_save[attr_name]=val
        @attributes[attr_name]=val
      end
      
      define_method "#{attr_name}_was" do 
        @attributes_were[attr_name]
      end
      
      define_method "#{attr_name}_was=" do |val|
        @attributes_were[attr_name]=val
      end
      private "#{attr_name}_was="
      
      attributes << attr_name
    end

    def increment(attr_name, delta_val)
      raise ArgumentError, "Can't increment attribute #{attr_name} because it is used by at least one non-incrementable index." unless self.class.can_increment_attribute? attr_name
      raise ArgumentError, "Can't increment attribute #{attr_name} by non-numeric value <#{delta_val}>. Increment only by numbers, please." unless delta_val.kind_of? Numeric
      
      @attributes_to_incr[attr_name.to_sym]=delta_val
      unless (val = send(attr_name, true)).nil?
        send "#{attr_name}=", val + delta_val
      end
      self
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
      @noload = true
      # to ensure atomicity, we unfortunately need two round trips to redis
      begin
        if @attributes_to_save.length > 0
          bulk_response = redis.pipelined do
            redis.watch key
            redis.hmget key, *@attributes_to_save.keys
          end
          bulk_response.last.each do |attr, val| #sync with server
            @attributes_were[attr]=val
          end
        end
        bulk_response = redis.multi do
          unless index_only
            @attributes_to_incr.each do |attr, incr_by_val|
              redis.hincrbyfloat key, attr, incr_by_val #redis server >= 2.6
              unless (val = send(attr, true)).nil?
                @attributes_were[attr]=val
              end
            end
            redis.mapped_hmset key, @attributes_to_save
            expire_sec = self.class.expire
          end

          update_redis_indices if defined? :update_redis_indices

          @attributes_to_save.each {|attr, val| @attributes_were[attr]=val }
          redis.expire key, expire_sec unless expire_sec.nil?
        end
      end while bulk_response.nil?
      @attributes_to_save.clear
      @attributes_to_incr.clear
      @noload = false
      self
    end

    def attribute_diff(attr)
      @attributes_to_incr[attr.to_sym]
    end
    #list of changed attributes
    def changed
      delta = (@attributes_to_save.keys + @attributes_to_incr.keys)
      delta.uniq!
      delta
    end
    #any unsaved changes?
    def changed?
      @attributes_to_save.empty? && @attributes_to_incr.empty?
    end

    def delete
      key = hash_key
      redis.multi do
        redis.del key
        delete_redis_indices if defined? :delete_redis_indices
      end
      self
    end

    def load(hash=nil)
      raise "Can't load #{self.class.name} with id #{id} -- model was specified index_only, so it was never saved." if index_only
      (hash || redis.hgetall(hash_key)).each do |attr_name, val|
        attr = attr_name.to_sym
        if (old_val = @attributes[attr]) != val
          @attributes_were[attr] = old_val unless old_val.nil?
          @attributes[attr] = val
        end
      end
      @loaded = true
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

    def redis=(r)
      @redis=r
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
        val = send attr_name, true
        @attr_hash[attr_name]= val unless attribute_was(attr_name) == val
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
