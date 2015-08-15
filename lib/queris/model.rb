require "redis"
module Queris
  
  class Model
    attr_reader :id
    include Queris #this doesn't trigger Queris::included as it seems it ought to...
    require "queris/mixin/queris_model"
    include ObjectMixin
    include QuerisModelMixin
    
    def self.attr_val_block
      @attr_val_block ||= {}
    end
    
    class << self
      def redis(redis_client=nil)
        if redis_client.kind_of? Redis
          @redis = redis_client
        end
        @redis || Queris.redis
      end

      def prefix
        @prefix ||= "#{Queris.redis_prefix}#{self.superclass.name}:#{self.name}:"
      end

      #get/setter
      def attributes(*attributes)
        unless attributes.nil?
          attributes.each do |attr|
            attribute attr
            if block_given?
              self.attr_val_block[attr.to_sym]=Proc.new
            end
          end
        end
        @attributes
      end
      def attribute(attr_name)
        @attributes ||= [] #Class instance var
        attr_name = attr_name.to_sym
        raise ArgumentError, "Attribute #{attr_name} already exists in Queris model #{self.name}." if @attributes.member? attr_name
        if block_given?
          bb=Proc.new
          self.attr_val_block[attr_name]=bb
        end
        define_method "#{attr_name}" do |noload=false|
          binding.pry if @attributes.nil?
          1
          if (val = @attributes[attr_name]).nil? && !@loaded && !noload && !@noload
            load
            send attr_name, true
          else
            val
          end
        end
        define_method "#{attr_name}=" do |val| #setter
          if self.class.attr_val_block[attr_name]
            val = self.class.attr_val_block[attr_name].call(val)
          end
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
      alias :attr :attribute
      alias :attrs :attributes

      #get/setter
      def expire(seconds=nil)
        #note that using expire will not update indices, leading to some serious staleness
        unless seconds.nil?
          @expire = seconds
        else
          @expire
        end
      end

      def find(id, opt={})
        got= get id, opt
        got.loaded? ? got : nil
      end
      alias :find_cached :find
      
      def get(id, opt={})
        ret=new(id)
        if opt[:redis]
          ret.load(nil, redis: opt[:redis])
        else
          ret.load
        end
        ret
      end

      def find_all #NOT FOR PRODUCTION USE!
        keys = redis.keys "#{prefix}*"
        keys.map! do |key|
          self.find key[prefix.length..-1]
        end
        keys
      end

      def restore(hash, id)
        new(id).load(hash)
      end

      %w(during_save before_save after_save).each do |callback|
        define_method callback do |&block|
          @callbacks ||= {}
          if block
            @callbacks[callback] ||= []
            @callbacks[callback]  << block
          else
            @callbacks[callback] || []
          end 
        end
      end
    end

    def run_callbacks(callback, redis=nil)
      (self.class.send(callback) || []).each {|block| block.call(self, redis)}
    end
    private :run_callbacks

    def initialize(id=nil, arg={})
      @attributes = {}
      @attributes_to_save = {}
      @attributes_to_incr = {}
      @attributes_were = {}
      @redis = arg[:redis]
      set_id id unless id.nil?
    end

    def set_id(nid, overwrite=false)
      noload do
        raise Error, "id cannot be a Hash" if Hash === nid
        raise Error, "id cannot be an Array" if Array === nid
        raise Error, "id already exists and is #{self.id}" unless overwrite || self.id.nil?
      end
      @id= nid
      self
    end
    def id=(nid)
      set_id nid
    end

    def save
      key = hash_key #before multi
      noload do
        run_callbacks :before_save
        # to ensure atomicity, we unfortunately need two round trips to redis
        begin
          if @attributes_to_save.length > 0
            attrs_to_save = @attributes_to_save.keys
            bulk_response = redis.pipelined do
              redis.watch key
              redis.hmget key, *attrs_to_save
            end
            current_saved_attr_vals = bulk_response.last
            attrs_to_save.each_with_index do |attr,i| #sync with server
              val=current_saved_attr_vals[i]
              @attributes_were[attr]=val
            end
            bulk_response = redis.multi do |r|
              unless index_only
                @attributes_to_incr.each do |attr, incr_by_val|
                  r.hincrbyfloat key, attr, incr_by_val #redis server >= 2.6
                  unless (val = send(attr, true)).nil?
                    @attributes_were[attr]=val
                  end
                end
                r.mapped_hmset key, @attributes_to_save
                expire_sec = self.class.expire
              end

              update_redis_indices if defined? :update_redis_indices
              @attributes_to_save.each {|attr, val| @attributes_were[attr]=val }
              r.expire key, expire_sec unless expire_sec.nil?
              run_callbacks :during_save, r
            end
          end
        end while bulk_response.nil?
        @attributes_to_save.clear
        @attributes_to_incr.clear
        ret= self
        run_callbacks :after_save, redis
        ret
      end
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

    def loaded?
      @loaded && self
    end
    
    def delete
      noload do
        key = hash_key
        redis.multi do
          redis.del key
          delete_redis_indices if defined? :delete_redis_indices
        end
        @noload=false
        self
      end
    end

    def load(hash=nil, opt={})
      raise SchemaError, "Can't load #{self.class.name} with id #{id} -- model was specified index_only, so it was never saved." if index_only
      unless hash
        hash_future, hash_exists = nil, nil
        hash_key
        (opt[:redis] || redis).multi do |r|
          hash_future = r.hgetall hash_key
          hash_exists = r.exists hash_key
        end
        if hash_exists.value
          hash = hash_future.value
        elsif not hash
          return nil
        end
      end
      hash.each do |attr_name, val|
        attr = attr_name.to_sym
        if self.class.attr_val_block[attr]
          val = self.class.attr_val_block[attr].call(val)
        end
        
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

    def hash_key(custom_id=nil)
      if id.nil?
        @id = new_id
      end
      @hash_key ||= "#{prefix}#{custom_id || id}"
    end
    alias :key :hash_key

    def noload
      @noload=true
      ret = yield
      @noload=false
      ret
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
    
    def new_id
      @last_id_key ||= "#{Queris.redis_prefix}#{self.class.superclass.name}:last_id:#{self.class.name}"
      redis.incr @last_id_key
    end
  end
end
