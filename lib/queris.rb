# encoding: utf-8
require "queris/version"
require 'rubygems'
require 'digest/sha1'
require "queris/indices"
require "queris/query"
require "queris/mixin/object"
require "queris/model"

# Queris is a querying and indexing tool for various Ruby objects.
module Queris
  
  @models = []
  @model_lookup={}
  @redis_connections=[]
  @redis_by_role={}
  @debug=false
  @redis_scripts={}
  class << self
    attr_accessor :redis_scripts
    attr_accessor :debug
    def debug?; @debug; end
  
    #retrieve redis connection matching given redis server role, in order or decreasing preference
    def redis(*redis_roles)
      redises(*redis_roles).sample || ($redis.kind_of?(Redis) ? $redis : nil) #for backwards compatibility with crappy old globals-using code.
    end
   
    #returns another connection to the same server
    def duplicate_redis_client(redis, role=false)
      raise "No redis client to duplicate."  unless redis
      raise "Not a redis client" unless Redis === redis
      cl = redis.client
      raise "Redis client doesn't have connection info (Can't get client info while in a redis.multi block... for now...)" unless cl.host
      r = Redis.new({scheme:    cl.scheme,
                    port:      cl.port,
                    host:      cl.host,
                    path:      cl.path,
                    timeout:   cl.timeout,
                    password:  cl.password,
                    db:        cl.db })
      add_redis r, role
    end
    # get all redis connections for given redis server role.
    # when more than one role is passed, treat them in order of decreasing preference
    # when no role is given, :master is assumed

    def redises(*redis_roles)
      redis_roles << :master if redis_roles.empty? #default
      redis_roles.each do |role|
        unless (redises=@redis_by_role[role.to_sym]).nil? || redises.empty?
          return redises
        end
      end
      []
    end

    def add_redis(redis, *roles)
      if !(Redis === redis) && (Redis === roles.first) # flipped aguments. that's okay, we accept those, too
        redis, roles = roles.first, [ redis ]
      end
      roles << :master if roles.empty?
      roles = [] if roles.length == 1 && !roles.first
      @redis_connections << redis
      roles.each do |role|
        role = role.to_sym
        @redis_by_role[role]||=[]
        @redis_by_role[role] << redis
      end
      
      #throw our lua scripts onto the server
      redis_scripts.each do |name, contents|
        hash = redis.script 'load', contents
        raise "Failed loading script #{name} onto server: mismatched hash" unless script_hash(name) == hash
      end
      
      if Object.const_defined? 'ActiveSupport'
        #the following is one ugly monkey(patch).
        # we assume that, since we're in Railsworld, the Redis logger
        # is up for grabs. It would be cleaner to wrap the redis client in a class, 
        # but I'm coding dirty for brevity. 
        # THIS MUST BE ADDRESSED IN THE FUTURE
        
        class << redis.client
          protected
          def logging(commands)
            ActiveSupport::Notifications.instrument("command.queris") { yield }
          end
        end
      end
      redis
    end
    
    #rebuild all known queris indices
    def rebuild!(clear=false)
      start = Time.now
      if Object.const_defined? 'Rails'
        Dir.glob("#{Rails.root}/app/models/*.rb").sort.each { |file| require_dependency file } #load all models
      end
      @models.each do |model| 
        if clear
          delkeys = redis.keys "#{model.redis_prefix}*"
          redis.del(*delkeys) unless delkeys.count == 0
          puts "Deleted #{delkeys.count} #{self.name} keys for #{model.name}."
        end
        model.build_redis_indices false
      end
      printf "All redis indices rebuilt in %.2f sec.\r\n", Time.now-start
      self
    end

    def all_redises; @redis_connections; end
    
    attr_accessor :models
    
    def register_model(model)
      unless @models.member? model
        @models << model
        @model_lookup[model.name.to_sym]=model
      end
    end

    def model(model_name)
      @model_lookup[model_name.to_sym]
    end

    def included(base)
      base.send :include, ObjectMixin
      if const_defined?('ActiveRecord') and base.superclass == ActiveRecord::Base then
        require "queris/mixin/active_record"
        base.send :include, ActiveRecordMixin
      elsif const_defined?('Ohm') and base.superclass == Ohm::Model
        require "queris/mixin/ohm"
        base.send :include, OhmMixin
      end
    end
  
    def redis_prefix(app_name=nil)
    #i'm using a simple string-concatenation key prefix scheme. I could have used something like Nest, but it seemed excessive.
      if Object.const_defined? 'Rails'
        "Rails:#{app_name || Rails.application.class.parent.to_s}:#{self.name}:"
      elsif app_name.nil?
        "#{self.name}:"
      else
        "#{app_name}:#{self.name}:"
      end
    end
  
    def to_redis_float(val)
      val=val.to_f
      case val
      when Float::INFINITY
        "inf"
      when -Float::INFINITY
        "-inf"
      else
        if val != val #NaN
          "nan"
        else
          val
        end
      end
    end
    
    def from_redis_float(val)
      case val
      when "inf", "+inf"
        Float::INFINITY
      when "-inf"
        -Float::INFINITY
      when "nan"
        Float::NAN
      else
        val.to_f
      end
    end
    
    def script(name)
      redis_scripts[name.to_sym]
    end

    def script_hash(name)
      name = name.to_sym
      @script_hash||={}
      unless (hash=@script_hash[name])
        contents = script(name)
        raise "Unknown redis script #{name}." unless contents
        hash = Digest::SHA1.hexdigest contents
        @script_hash[name] = hash
      end
      hash
    end
    
    def load_lua_script(name, contents)
      redis_scripts[name.to_sym]=contents
    end
    #load redis lua scripts
    Dir[File.join(File.dirname(__FILE__),'queris/redis_scripts/*.lua')].each do |path|
      name = File.basename path, '.lua'
      script = IO.read(path)
      Queris.load_lua_script(name, script)
    end
  end
end

#ugly rails hooks
if Object.const_defined? 'Rails'
  require "rails/log_subscriber"
  require "rails/request_timing"
end
#ugly rake hooks
if Object.const_defined? 'Rake'
  class QuerisTasks < Rails::Railtie
    rake_tasks do
      Dir[File.join(File.dirname(__FILE__),'tasks/*.rake')].each { |f| load f }
    end
  end
end
