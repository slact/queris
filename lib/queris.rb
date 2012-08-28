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
  
  def self.debug?
    @debug
  end
  def self.debug=(enabled)
    @debug = enabled
  end
  
  #retrieve redis connection matching given redis server role, in order or decreasing preference
  def self.redis(*redis_roles)
    redises(*redis_roles).sample || ($redis.kind_of?(Redis) ? $redis : nil) #for backwards compatibility with crappy old globals-using code.
  end
   
  #returns another connection to the same server
  def self.duplicate_redis_client(redis, role=false)
    cl = redis.client
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

  def self.redises(*redis_roles)
    redis_roles << :master if redis_roles.empty? #default
    redis_roles.each do |role|
      unless (redises=@redis_by_role[role.to_sym]).nil? || redises.empty?
        return redises
      end
    end
    []
  end
  
  def self.add_redis(redis, *roles)
    roles << :master if roles.empty?
    roles = [] if roles.length == 1 && !roles.first
    @redis_connections << redis
    roles.each do |role|
      role = role.to_sym
      @redis_by_role[role]||=[]
      @redis_by_role[role] << redis
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
  
  def self.redis_master
    redis :master
  end

  def self.query_redis
    redis :slave, :master
  end
  
  #rebuild all known queris indices
  def self.rebuild!(clear=false)
    start = Time.now
    if Object.const_defined? 'Rails'
      Dir.glob("#{Rails.root}/app/models/*.rb").sort.each { |file| require_dependency file } #load all models
    end
    @models.each do |model| 
      if clear
        delkeys = redis.keys "#{model.redis_prefix}*"
        deleted = redis.del(*delkeys) unless delkeys.count == 0
        puts "Deleted #{delkeys.count} #{self.name} keys for #{model.name}."
      end
      model.build_redis_indices false
    end
    printf "All redis indices rebuilt in %.2f sec.\r\n", Time.now-start
    self
  end

  def self.all_redises
    @redis_connections
  end
  
  def self.register_model(model)
    unless @models.member? model
      @models << model
      @model_lookup[model.name.to_sym]=model
    end
  end
  
  def self.model(model_name)
    @model_lookup[model_name.to_sym]
  end
  
  #OBSOLETE
  def self.use_redis(redis_master, opt={})
    add_redis redis_master, :master
  end
  
  #OBSOLETE
  def self.use_query_redis(*args)
    args.delete_if {|arg| not arg.kind_of? Redis}.each do |arg|
      add_redis arg, :slave
    end
  end

  def self.included(base)
    base.send :include, ObjectMixin
    if const_defined?('ActiveRecord') and base.superclass == ActiveRecord::Base then
      require "queris/mixin/active_record"
      base.send :include, ActiveRecordMixin
    elsif const_defined?('Ohm') and base.superclass == Ohm::Model
      require "queris/mixin/ohm"
      base.send :include, OhmMixin
    end
  end
  
  def self.redis_prefix(app_name=nil)
   #i'm using a simple string-concatenation key prefix scheme. I could have used something like Nest, but it seemed excessive.
    if Object.const_defined? 'Rails'
      "Rails:#{app_name || Rails.application.class.parent.to_s}:#{self.name}:"
    elsif app_name.nil?
      "#{self.name}:"
    else
      "#{app_name}:#{self.name}:"
    end
  end
end

#ugly rails hooks
if Object.const_defined? 'Rails'
  require "rails/log_subscriber"
  require "rails/request_timing"
end