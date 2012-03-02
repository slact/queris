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
  
  @indexed_models = []
  
  @redis_connections=[]
  @redis_by_role={}
  
  #retrieve redis connection matching given redis server role, in order or decreasing preference
  def self.redis(*redis_roles)
    redises(*redis_roles).sample || ($redis.kind_of?(Redis) ? $redis : nil) #for backwards compatibility with crappy old globals-using code.
  end
    

  # get all redis connections for given redis server role.
  # when more than one role is passed, treat them in order of decreasing preference
  # when no role is given, :master is assumed

  def self.redises(*redis_roles)
    redis_roles << :master if redis_roles.empty? #default
    redis_roles.each do |role|
      unless (redises=@redis_by_role[role]).nil? || redises.empty?
        return redises
      end
    end
    []
  end
  
  def self.add_redis(redis, *roles)
    roles << :master if roles.empty?
    @redis_connections << redis
    roles.each do |role|
      @redis_by_role[role]||=[]
      @redis_by_role[role] << redis
    end
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
    if Rails && Rails.root #if we're in rails
      Dir.glob("#{Rails.root}/app/models/*.rb").sort.each { |file| require_dependency file } #load all models
    end
    @indexed_models.each do |model| 
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
    @indexed_models << model unless @indexed_models.member? model
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
    if ActiveRecord and base.superclass == ActiveRecord::Base then
      require "queris/mixin/active_record"
      base.send :include, ActiveRecordMixin
    elsif Ohm and base.superclass == Ohm::Model
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
