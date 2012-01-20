# encoding: utf-8
require "queris/version"
require 'rubygems'
require 'digest/sha1'
require "queris/indices"
require "queris/query"
require "queris/mixin/object"

module Queris
  # Queris is a querying and indexing tool for various Ruby objects.
  @indexed_models = []
  @redis
  @query_redis=[]
  
  #shared redis connection
  def self.redis
    @redis || ($redis.kind_of?(Redis) ? $redis : nil) #for backwards compatibility with crappy old globals-using code.
  end

  def self.query_redis
    unless @query_redis.nil? or @query_redis.empty?
      @query_redis[rand @query_redis.length]
    else
      redis
    end
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
    ([redis] + @query_redis).uniq
  end
  
  def self.register_model(model)
    @indexed_models << model unless @indexed_models.member? model
  end
  def self.use_redis(redis, opt={})
    @query_redis << redis unless opt[:nocache]
    @redis=redis
  end
  def self.use_query_redis(*args)
    args.delete_if {|arg| not arg.kind_of? Redis}.each do |arg|
      @query_redis << arg
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
    if Rails
      "Rails:#{app_name || Rails.application.class.parent.to_s}:#{self.name}:"
    else
      "#{app_name && "#{app_name}:"}#{self.name}:"
    end
  end
end
