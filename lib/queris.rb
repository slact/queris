# encoding: utf-8
require "queris/version"
require 'rubygems'
require 'digest/sha1'
require "queris/indices"
require "queris/query"
require "queris/mixin/object"

module Queris
  @indexed_models = []
  @redis
  
  #shared redis connection
  def self.redis
    @redis || ($redis.kind_of?(Redis) ? $redis : nil) #for backwards compatibility with crappy old globals-using code.
  end

  #rebuild all known queris indices
  def self.rebuild!(clear=false)
    start = Time.now
    if clear
      redis.flushdb
      puts "Redis db flushed."
    end
    if Rails && Rails.root #if we're in rails
      Dir.glob("#{Rails.root}/app/models/*.rb").sort.each { |file| require_dependency file } #load all models
    end
    @indexed_models.each{|m| m.build_redis_indices false}
    printf "All redis indices rebuilt in %.2f sec.\r\n", Time.now-start
    self
  end

  def self.register_model(model)
    @indexed_models << model unless @indexed_models.member? model
  end
  def self.use_redis(redis)
    @redis=redis
  end
  
  def self.included(base)
    base.send :include, ObjectMixin
    if ActiveRecord and base.superclass == ActiveRecord::Base then
      require "queris/mixin/active_record"
      base.send :include, ActiveRecordMixin
    end
  end
  
  [:create, :update, :delete].each do |op|
    define_method "#{op}_redis_indices" do
      self.class.redis_indices.each { |index| index.send op, self}
    end
  end
end