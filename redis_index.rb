# encoding: utf-8
require 'rubygems'
require 'digest/sha1'
module RedisIndex
  @indexed_models = []
  def self.rebuild!(clear=false)
    start = Time.now
    if clear
      $redis.flushdb
      puts "Redis db flushed."
    end
    Dir.glob("#{Rails.root}/app/models/*.rb").sort.each { |file| require_dependency file } #load all models
    @indexed_models.each{|m| m.build_redis_indices false}
    printf "All redis indices rebuilt in %.2f sec.\r\n", Time.now-start
    self
  end
  def self.add_model(model)
    @indexed_models << model unless @indexed_models.member? model
  end
  
  
  def self.included(base)
    base.class_eval do
      class << self
        def redis_indices
          @redis_indices
        end
        def redis_index (index_name=nil, index_class = Index)
          raise ArgumentError, "#{index_class} must be a subclass of RedisIndex::Index" unless index_class <= Index
          case index_name
          when index_class, NilClass, Query
            return index_name
          end
          @redis_indices||=[] #this line sucks.
          index = @redis_indices.find { |i| index_name.respond_to?(:to_sym) && i.name == index_name.to_sym && i.kind_of?(index_class)}
          raise Exception, "Index #{index_name} not found in #{name}" unless index
          index
        end
        def add_redis_index(index)
          @redis_indices||=[]
          @redis_indices.push index
        end
        def redis_prefix
          @redis_prefix||="Rails:#{Rails.application.class.parent.to_s}:#{self.name}:"
        end
      end
    end
    base.extend ClassMethods
    base.after_create :create_redis_indices
    base.before_save :update_redis_indices, :uncache
    base.before_destroy :delete_redis_indices, :uncache
    base.after_initialize do 
      
    end

  end

  module ClassMethods
    def index_attribute(arg={}, &block)
      index_class = arg[:index] || SearchIndex
      raise ArgumentError, "index argument must be in RedisIndex::Index if given" unless index_class <= Index
      RedisIndex.add_model self
      index_class.new(arg.merge(:model => self), &block)
    end

    def index_attribute_for(arg)
      raise ArgumentError ,"index_attribute_for requires :model argument" unless arg[:model]
      index = index_attribute(arg) do |index|
        index.name = "foreign_index_#{index.name}"
      end
      arg[:model].send :index_attribute, arg.merge(:index=> ForeignIndex, :real_index => index)
    end

    def index_attribute_from(arg) #doesn't work yet.
      model = arg[:model]
      model.send(:include, RedisIndex) unless model.include? RedisIndex
      model.send(:index_attribute_for, arg.merge(:model => self))
    end

    def index_range_attribute(arg)
      index_attribute arg.merge :index => RangeIndex
    end
    def index_range_attributes(*arg)
      base_param = arg.last.kind_of?(Hash) ? arg.pop : {}
      arg.each do |attr|
        index_range_attribute base_param.merge :attribute => attr
      end
    end
    alias index_sort_attribute index_range_attribute

    def index_attributes(*arg)
      base_param = arg.last.kind_of?(Hash) ? arg.pop : {}
      arg.each do |attr|
        index_attribute base_param.merge :attribute => attr
      end
      self
    end

    def redis_query (arg={})
      query = ActiveRecordQuery.new arg.merge(:model => self)
      yield query if block_given?
      query
    end

    alias_method :query, :redis_query unless self.respond_to? :query
    
    def build_redis_indices(build_foreign = true)
      start_time = Time.now
      all = self.find(:all)
      sql_time = Time.now - start_time
      redis_start_time, printy, total =Time.now, 0, all.count - 1
      $redis.multi do
        all.each_with_index do |row, i|
          if printy == i
            print "\rBuilding redis indices... #{((i.to_f/total) * 100).round.to_i}%" unless total == 0
            printy += (total * 0.05).round
          end
          row.create_redis_indices 
        end
        print "\rBuilt all native indices for #{self.name}. Committing to redis..."
      end
      print "\rBuilt redis indices for #{total} rows in #{(Time.now - redis_start_time).round 3} sec. (#{sql_time.round 3} sec. for SQL).\r\n"
      #update all foreign indices
      foreign = 0
      redis_indices.each do |index|
        if index.kind_of? ForeignIndex
          foreign+=1
          index.real_index.model.send :build_redis_indices 
        end
      end if build_foreign
      puts "Built #{redis_indices.count} ind#{redis_indices.index.count == 1 ? "ex" : "ices"} (#{build_foreign ? foreign : 'skipped'} foreign) for #{self.name} in #{(Time.now - start_time).round(3)} seconds."
      self
    end
      
    def cache_key(id)
      "#{redis_prefix}#{id}:cached"
    end
    
    def find_cached(id)
      key = cache_key id
      if marshaled = $redis.get(key)
        Marshal.load marshaled
      elsif (found = find id)
        $redis.set key, Marshal.dump(found)
        found
      end
    end
  end
  
  [:create, :update, :delete].each do |op|
    define_method "#{op}_redis_indices" do
      self.class.redis_indices.each { |index| index.send op, self}
    end
  end
  
  def uncache
    $redis.del self.class.cache_key(id)
  end
end
require "redis_index/indices"
require "redis_index/query"
require "redis_index/mixin/active_record"