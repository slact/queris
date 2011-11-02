module Queris
  module ObjectMixin
    def self.included base
      base.extend ObjectMixin
    end
    def redis_indices
      @redis_indices
    end
    def redis_index (index_name=nil, index_class = Index)
      raise ArgumentError, "#{index_class} must be a subclass of Queris::Index" unless index_class <= Index
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
    def redis_prefix (app_name=nil)
      if @redis_prefix.nil? || !app_name.nil?
        if Rails
          @redis_prefix="Rails:#{app_name || Rails.application.class.parent.to_s}:#{self.name}:"
        else
          @redis_prefix="#{app_name && "#{app_name}:"}#{self.name}:"
        end
      else
        @redis_prefix
      end
    end
    
    def index_attribute(arg={}, &block)
      index_class = arg[:index] || Queris::SearchIndex
      raise ArgumentError, "index argument must be in Queris::Index if given" unless index_class <= Queris::Index
      Queris.register_model self
      index_class.new(arg.merge(:model => self), &block)
    end

    def index_attribute_for(arg)
      raise ArgumentError ,"index_attribute_for requires :model argument" unless arg[:model]
      index = index_attribute(arg) do |index|
        index.name = "foreign_index_#{index.name}"
      end
      arg[:model].send :index_attribute, arg.merge(:index=> Queris::ForeignIndex, :real_index => index)
    end

    def index_attribute_from(arg) 
      model = arg[:model]
      model.send(:include, Queris) unless model.include? Queris
      model.send(:index_attribute_for, arg.merge(:model => self))
    end

    def index_range_attribute(arg)
      index_attribute arg.merge :index => Queris::RangeIndex
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

    def query(arg={})
      redis_query arg
    end
      
    def redis_query (arg={})
      query = Queris::Query.new arg.merge(:model => self)
      yield query if block_given?
      query
    end
    
    def build_redis_indices(build_foreign = true)
      start_time = Time.now
      all = self.find_all
      fetch_time = Time.now - start_time
      redis_start_time, printy, total =Time.now, 0, all.count - 1
      Queris.redis.multi do
        all.each_with_index do |row, i|
          if printy == i
            print "\rBuilding redis indices... #{((i.to_f/total) * 100).round.to_i}%" unless total == 0
            printy += (total * 0.05).round
          end
          row.create_redis_indices 
        end
        print "\rBuilt all native indices for #{self.name}. Committing to redis..."
      end
      print "\rBuilt redis indices for #{total} rows in #{(Time.now - redis_start_time).round 3} sec. (#{fetch_time.round 3} sec. to fetch all data).\r\n"
      #update all foreign indices
      foreign = 0
      redis_indices.each do |index|
        if index.kind_of? Queris::ForeignIndex
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
      if marshaled = Queris.redis.get(key)
        Marshal.load marshaled
      elsif (found = find id) #ActiveRecord-like call. Doesn't belong here.
        Queris.redis.set key, Marshal.dump(found)
        found
      end
    end
  end
end