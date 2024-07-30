# encoding: utf-8
require "queris/version"
require 'rubygems'
require 'digest/sha1'
require "queris/errors"
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
    def digest(val)
      Digest::SHA1.hexdigest(val.to_s)
    end
    def debug?; @debug; end
  
    #retrieve redis connection matching given redis server role, in order or decreasing preference
    def redis(*redis_roles)
      redises(*redis_roles).sample || ($redis.kind_of?(Redis) ? $redis : nil) #for backwards compatibility with crappy old globals-using code.
    end
   
    #returns another connection to the same server
    def duplicate_redis_client(redis, role=false)
      raise RedisException, "No redis client to duplicate."  unless redis
      raise RedisException, "Not a redis client" unless Redis === redis
      cl = redis.client
      raise RedisException, "Redis client doesn't have connection info (Can't get client info while in a redis.multi block... for now...)" unless cl.host
      r = Redis.new({
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

    def load_lua_script(redis, name, contents)
      begin
        hash = redis.script 'load', contents
      rescue Redis::CommandError => e
        raise ClientError, "Error loading script #{name}: #{e}"
      end
      raise Error, "Failed loading script #{name} onto server: mismatched hash" unless script_hash(name) == hash
    end
    private :load_lua_script
    
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
        load_lua_script redis, name, contents
      end
      
      def track_stats?
        false
      end
      def track_stats!
        raise Error, "Not supported right now"
      end
      def stats
        return false
      end
      attr_accessor :log_stats_per_request
      def log_stats_per_request?
        false
      end
      def log_stats_per_request!
        raise Error, "Not supported right now"
      end
      
      redis
    end
    
    def clear_cache!
      cleared = 0
      @models.each { |model| cleared += model.clear_cache! }
      cleared
    end

    def clear_queries!
      cleared = 0
      @models.each do |model|
        cleared += model.clear_queries! || 0
      end
      cleared
    end
    
    def clear!
      clear_cache! + clear_queries!
    end
    
    def info
      models.each &:info
    end
    
    #reconnect all redic clients
    def reconnect
      all_redises.each { |r| r.close }
    end
    def disconnect
      all_redises.each { |r| r.close }
    end
    
    def build_missing_indices!
      @models.each do |model|
        model.build_missing_redis_indices
      end
      self
    end
    
    #rebuild all known queris indices
    def rebuild!(clear=false)
      start = Time.now
      if Object.const_defined? 'Rails'
        Dir.glob("#{Rails.root}/app/models/*.rb").sort.each { |file| require_dependency file } #load all models
      end
      @models.each do |model| 
        if clear
          delkeys = redis.keys "#{model.prefix}*"
          redis.multi do |r|
            delkeys.each { |k| redis.del k }
          end
          puts "Deleted #{delkeys.count} #{self.name} keys for #{model.name}."
        end
        model.build_redis_indices nil, false
      end
      printf "All redis indices rebuilt in %.2f sec.\r\n", Time.now-start
      self
    end

    def all_redises; @redis_connections; end
    def redis_role(redis)
      @redis_by_role.each do |role, redises|
        if Redis::Client === redis
          return role if redises.map{|r| r.client}.member? redis
        else
          return role if redises.member? redis
        end
      end
    end
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
        raise  Error, "Unknown redis script #{name}." unless contents
        hash = Queris.digest contents
        @script_hash[name] = hash
      end
      hash
    end
    
    def run_script(script, redis, keys=[], args=[])
      begin
        redis.evalsha script_hash(script), keys, args
      rescue Redis::CommandError => e
        raise Redis::CommandError, e.to_s.gsub(/^ERR Error running script/, "ERR Error running script #{script}")
      end
    end
    
    def import_lua_script(name, contents)
      name=name.to_sym
      if redis_scripts[name]
        if redis_scripts[name]==contents
          raise Queris::Error, "Tried loading script #{name} more than once. this is disallowed."
        else
          raise Queris::Error, "A redis lua script names #{name} already exists."
        end
      else
        redis_scripts[name]=contents
      end
      all_redises.each do |r|
        binding.pry
        1+1.12
        load_lua_script r, name, contents
      end
    end
    #load redis lua scripts
    Dir[File.join(File.dirname(__FILE__),'../data/redis_scripts/*.lua')].each do |path|
      name = File.basename path, '.lua'
      script = IO.read(path)
      Queris.import_lua_script(name, script)
    end
  end
  
  class RedisStats
    class << self
      def record(redis, time)
        @time ||= {}
        @roundtrips ||= {}
        @time[redis] = (@time[redis] || 0) + time
        @roundtrips[redis] = (@roundtrips[redis] || 0) + 1
        if @querying
          @time_query ||= {}
          @roundtrips_query ||= {}
          @time_query[redis] = (@time_query[redis] || 0) + time
          @roundtrips_query[redis] = (@roundtrips_query[redis] || 0) + 1
        end
        self
      end
      def querying=(val)
        @querying=val
      end
      def time(redis)
        (@time || {})[redis.client] || 0
      end
      def query_time(redis)
        (@time_query || {})[redis.client] || 0
      end
      def roundtrips(redis)
        (@roundtrips || {})[redis.client] || 0
      end
      def query_roundtrips(redis)
        (@roundtrips_query || {})[redis.client] || 0
      end
      def reset
        (@time || {}).clear
        (@roundtrips || {}).clear
        (@roundtrips_query || {}).clear
        (@time_query || {}).clear
        self
      end
      def summary
        format = "%-10s %-7s %-10s %-5s %s"
        ret = Queris.all_redises.map do |r|
          format % [Queris.redis_role(r) || r.host, time(r).round(3), query_time(r).round(3), roundtrips(r), query_roundtrips(r)]
        end
        if ret.count>0
          ret.unshift(format % ["", "all", "query", "all", "query"])
          ret.unshift ("%-12s %-16s %s" % %w(Role Time(s) Roundtrips))
        end
        ret.empty? ? "no data" : ret.join("\r\n")
      end
      def totals(what=nil)
        t, rt = 0, 0
        Queris.all_redises.map do |r|
          t += time(r)
          rt += roundtrips(r)
        end
        if what == :time
          "time: #{t.round(3)}sec"
        elsif what == :roundtrips
          "roundtrips: #{rt}"
        else
          "time: #{t.round(3)}sec, roundtrips: #{rt}"
        end
      end
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
  if Object.const_defined? 'Rails'
    class QuerisTasks < Rails::Railtie
      rake_tasks do
        Dir[File.join(File.dirname(__FILE__),'tasks/*.rake')].each { |f| load f }
      end
    end
  end
end
