$LOAD_PATH.unshift(File.expand_path(File.dirname(__FILE__) + "/../lib"))
require "redis/connection/hiredis"
require "redis"
require "queris"

Queris.add_redis :master, Redis.new(:host => 'localhost', 
                  :port => 6379, 
                  :db => 13,
                 # :logger => Logger.new(STDOUT)
                 )
#Queris.add_redis :metaquery, Redis.new(:host => 'willzyx', :port => 6380)
class Bar < Queris::Model
  attrs :foo_id, :bar
end
class Foo < Queris::Model
  attrs :a, :b, :b2, :c
  index_attribute name: :a, live: true
  index_attribute :name => :here, :index => Queris::ExpiringPresenceIndex, :ttl => 120, :live => true
  index_attributes :b, :b2, :index => Queris::DecayingAccumulatorIndex, :half_life => 604800
  index_range_attribute :c
  index_attribute_from model: Bar, name: :bar, :attribute => :bar, :key => :foo_id
  #live_queries
  #profile_queries :lite
end

def test
  Queris.debug= true
  f = Foo.new
  f.a = rand
  f.b = 9
  f.b2 = 10
  f.c = 12
  binding.pry
  f.save
  
  q = Foo.query :live => true, :ttl => 60
  q.union :a, 11
  q.union :a, 15
  q.diff :a, 12
  q.union :c, 9
  q.intersect :b, 12
  #q.sort :c
  
  subq = Foo.query.union(:b, 2).sort(:c)
  q.union(subq)
  q.sort(subq)
  q.intersect Foo.query.union(:b, 44).diff(:c, 1)
  q.params[:bar]=:baz
  q.params[:cc]=32

  d = q.marshal_dump
  m = Marshal.dump q

  l = Marshal.load m
  #pr = Queris::QueryProfilerLite.find(q)

  r = Foo.query(live: true, ttl: 30).union(:a, 1).union(:b,1).intersect(:c,3).intersect(Foo.query.union(:b2, 30).diff(:a, 10)).diff(:c, 15)
  r2 = Foo.query(live: true, ttl: 120).union(:a, 1).union(:b, 22).intersect(:here).sort("-c")
  hereq=Foo.query(live: true).union(:here).union(:a,1223)
  binding.pry
  r2.query
  o = Foo.new
  o.a= 1
  #qqq=Queris::QueryStore.query(Foo, realtime: true).union(:a).union(:b).union(:b2)
  #qqq.flush; qqq.count
  #mq=Queris::QueryStore.metaquery.union(:index)
  #mq.flush; mq.count
  binding.pry
  r.member? o
end
test
