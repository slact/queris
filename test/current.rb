$LOAD_PATH.unshift(File.expand_path(File.dirname(__FILE__) + "/../lib"))
require "redis/connection/hiredis"
require "redis"
require "queris"

class Foo < Queris::Model
  attrs :a, :b, :b2, :c
  redis Redis.new(:host => 'localhost', 
                  :port => 6379, 
                  :db => 13,
                 # :logger => Logger.new(STDOUT)
                 )
  index_attribute :a
  index_attributes :b, :b2, :index => Queris::DecayingAccumulatorIndex, :half_life => 604800
  index_range_attribute :c
  live_queries
  #profile_queries :lite
end

def test
  Queris.debug= true
  f = Foo.new
  f.a = rand
  f.b = 9
  f.b2 = 10
  f.c = 12
  f.save

  q = Foo.query :live => true
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

  r = Foo.query.union(:a, 1).union(:b,1).intersect(:c,3).intersect(Foo.query.union(:b2, 30).diff(:a, 10)).diff(:c, 15)
  r2 = Foo.query(live: true).union(:a, 1).union(:b, 22).sort("-c")
  r2.query
  o = Foo.new
  o.a= 1
  binding.pry
  r.member? o
end
test
