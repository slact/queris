$LOAD_PATH.unshift(File.expand_path(File.dirname(__FILE__) + "/../lib"))
require "redis"
require "queris"

class Foo < Queris::Model
  attrs :a, :b, :b2, :c
  redis Redis.new(:host => 'localhost', :port => 6379, :db => 13, :logger => Logger.new(STDOUT))
  index_attribute :a
  index_attributes :b, :b2, :index => Queris::DecayingAccumulatorIndex, :half_life => 604800
  index_range_attribute :c
  #profile_queries :lite
end

def test
  f = Foo.new
  f.a = rand
  f.b = 9
  f.b2 = 10
  f.c = 12
  f.save
  q = nil
  1000.times do 
    q = Foo.query do
      union :a, 11
      union :a, 15
      diff :a, 12
      union :c, 9
      intersect :b, 12
    end
  end
  binding.pry
  #pr = Queris::QueryProfilerLite.find(q)
end

test
