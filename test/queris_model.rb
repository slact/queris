$LOAD_PATH.unshift(File.expand_path(File.dirname(__FILE__) + "/../lib"))
require "redis"
require "queris"

class Foo < Queris::Model
  attrs :a, :b, :c
  redis Redis.new(:host => 'localhost', :port => 6379, :db => 13)
  index_attributes :a, :b, :index => Queris::DecayingAccumulatorIndex, :half_life => 604800
  profile_queries
end

def test
  f = Foo.new
  f.a = 11
  f.b = 9
  f.save
  
  q = Foo.query.union(:a, 11).diff(:a, 12)
  
  pr = Queris::QueryProfiler.find(q)
  
  binding.pry
end
test
