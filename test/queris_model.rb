$LOAD_PATH.unshift(File.expand_path(File.dirname(__FILE__) + "/../lib"))
require "redis"
require "queris"
Queris.use_redis Redis.new(:host => 'localhost', :port => 6379, :db => 13)

class Foo < Queris::Model
  attrs :a, :b, :c
  expire 1024
  def initialize
    
  end
  
end

def test
  


  f = Foo.new

  f.a = 11
  f.b = 9
  f.save
  binding.pry
end
test