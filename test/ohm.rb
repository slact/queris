$LOAD_PATH.unshift(File.expand_path(File.dirname(__FILE__) + "/../lib"))
require "redis"
require "queris"
require "ohm"
require "ohm/contrib"

Queris.add_redis :master, Redis.new(:host => 'localhost', 
                  :port => 6379, 
                  :db => 13,
                 # :logger => Logger.new(STDOUT)
                 )
Ohm.connect url: "redis://localhost:6379/13"

class Foo < Ohm::Model
  attribute :fooo
end

class Bar < Ohm::Model
  attribute :foo_id
  attribute :bar
  list :foolist, Foo
  attribute :score
  include Queris
  index_attributes :foo_id, :bar, :tags, :somelist
  index_range_attribute :score
end

def test
  b = Bar.new
  b.score=55
  b.bar="a"
  b.save
  binding.pry
end
test
 
