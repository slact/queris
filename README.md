Nutshell
========
A Ruby object indexing and querying tool using Redis to store data and perform set operations.

What's all this about?
====
Redis is a flexible data structures server. It's pretty fast, but if you want to index something, you have to do it manually.
Queris can be bolted onto some Ruby objects (hashes, ActiveRecord models, Ohm objects, etc.) to maintain indices and perform queries. Attributes can be indexed into regular or sorted sets, allowing set selection on arbitrary data, and range selection on numeric data (if desired). More complex indexing, like n-gram text search, can be implemented client-side (and will be included in some future Queris version.)
Queries can perform set operations on all indices and other queries, and can be sorted. Each query can be cached for a custom duration. Queries can also be live (reflect realtime changes to complex set operations) in O(log(n)) server time.
Thus Queris offers customizeable indexing, and nestable, cacheable, optionally realtime, set queries (with a few more bells and whistles).

Learning By Example
===================
First connect to redis. Let's assume a Redis server on localhost at standard port 6379:
```ruby
  Queris.add_redis Redis.new
```
Let's say you have a User ActiveRecord object with some obvious attributes - id, name, email, age
```ruby
class User < ActiveRecord::Base
  include Queris
  
  #declare which attributes to index, and how
  index_attributes :name, :email   #simple set index
  index_range_attribute :age   #sorted set index
  index_attribute_from UserTags, :attribute => :tag, :key => :userId  #index from a different model
end
```
To build indices:
```ruby
  User.build_redis_indices
```
You can now query Users:
```ruby
young_bob_and_steve = User.query(:ttl=>2.days).union(:name, ["Steve", "Bob"]).intersect(:age, 0..30).diff(:email, "steve@example.org") #query expires in 2 days
#you can have subqueries, too:
bob_and_steve_and_bill = User.query.union(young_bob_and_steve).union(:name, "Bill")
#now get the results
bob_and_steve_and_bill.results #all results
bob_and_steve_and_bill.count #result count
bob_and_steve_and_bill.results(3..5) # results # 3 through 5
```
Queries can be expressed in set notation:
```ruby
bob_and_steve_and_bill.to_s # same as .explain
#  ((name<["Steve", "Bob"]> ∩ age<0..30> ∖ email<steve@example.org>) ∪ name<Bill>)
```

...more to follow...