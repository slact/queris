About
======
A flexible indexing and querying tool using Redis.

Why?
====
Redis is a flexible data structures server. It's pretty fast, but if you want to index something, you have to do it manually.
Queris can be bolted onto some Ruby objects (hashes, ActiveRecord models, etc.) to maintain indices and perform queries.

Learning By Example
===================

Let's say you have a User ActiveRecord object with some obvious attributes - id, name, email, age
```ruby
class User < ActiveRecord::Base
  include Queris

  # cache objects in Redis
  cache_all_attributes
  
  #declare which attributes to index, and how
  index_attributes :name, :email   #simple set indexing
  index_range_attribute :age   #sorted set index
  #...
end
```

You can now query Users:
```ruby
young_bob_and_steve = User.query.union(:name, ["Steve", "Bob"]).intersect(:age, 0..30).diff(:email, "steve@example.org")
#you can have subqueries, too:
bob_and_steve_and_bill = User.query.union(young_bob_and_steve).union(:name, "Bill")
#now get the results
bob_and_steve_and_bill.results #all results
bob_and_steve_and_bill.count #result count
bob_and_steve_and_bull.results(3..5) # results # 3 through 5
```

...more to follow...