class Foo
  attr_accessor :wee
  def initialize
    @cb=[]
    @wee = :foo
  end
    
  def add(cb=nil)
    cb = Proc.new if cb.nil? && block_given?
    raise "invalid cb" unless cb.respond_to?(:call) && cb.respond_to(:arity)
    @cb << cb unless cb.nil?
  end
  
  def run(foo)
    @cb.each do |cb|
      binding.pry unless cb.respond_to? :call
      cb.call(foo)
    end
  end
  
end

class Bar
  attr_accessor :wee
  def initialize
    @wee = :bar
  end
end
$f = Foo.new
$b = Bar.new

$f.add do |x| puts "#{x} and then some" end
beezwax = "buzz"
$f.add do |x| puts "#{x} and also #{beezwax}" end
$f.add Bar.new
$f.add $b.wee


$f.run 111