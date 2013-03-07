# encoding: utf-8
module Queris
  class Query
    private
    class Op #query operation
  
      class Operand #boilerplate
        attr_accessor :index, :value
        def initialize(op_index, val)
          @index = op_index
          @value = val
        end
        def marshal_dump
          [(Query === index ? index.id : index.name).to_sym, value]
        end
        def key
          index.key_for_query value
        end
        def optimized_key
          k=key
          @optimized||={}
          if Enumerable===k
            k.map! { |ky| @optimized[ky] || ky }
          else
            k = @optimized[k] || k
          end
          k
        end
        def split
          if Array === key && Enumerable === value
            raise ClientError, "Sanity check failed - different number of keys and values, bailing." if key.length != value.length
            value.map do |val|
              self.class.new(index, val)
            end
          else
            [ self ]
          end
        end
        def is_query?
          Query === @index
        end
        def gather_key_sizes
          @size={}
          k=index.key value #we want true index key, not key_for_query
          if Enumerable === k
            k.each { |k| @size[k]=index.key_size(k) }
          else
            @size[k]=index.key_size(k)
          end
        end
        def key_size(redis_key)
          raise ClientError "Attepted to get query operand key size, but it's not ready yet." unless Hash === @size
          s = @size[redis_key]
          return Redis::Future === s ? s.value : s
        end
        def preintersect(smallkey, mykey)
          #puts "preintersect #{self} with #{smallkey}"
          @preintersect||={}
          @optimized||={}
          @preintersect[mykey]=smallkey
          @optimized[mykey]="#{mykey}:optimized:#{Queris.digest mykey}"
        end
        def optimized?
          @optimized && !@optimized.empty?
        end
        attr_reader :optimization_key
        
        def run_optimizations(redis)
          if @preintersect
            @preintersect.each do |k, smallkey|
              #puts "running optimization - preintersecting #{k} and #{smallkey}"
              Queris.run_script(:query_intersect_optimization, redis, [@optimized[k], k, smallkey])
            end
            #puts "preintersected some stuff"
          else
            #puts "no optimizations to run"
          end
        end
        def optimized_temp_keys
          @optimized.nil? ? [] : @optimized.values
        end
        def json_redis_dump(op_name = nil)
          ret = []
          miniop = {}
          if is_query?
            miniop = {query: index.json_redis_dump}
          else
            index.json_redis_dump miniop
            if Range === @value && @index.handle_range?
              miniop[:min] = @index.val(@value.begin)
              miniop[@value.exclude_end? ? :max : :max_or_equal] = @index.val(@value.end)
              miniop[:key] = @index.key
            elsif Enumerable === @value
              @value.each do |val|
                miniop = { equal: @index.val(val), key: @index.key(val) }
                miniop[:op] = op_name if op_name
                ret << miniop
              end
              return ret
            else
              miniop[:equal] = @index.val(@value) unless miniop[:nocompare]
            end
          end
          miniop[:key] = @index.key(@value)
          miniop[:op]=op_name if op_name
          ret << miniop
          ret
        end
      end

      attr_accessor :operands, :fragile
      def initialize(fragile=false)
        @operands = []
        @keys = []
        @weights = []
        @subqueries = []
        @fragile = fragile
      end
      def push(index, val) # push operand
        @ready = nil
        @operands << Operand.new(index,val)
        self
      end
      def symbol
        @symbol || self.class::SYMBOL
      end
      def command
        @command || self.class::COMMAND
      end
      def keys(target_key=nil, first = nil)
        prepare
        @keys[0]=target_key unless target_key.nil?
        first ? @keys[1..-1] : @keys
      end
      def weights(first = nil)
        prepare
        first ? @weights[1..-1] : @weights
      end
      def target_key_weight
        1
      end
      def operand_key_weight(op)
        1
      end
      def optimized_temp_keys
        optimized = []
        operands.each { |op| optimized |= op.optimized_temp_keys }
        optimized
      end
      def subqueries
        prepare
        @subqueries || []
      end
      def optimize(smallkey, smallsize)
        #optimization walker. doesn't really do much unless given a decision block
        @optimized = nil
        operands.each do |op|
          idx = op.index
          key = idx.key op.value
          if Enumerable === key
            key.each do |k|
              yield k, op.key_size(k), op if block_given?
            end
          else
            yield key, op.key_size(key), op if block_given?
          end
          if op.optimized?
            @optimized = true
            notready!
          end
        end
        return smallkey, smallsize
      end
      def optimized?
        @optimized
      end
      def notready!
        @ready=nil; self
      end
      private :notready!
      def prepare
        return if @ready
        @keys, @weights, @subqueries = [:result_key], [target_key_weight], []
        operands.each do |op|
          k = block_given? ? yield(op) : op.optimized_key
          num_keys = @keys.length
          if Array === k
            @keys |= k
          else
            @keys << k
          end
          if (@keys.length - num_keys) < 0
            raise ArgumentError, "something really wrong here"
          end
          @weights += [ operand_key_weight(op) ] * (@keys.length - num_keys)
          @subqueries << op.index if Query === op.index
        end
        @ready = true
      end
      def notready!
        @ready = nil
        self
      end
      def operand_key(op)
        op.index.key_for_query op.value
      end
      def run(redis, target, first=false, trace_callback=false)
        subqueries_on_slave = !subqueries.empty? && redis != Queris.redis(:master)
        
        if subqueries_on_slave || optimized?
          #prevent dummy result string on master from race-conditioning its way into the query
          redis.multi
          Queris.run_script :delete_if_string, redis, subqueries.map{|s| s.key} if subqueries_on_slave
        end
        operands.each do |op| 
          op.run_optimizations(redis)
          op.index.before_query_op(redis, target, op.value, op) if op.index.respond_to? :before_query_op 
        end
        unless trace_callback
          redis.send self.class::COMMAND, target, keys(target, first), :weights => weights(first)
        else
          operands.each do |operand|
            operand.split.each do |op| #ensure one key per operand
              keys = [ target, op.key ]
              weights = [target_key_weight, operand_key_weight(op)]
              if first
                keys.shift; weights.shift
                first = false
              end
              redis.send self.class::COMMAND, target, keys, :weights => weights
              trace_callback.call(self, op, target) if trace_callback
            end
          end
        end
        if subqueries_on_slave || optimized?
          redis.exec
        end
        operands.each { |op| op.index.after_query_op(redis, target, op.value, op) if op.index.respond_to? :after_query_op }
      end

      def marshal_dump
        [self.class::SYMBOL, operands.map {|op| op.marshal_dump}]
      end
      def json_redis_dump(etc={})
        all_ops = []
        operands.map do |op|
          all_ops.concat op.json_redis_dump(self.class::NAME)
        end
        all_ops
      end
      def to_s
        "#{symbol} #{operands.map{|o| Query === o.index ? o.index : "#{o.index.name}<#{o.value}>"}.join(" #{symbol} ")}"
      end
    end
    
    class UnionOp < Op
      COMMAND = :zunionstore
      SYMBOL = :'∪'
      NAME = :union
      
      OPTIMIZATION_THRESHOLD_MULTIPLIER = 3
      def optimize(smallkey, smallsize)
        super do |key, size, op|
          if smallsize * self.class::OPTIMIZATION_THRESHOLD_MULTIPLIER < size
            #puts "optimization reduced union(?) operand from #{size} to #{smallsize}"
            op.preintersect(smallkey, key)
          end
        end
        return smallkey, smallsize
      end
    end
    class IntersectOp < Op
      COMMAND = :zinterstore
      SYMBOL = :'∩'
      NAME = :intersect
      
      OPTIMIZATION_THRESHOLD_MULTIPLIER = 5
      def optimize(smallkey, smallsize)
        smallestkey, smallestsize, smallestop = Float::INFINITY, Float::INFINITY, nil
        super do |key, size, op|
          smallestkey, smallestsize, smallestop = key, size, op if size < smallestsize
        end
        if smallsize * self.class::OPTIMIZATION_THRESHOLD_MULTIPLIER < smallestsize
          #puts "optimization reduced intersect operand from #{smallestsize} to #{smallsize}"
          smallestop.preintersect(smallkey, smallestkey)
        end
        if smallestsize < smallsize
          #puts "found a smaller intersect key: |#{smallestkey}|=#{smallestsize}"
          return smallestkey, smallestsize
        else
          return smallkey, smallsize
        end
      end
    end
    class DiffOp < UnionOp
      SYMBOL = :'∖'
      NAME = :diff
      
      def target_key_weight; 0; end
      def operand_key_weight(op=nil); :'-inf'; end
      def run(redis, result_key, *arg)
        super redis, result_key, *arg
        redis.zremrangebyscore result_key, :'-inf', :'-inf'
        # BUG: sorted sets with -inf scores will be treated incorrectly when diffing
      end
    end
    class SortOp < Op
      COMMAND = :zinterstore
      SYMBOL = :sortby
      def json_redis_dump
        operands.map do |op|
          {key: op.index.key, multiplier: op.value}
        end
      end
      def push(index, reverse=nil)
        super(index, reverse ? -1 : 1)
      end
      def target_key_weight; 0; end
      def operand_key_weight(op); op.value; end
      def prepare
        #don't trigger the rangehack
        super { |op| op.index.key op.value }
      end
      def operand_key(op)
        op.index.key op.value
      end
      def run(redis, target, first=false, trace_callback=nil)
        sort_keys = keys(target, first)
        redis.send self.class::COMMAND, target, sort_keys, :weights => weights(first)
        if trace_callback
          raise NotImplemented, "Can't trace multi-sorts yet." if sort_keys.count > 2 || operands.count > 1
          trace_callback.call(self, operands.first, target)
        end
      end
    end
  end
end
