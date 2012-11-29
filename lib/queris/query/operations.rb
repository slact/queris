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
        def split
          if Array === key && Enumerable === value
            raise "uhoh" if key.length != value.length
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
      def prepare
        return if @ready
        @keys, @weights = [:result_key], [target_key_weight]
        operands.each do |op|
          k = block_given? ? yield(op) : op.key
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
        end
        @ready = true
      end
      def operand_key(op)
        op.index.key_for_query op.value
      end
      def run(redis, target, first=false, trace_callback=false)
        operands.each { |op| op.index.before_query_op(redis, target, op.value, op) if op.index.respond_to? :before_query_op }
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
    end
    class IntersectOp < Op
      COMMAND = :zinterstore
      SYMBOL = :'∩'
      NAME = :intersect
    end
    class DiffOp < Op
      COMMAND = :zunionstore
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
          raise "Can't trace multi-sorts yet." if sort_keys.count > 2 || operands.count > 1
          trace_callback.call(self, operands.first, target)
        end
      end
    end
  end
end
