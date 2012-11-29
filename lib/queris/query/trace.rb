# encoding: utf-8
module Queris
  class Query
    private
    class Trace
      class TraceBase
        def indent(n=nil)
          @indent=n if n
          "  " * indentation
        end
        def indentation
          @indent || 0
        end
      end
      class TraceMessage < TraceBase
        def initialize(txt)
          @text=txt
        end
        def to_s
          "#{indent}#{@text}"
        end
      end
      class TraceOp < TraceBase #query operation tracer
        def initialize(redis, operation, operand, results_key)
          @redis=redis
          @op=operation
          @operand=operand
          @results_key=results_key
          if Array === @operand.key
            binding.pry
            raise "Only single-key operands can be traced." 
          end
          prepare
        end
        
        def prepare
          @index_key = @op.operand_key @operand
          @futures= {
            :results_size => Queris.run_script(:multisize, @redis, [@results_key]),
            :results_type => @redis.type(@results_key),
            :operand_size => Queris.run_script(:multisize, @redis, [@index_key]),
            :operand_type => @redis.type(@index_key),
          }
        end
        def to_s
          op_info = fval(:operand_type) == 'none' ? "key absent" : "|#{fval :operand_type} key|=#{fval :operand_size}"
          if @operand.is_query?
            "#{indent}#{@op.symbol} subquery<#{@operand.index.id}> (#{op_info}) => #{fval :results_size}\r\n" + 
            "#{@operand.index.trace(indentation + 1)}"
          else
            "#{indent}#{@op.symbol} #{@operand.index.name}#{@operand.value.nil? ? '' : "<#{@operand.value}>"} (#{op_info}) => #{fval :results_size}"
          end
        end
        
        private
        def fval(name) #future value
          val = @futures[name.to_sym]
          begin
            Redis::Future === val ? val.value : val
          rescue Redis::FutureNotReady => e
            "unavailable"
          end
        end
      end
      
      attr_accessor :buffer
      def initialize(query)
        @query=query
        @buffer = []
        @indentation = 0
      end
      def to_s
        out=[]
        buffer.each do |line|
          out << line.to_s
          out << line.subquery.trace(@indentation + 1) if line.respond_to?(:subquery) && line.subquery
        end
        out.join "\r\n"
      end
      def indent(n=1)
        @indentation += n
        @buffer.each { |line| line.indent n }
        self
      end
      def op(*arg)
        buffer << TraceOp.new(@query.redis, *arg)
        self
      end
      def message(text)
        buffer << TraceMessage.new(text)
      end
    end
    
  end
end
