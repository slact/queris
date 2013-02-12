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
        def set_options(opt={})
          @opt=opt
        end
        def fval(val) #future value
          begin
            Redis::Future === val ? val.value : val
          rescue Redis::FutureNotReady => e
            "unavailable"
          end
        end
      end
      class TraceMessage < TraceBase
        def initialize(txt)
          @text=txt
        end
        def to_s
          "#{indent}#{fval @text}"
        end
      end
      class TraceOp < TraceBase #query operation tracer
        def initialize(redis, operation, operand, results_key)
          @redis=redis
          @op=operation
          @operand=operand
          @results_key=results_key
          if Array === @operand.key
            raise NotImplemented, "Only single-key operands can be traced." 
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
        def fval(name)
          val = @futures[name.to_sym]
          super val
        end
        def to_s
          op_info = fval(:operand_type) == 'none' ? "key absent" : "|#{fval :operand_type} key|=#{fval :operand_size}"
          op_key = "#{"[#{@index_key}] " if @opt[:keys]}"
          if @operand.is_query?
            "#{indent}#{@op.symbol} subquery<#{@operand.index.id}> #{op_key}(#{op_info}) => #{fval :results_size}\r\n" + 
            "#{@operand.index.trace(@opt.merge(:output => false, :indent => indentation + 1))}"
          else
            "#{indent}#{@op.symbol} #{@operand.index.name}#{@operand.value.nil? ? '' : "<#{@operand.value}>"} #{op_key}(#{op_info}) => #{fval :results_size}"
          end
        end
        
        private
        
      end
      
      attr_accessor :buffer
      def initialize(query, opt={})
        @query=query
        @buffer = []
        @indentation = 0
        @options= Hash === opt ? opt : {}
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
        t = TraceOp.new(@query.redis, *arg)
        t.set_options @options
        buffer << t
        self
      end
      def message(text)
        t = TraceMessage.new(text)
        t.set_options @options
        buffer << t
        self
      end
    end
    
  end
end
