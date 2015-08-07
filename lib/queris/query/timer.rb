module Queris
  class Query
    class Timer
      def initialize
        @time_start={}
        @time={}
        @times_recorded={}
      end
      def start(attr)
        attr = attr.to_sym
        @time_start[attr]=Time.now.to_f
        @time[attr] ||= '?'
      end
      def finish(attr)
        attr = attr.to_sym
        start_time = @time_start[attr]
        raise "Query Profiling timing attribute #{attr} was never started." if start_time.nil?
        t = Time.now.to_f - start_time
        @time_start[attr]=nil
        record attr, (Time.now.to_f - start_time)
      end
      def record(attr, val)
        attr = attr.to_sym
        @times_recorded[attr]||=0
        @times_recorded[attr]+=1
        @time[attr]=0 if @time[attr].nil? || @time[attr]=='?'
        @time[attr]+=val
      end
      def to_s
        mapped = @time.map do |k,v|
          v = v.round(4) if Numeric === v
          if (times = @times_recorded[k]) != 1
            "#{k}(#{times} times):#{v}"
          else
            "#{k}:#{v}"
          end
        end
        mapped.join ", "
      end
    end
  end
end