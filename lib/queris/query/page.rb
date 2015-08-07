module Queris
  class Query
    class Page
      attr_accessor :page, :range
      def initialize(prefix, sortops, page_size, ttl)
        @prefix=prefix
        @ops=sortops
        @pagesize=page_size
        @ttl=ttl
        @page=0
      end
      def key
        "#{@prefix}page:#{Queris.digest(@ops.join)}:#{@pagesize}:#{@page}"
      end
      def size; @pagesize; end
      def volatile_query_keys(q)
        [ q.results_key(:last_loaded_page) ]
      end
      def gather_data(redis, results_key, pagecount_key)
        #puts "gather page data for key #{results_key}"
        @current_count= Queris.run_script :multisize, redis, [results_key]
        @last_loaded_page ||= redis.get pagecount_key
        @total_count ||= redis.zcard source_key
      end
      def inspect_query(r, q)
        gather_data(r, q.results_key, q.results_key(:last_loaded_page))
        gather_ready_data(r, q)
      end
      def query_run_stage_after_run(r, q)
        puts "write last_loaded_page for #{q}"
        llp = fluxcap(@last_loaded_page)
        llp=llp.to_i if llp
        @last_loaded_page = @page
        r.set q.results_key(:last_loaded_page), fluxcap(@last_loaded_page)
        inspect_query(r, q)
      end
      def gather_ready_data(r, q)
        @ready = Queris.run_script(:paged_query_ready, r, [q.results_key, q.results_key(:exists), source_key, q.runstate_key(:ready)])
      end
      
      def seek
        last, cur_count = fluxcap(@last_loaded_page), fluxcap(@current_count)
        last=nil if last == ""
        last=last.to_i unless last.nil?
        cur_count=cur_count.to_i unless cur_count.nil?
        @key=nil
        if last.nil?
          @page=0
          puts "seeking next page... will be #{@page} last_loaded = #{last}, cur_count = #{cur_count}, total max = #{fluxcap @total_count}"
          false
        elsif cur_count < range.max && !no_more_pages?
          @page = last + 1
          puts "seeking next page... will be #{@page} last_loaded = #{last}, cur_count = #{cur_count}, total max = #{fluxcap @total_count}"
          false
        else
          puts "no need to seek, we are here"
          true
        end
      end
      def no_more_pages?
        last, cur_count = fluxcap(@last_loaded_page), fluxcap(@current_count)
        return nil if last == "" || last.nil?
        last=last.to_i
        (last + 1) * size > fluxcap(@total_count)
      end
      def ready?
        raise Error, "Asked if a page was ready without having set a desired range first" unless @range
        ready = (fluxcap(@current_count) || -Float::INFINITY) >= @range.max || no_more_pages?
        yield(self) if !ready && block_given?
        ready
      end
      
      
      def source_key
        raise NotImplemented, "paging by multiple sorts not yet implemented" if @ops.count > 1
        binding.pry if @ops.first.nil?
        @ops.first.keys[1]
      end
      def source_id
        Queris.digest source_key
      end
      def created?; @created==@page; end
      def create_page(redis)
        llp = fluxcap(@last_loaded_page)
        llp=nil if llp==""
        return if (llp && llp == @page)
        redis.eval("redis.log(redis.LOG_WARNING, 'want page #{@page}!!!!!!!!1')")
        Queris.run_script(:create_page_if_absent, redis, [key, source_key], [@pagesize * @page, @pagesize * (@page + 1) -1, @ttl])
      end
      private
      def fluxcap(val)#possible future value
        begin
          Redis::Future === val ? val.value : val
        rescue Redis::FutureNotReady
          nil
        end
      end
    end
  end
end

