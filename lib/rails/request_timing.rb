module Queris
  module ControllerRuntime
    extend ActiveSupport::Concern

    protected

    def append_info_to_payload(payload)
      super
      payload[:redis_queris_runtime] = Queris::LogSubscriber.reset_runtime
      if Queris.log_stats_per_request?
        payload[:queris_stats] = Queris::RedisStats.summary
        Queris::RedisStats.reset
      end
    end

    module ClassMethods
      def log_process_action(payload)
        messages, queris_runtime = super, payload[:redis_queris_runtime]
        logger.info "Queris stats by server\r\n" + payload[:queris_stats] if payload[:queris_stats]
        messages << ("Redis via Queris: %.1fms" % queris_runtime.to_f) if queris_runtime
        messages
      end
    end
  end
end

ActiveSupport.on_load(:action_controller) do
  include Queris::ControllerRuntime
end
