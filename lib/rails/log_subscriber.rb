module Queris
  class LogSubscriber < ActiveSupport::LogSubscriber
    def self.runtime=(value)
      Thread.current["queris_redis_runtime"] = value
    end

    def self.runtime
      Thread.current["queris_redis_runtime"] ||= 0
    end

    def self.reset_runtime
      rt, self.runtime = runtime, 0
      rt
    end

    def command(event)
      self.class.runtime += event.duration
    end
  end
end

Queris::LogSubscriber.attach_to :queris
