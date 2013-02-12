module Queris
  class Error < Exception; end
  class ClientError < Error; end
  class SchemaError < ClientError; end
  class ServerError < Error; end
  class RedisError < ServerError; end
  class NotImplemented < Error; end
end