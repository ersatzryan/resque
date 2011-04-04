require 'redis/namespace'

module Resque
  module DataStore
    class Redis < Base
      # Accepts:
      #   1. A 'hostname:port' string
      #   2. A 'hostname:port:db' string (to select the Redis db)
      #   3. A 'hostname:port/namespace' string (to set the Redis namespace)
      #   4. A redis URL string 'redis://host:port'
      #   5. An instance of `Redis`, `Redis::Client`, `Redis::DistRedis`,
      #      or `Redis::Namespace`.
      def initialize(server)
        if server.respond_to? :split
          if server =~ /redis\:\/\//
            redis = ::Redis.connect(:url => server, :thread_safe => true)
          else
            server, namespace = server.split('/', 2)
            host, port, db = server.split(':')
            redis = ::Redis.new(:host => host, :port => port,
                                :thread_safe => true, :db => db)
          end
          namespace ||= :resque

          @redis = ::Redis::Namespace.new(namespace, :redis => redis)
        elsif server.respond_to? :namespace=
          @redis = server
        else
          @redis = ::Redis::Namespace.new(:resque, :redis => server)
        end
      end

      def flush
        @redis.flushall
      end

      def push(queue, item)
        watch_queue(queue)
        @redis.rpush "queue:#{queue}", encode(item)
      end

      # Pops a job off a queue. Queue name should be a string.
      #
      # Returns a Ruby object.
      def pop(queue)
        decode @redis.lpop("queue:#{queue}")
      end

      # Used internally to keep track of which queues we've created.
      # Don't call this directly.
      def watch_queue(queue)
        @redis.sadd(:queues, queue.to_s)
      end

      # Given a queue name, completely deletes the queue.
      def remove_queue(queue)
        @redis.srem(:queues, queue.to_s)
        @redis.del("queue:#{queue}")
      end

      # Returns an array of all known Resque queues as strings.
      def queues
        @redis.smembers(:queues)
      end

      # Returns an integer representing the size of a queue.
      # Queue name should be a string.
      def size(queue)
        @redis.llen("queue:#{queue}").to_i
      end

      # Does the dirty work of fetching a range of items from a Redis list
      # and converting them into Ruby objects.
      def list_range(key, start = 0, count = 1)
        if count == 1
          decode @redis.lindex(key, start)
        else
          Array(@redis.lrange(key, start, start+count-1)).map do |item|
            decode item
          end
        end
      end

      def remove(queue, klass, *args)

        klass = klass.to_s
        queue = "queue:#{queue}"
        destroyed = 0

        if args.empty?
          @redis.lrange(queue, 0, -1).each do |string|
            if decode(string)['class'] == klass
              destroyed += @redis.lrem(queue, 0, string).to_i
            end
          end
        else
          destroyed += @redis.lrem(queue, 0, encode(:class => klass, :args => args))
        end

        destroyed
      end
    end
  end
end
