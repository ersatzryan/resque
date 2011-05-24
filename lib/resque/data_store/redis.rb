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

      def to_s
        @redis.instance_variable_get(:@redis).id
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

      def peek(queue, start = 0, count = 1)
        list_range("queue:#{queue}", start, count)
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

      def get_stat(stat)
        @redis.get("stat:#{stat}").to_i
      end

      def clear_stat(stat)
        @redis.del("stat:#{stat}")
      end

      def increment_by(stat, by = 1)
        @redis.incrby("stat:#{stat}", by)
      end

      def workers(working_only = false)
        workers = Array(@redis.smembers(:workers))

        if working_only && !workers.empty?
          workers.map! {|name| "worker:#{name}"}
          workers = @redis.mapped_mget(*workers).reject {|key, value| value.nil? }.keys.map {|key| key.sub("worker:", '') }
        end

        workers.map {|id| find_worker(id)}.compact
      end

      def find_worker(worker_id)
        queues = worker_queues(worker_id)
        worker = Resque::Worker.new(*queues)
        worker.to_s = worker_id
        worker
      end

      def worker_queues(worker_id)
        worker_id.split(":")[-1].split(',')
      end

      def add_worker(worker)
        @redis.sadd(:workers, worker)
      end

      def start_worker(worker, string_time)
        @redis.set("worker:#{worker}:started", string_time)
      end

      def unregister_worker(worker)
        @redis.srem(:workers, worker)
        @redis.del("worker:#{worker}")
        @redis.del("worker:#{worker}:started")

        Stat.clear("processed:#{worker}")
        Stat.clear("failed:#{worker}")
      end

      def worker_payload(worker)
        decode(@redis.get("worker:#{worker}")) || {}
      end

      def worker_done(worker)
        @redis.del("worker:#{worker}")
      end

      def worker_working_on(worker, data)
        @redis.set("worker:#{worker}", encode(data))
      end

      def worker_started_at(worker)
        @redis.get("worker:#{worker}:started")
      end

      def worker_state(worker)
        @redis.exists("worker:#{worker}") ? :working : :idle
      end

      def worker_exists?(worker_id)
        @redis.sismember(:workers, worker_id)
      end

      def add_failure(data)
        @redis.rpush(:failed, encode(data))
      end

      def count_failures
        @redis.llen(:failed).to_i
      end

      def failures(start = 0, count = 1)
        list_range(:failed, start, count)
      end

      def clear_failures
        @redis.del(:failed)
      end

      def get_size_of(key)
        case @redis.type(key)
        when 'none'
          []
        when 'list'
          @redis.llen(key)
        when 'set'
          @redis.scard(key)
        when 'string'
          @redis.get(key).length
        when 'zset'
          @redis.zcard(key)
        end
      end

      def get_value(key, start=0)
        case @redis.type(key)
        when 'none'
          []
        when 'list'
          @redis.lrange(key, start, start + 20)
        when 'set'
          @redis.smembers(key)[start..(start + 20)]
        when 'string'
          @redis.get(key)
        when 'zset'
          @redis.zrange(key, start, start + 20)
        end
      end

      def collections
        @redis.keys("*").map do |key|
          key.sub("#{@redis.namespace}:", '')
        end
      end

    end
  end
end
