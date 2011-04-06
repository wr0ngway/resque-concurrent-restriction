module Resque
  module Plugins
    module ConcurrentRestriction

      # Used by the user in their job class to set the concurrency limit
      def concurrent(limit)
        @concurrent = limit
      end

      # Allows the user to specify the unique key that identifies a set
      # of jobs that share a concurrency limit.  Defaults to the job class name
      def concurrent_identifier(*args)
        self.to_s
      end

      # Used to query what the limit the user has set
      def concurrent_limit
        @concurrent ||= 1
      end

      # The key used to acquire a lock so we can operate on multiple
      # redis structures (runnables set, running_count) atomically
      def lock_key(tracking_key)
        parts = tracking_key.split(":")
        "concurrent:lock:#{parts[2..-1].join(':')}"
      end

      # The redis key used to store the number of currently running
      # jobs for the restriction_identifier
      def running_count_key(*args)
        "concurrent:count:#{self.concurrent_identifier(*args)}"
      end

      # The key for the redis list where restricted jobs for the given resque queue are stored
      def restriction_queue_key(queue, tracking_key)
        parts = tracking_key.split(":")
        "concurrent:queue:#{queue}:#{parts[2..-1].join(':')}"
      end

      def restriction_queue_availability_key(tracking_key)
        parts = tracking_key.split(":")
        "concurrent:queue_availability:#{parts[2..-1].join(':')}"
      end

      # The key that groups all jobs of the same restriction_identifier together
      # so that we can work on any of those jobs if they are runnable
      # Stored in runnables set, and used to build keys for each queue where jobs
      # for those queues are stored
      def tracking_key(*args)
        "concurrent:tracking:#{self.concurrent_identifier(*args)}"
      end

      # The key to the redis set where we keep a list of runnable tracking_keys
      def runnables_key(queue=nil)
        key = ":#{queue}" if queue
        "concurrent:runnable#{key}"
      end

      # Encodes the job intot he restriction queue
      def encode(job)
        item = {:queue => job.queue, :payload => job.payload}
        Resque.encode(item)
      end

      # Decodes the job from the restriction queue
      def decode(str)
        item = Resque.decode(str)
        Resque::Job.new(item['queue'], item['payload']) if item
      end

      # The restriction queues that have data for each tracking key
      # Adds/Removes the queue to the list of queues for that tracking key
      # so we can quickly tell in next_runnable_job if a runnable job exists on a
      # specific restriction queue
      def track_queue(tracking_key, queue, action)
        case action
          when :add then Resque.redis.send(:sadd, restriction_queue_availability_key(tracking_key), queue)
          when :remove then Resque.redis.send(:srem, restriction_queue_availability_key(tracking_key), queue)
          else raise "Invalid action to ConcurrentRestriction.track_queue"
        end
      end

      # Pushes the job to the restriction queue
      def push_to_restriction_queue(job, location=:back)
        tracking_key = tracking_key(*job.args)

        case location
          when :back then Resque.redis.rpush(restriction_queue_key(job.queue, tracking_key), encode(job))
          when :front then Resque.redis.lpush(restriction_queue_key(job.queue, tracking_key), encode(job))
          else raise "Invalid location to ConcurrentRestriction.push_to_restriction_queue"
        end

        track_queue(tracking_key, job.queue, :add)
      end

      # Pops a job from the restriction queue
      def pop_from_restriction_queue(queue, tracking_key)
        str = Resque.redis.lpop(restriction_queue_key(queue, tracking_key))

        track_queue(tracking_key, queue, :remove)

        decode(str)
      end

      # Returns the number of jobs currently running
      def running_count(*args)
        Resque.redis.get(running_count_key(*args)).to_i
      end

      # Grabs the raw data (undecoded) from the restriction queue
      def restriction_queue_raw(queue, *args)
        Array(Resque.redis.lrange(restriction_queue_key(queue, tracking_key(*args)), 0, -1))
      end

      # Grabs the contents of the restriction queue (decoded)
      def restriction_queue(queue, *args)
        restriction_queue_raw(queue, *args).collect {|s| decode(s) }
      end

      # Returns the list of tracking_keys that have jobs waiting to run (are not over the concurrency limit)
      def runnables(queue=nil)
        Resque.redis.smembers(runnables_key(queue))
      end

      # Keeps track of which jobs are currently runnable, that is the
      # tracking_key should have jobs on some restriction queue and
      # also have less than concurrency_limit jobs running
      #
      def mark_runnable(runnable, *args)
        tracking_key = tracking_key(*args)
        queues = Resque.redis.smembers(restriction_queue_availability_key(tracking_key))
        queues.each do |queue|
          runnable_queues_key = runnables_key(queue)
          if runnable
            Resque.redis.sadd(runnable_queues_key, tracking_key)
          else
            Resque.redis.srem(runnable_queues_key, tracking_key)
          end
        end
        if runnable
          Resque.redis.sadd(runnables_key, tracking_key) if queues.size > 0
        else
          Resque.redis.srem(runnables_key, tracking_key)
        end
      end

      # Acquires a lock using the given key and lock expiration time
      def acquire_lock(lock_key, lock_expiration)
        # acquire the lock to work on the restriction queue
        expiration_time = lock_expiration + 1
        acquired_lock = Resque.redis.setnx(lock_key, expiration_time)

        # If we didn't acquire the lock, check the expiration as described
        # at http://redis.io/commands/setnx
        if ! acquired_lock
          # If expiration time is in the future, then someone else beat us to getting the lock
          old_expiration_time = Resque.redis.get(lock_key)
          return false if old_expiration_time.to_i > Time.now.to_i

          # if expiration time was in the future when we set it, then someone beat us to it
          old_expiration_time = Resque.redis.getset(lock_key, expiration_time)
          return false if old_expiration_time.to_i > Time.now.to_i
        end

        # expire the lock eventually so we clean up keys - not needed to timeout
        # lock, just to keep redis clean for locks that aren't being used'
        Resque.redis.expireat(lock_key, expiration_time + 300)

        return true
      end

      # Releases the lock acquired by #acquire_lock
      def release_lock(lock_key, lock_expiration)
        # Only delete the lock if the one we created hasn't expired
        expiration_time = lock_expiration + 1
        Resque.redis.del(lock_key) if expiration_time > Time.now.to_i
      end


      # Uses a lock to ensure that a sequence of redis operations happen atomically
      # We don't use watch/multi/exec as it doesn't work in a DistributedRedis setup
      def run_atomically(lock_key)
        trying = true

        while trying do
          lock_expiration = Time.now.to_i + 10
          if acquire_lock(lock_key, lock_expiration)
            begin
              yield
            ensure
              release_lock(lock_key, lock_expiration)
            end
            trying = false
          else
            sleep (rand(1000) * 0.0001)
          end
        end

      end

      # Pushes the job to restriction queue if it is restricted
      # If the job is within the concurrency limit, thus needs to be run, we
      # keep the running count incremented so that other calls don't erroneously
      # see a lower value and run their job.  This count gets decremented by call
      # to release_restriction when job completes
      def stash_if_restricted(job)
        restricted = false
        args = job.args
        count_key = running_count_key(*args)
        tracking_key = tracking_key(*args)
        lock_key = lock_key(tracking_key)

        run_atomically(lock_key) do

          # increment by one to see if we are allowed to run
          value = Resque.redis.incr(count_key)
          restricted = (value > concurrent_limit)

          mark_runnable(!restricted, *args)

          if restricted
            push_to_restriction_queue(job)
            Resque.redis.decr(count_key)
          end

        end

        return restricted
      end

      # Returns the next job that is runnable
      def next_runnable_job(queue)
        tracking_key = Resque.redis.srandmember(runnables_key(queue))
        return nil unless tracking_key

        job = nil
        lock_key = lock_key(tracking_key)

        run_atomically(lock_key) do
            # since we don't have a lock when we get the runnable,
            # we need to check it again
            still_runnable = Resque.redis.sismember(runnables_key(queue), tracking_key)
            if still_runnable

              job = pop_from_restriction_queue(queue, tracking_key)
              if job
                klazz = job.payload_class
                args = job.args
                count_key = klazz.running_count_key(*args)

                # increment by one to see if we are allowed to run
                value = Resque.redis.incr(count_key)
                restricted = (value > klazz.concurrent_limit)

                klazz.mark_runnable(!restricted, *args)

                # this shouldn't happen here since we should be only operating on
                # a queue that is runnable, but play it safe and repush if restricted
                if restricted
                  klazz.push_to_restriction_queue(job, :front)
                  Resque.redis.decr(count_key)
                  job = nil
                end
              end
            end

        end

        return job
          
      end

      # Decrements the running_count - to be called at end of job
      def release_restriction(job)
        args = job.args
        tracking_key = tracking_key(*args)
        lock_key = lock_key(tracking_key)

        run_atomically(lock_key) do

          # decrement the count after a job has run
          key = running_count_key(*args)
          value = Resque.redis.decr(key)
          Resque.redis.set(key, 0) if value < 0
          restricted = (value >= concurrent_limit)
          mark_runnable(!restricted, *args)
          
        end
      end

      def stats
        results = {}

        queue_keys = Resque.redis.keys("concurrent:queue:*")

        queue_sizes = {}
        ident_sizes = {}
        queue_keys.each do |k|
          parts = k.split(":")
          ident = parts[3..-1].join(":")
          queue_name = parts[2]
          size = Resque.redis.llen(k)
          queue_sizes[queue_name] ||= 0
          queue_sizes[queue_name] += size
          ident_sizes[ident] ||= 0
          ident_sizes[ident] += size
        end

        count_keys = Resque.redis.keys("concurrent:count:*")
        running_counts = {}
        count_keys.each do |k|
          parts = k.split(":")
          ident = parts[2..-1].join(":")
          running_counts[ident] = Resque.redis.get(k).to_i
        end

        lock_keys = Resque.redis.keys("concurrent:lock:*")
        lock_count = lock_keys.size

        runnable_count = Resque.redis.scard(runnables_key)

        return {
            :queue_totals => {
                :by_queue_name => queue_sizes,
                :by_identifier => ident_sizes
            },
            :running_counts => running_counts,
            :lock_count => lock_count,
            :runnable_count => runnable_count,
        }
        
      end

    end

    # Allows users to subclass instead of extending in their job classes
    class ConcurrentRestrictionJob
      extend ConcurrentRestriction
    end

  end
end
