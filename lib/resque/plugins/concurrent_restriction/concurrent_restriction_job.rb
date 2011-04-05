module Resque
  module Plugins
    module ConcurrentRestriction

      # Used by the user in their job class to set the concurrency limit
      def concurrent(limit)
        @concurrent = limit
      end

      # Allows the user to specify the unique key that identifies a set
      # of jobs that share a concurrency limit.  Defaults to the job class name
      def restriction_identifier(*args)
        self.to_s
      end

      # Used to query what the limit the user has set
      def concurrent_limit
        @concurrent ||= 1
      end

      # The key used to acquire a lock so we can operate on multiple
      # redis structures (runnables set, running_count) atomically
      def lock_key(tracking_key)
        "#{tracking_key}:lock"
      end

      # The redis key used to store the number of currently running
      # jobs for the restriction_identifier
      def running_count_key(*args)
        "restriction:count:#{self.restriction_identifier(*args)}"
      end

      # The key for the redis list where restricted jobs for the given resque queue are stored
      def restriction_queue_key(queue, tracking_key)
        "#{tracking_key}:#{queue}"
      end

      # The key that groups all jobs of the same restriction_identifier together
      # so that we can work on any of those jobs if they are runnable
      # Stored in runnables set, and used to build keys for each queue where jobs
      # for those queues are stored
      def tracking_key(*args)
        "restriction:tracking:#{self.restriction_identifier(*args)}"
      end

      # The key to the redis set where we keep a list of runnable tracking_keys
      def runnables_key
        "restriction:runnable"
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

      # Pushes the job to the restriction queue
      def push_to_restriction_queue(job, location=:back)
        case location
          when :back then Resque.redis.rpush(restriction_queue_key(job.queue, tracking_key(*job.args)), encode(job))
          when :front then Resque.redis.lpush(restriction_queue_key(job.queue, tracking_key(*job.args)), encode(job))
          else raise "Invalid location to ConcurrentRestriction.push_to_restriction_queue"
        end
      end

      # Pops a job from the restriction queue
      def pop_from_restriction_queue(queue, tracking_key)
        str = Resque.redis.lpop(restriction_queue_key(queue, tracking_key))
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
      def runnables
        Resque.redis.smembers(runnables_key)
      end

      # Keeps track of which jobs are currently runnable
      #
      def mark_runnable(runnable, *args)
        if runnable
          Resque.redis.sadd(runnables_key, tracking_key(*args))
        else
          Resque.redis.srem(runnables_key, tracking_key(*args))
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
            sleep (rand(1000) * 0.001)
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
        tracking_key = Resque.redis.srandmember(runnables_key)
        return nil unless tracking_key

        job = nil
        lock_key = lock_key(tracking_key)

        run_atomically(lock_key) do
            # since we don't have a lock when we get the runnable,
            # we need to check it again
            still_runnable = Resque.redis.sismember(runnables_key, tracking_key)
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
                # a queue that is runnable, but lay it safe and repush if restricted
                if restricted
                  klazz.push_to_restriction_queue(job, :front)
                  Resque.redis.decr(count_key)
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


    end

    # Allows users to subclass instead of extending in their job classes
    class ConcurrentRestrictionJob
      extend ConcurrentRestriction
    end

  end
end
