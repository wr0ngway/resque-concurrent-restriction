# To configure resque concurrent restriction, add something like the
# following to an initializer (defaults shown):
#
#    Resque::Plugins::ConcurrentRestriction.configure do |config|
#      # The lock timeout for the restriction queue lock
#      config.lock_timeout = 60
#      # Try to pick jobs off of the restricted queue before normal queues
#      config.restricted_before_queued = true
#    end

module Resque
  module Plugins
    module ConcurrentRestriction

      # Allows configuring via class accessors
      class << self
        # optional
        attr_accessor :lock_timeout, :restricted_before_queued
      end

      # default values
      self.lock_timeout = 60
      self.restricted_before_queued = false

      # Allows configuring via class accessors
      def self.configure
        yield self
      end

      # Redis Data Structures
      #
      # concurrent.lock.tracking_id => timestamp
      #   Maintains the distributed lock for the tracking_key to ensure
      #   atomic modification of other data structures
      #
      # concurrent.count.tracking_id => count
      #   The count of currently running jobs for the tracking_id
      #
      # concurrent.queue.queue_name.tracking_id => List[job1, job2, ...]
      #   The queue of items that is currently unable to run due to count being exceeded
      #
      # concurrent.queue_availability.tracking_key => Set[queue_name1, queue_name2, ...]
      #   Maintains the set of queues that currently have something
      #   runnable for each tracking_id
      #
      # concurrent.runnable[.queue_name] => Set[tracking_id1, tracking_id2, ...]
      #   Maintains the set of tracking_ids that have something
      #   runnable for each queue (globally without .queue_name postfix in key)
      #
      # The behavior has two points of entry:
      #
      # When the Resque::Worker is looking for a job to run from a restriction
      # queue, we use the queue_name to look up the set of tracking IDs that
      # are currently runnable for that queue.  If we get a tracking id, we
      # know that there is a restriction queue with something runnable in it,
      # and we then use that tracking_id and queue to look up and pop something
      # off of the restriction queue.
      #
      # When the Resque::Worker gets a job off of a normal resque queue, it uses
      # the count to see if that job is currently restricted.  If not, it runs it
      # as normal, but if it is restricted, then it sticks it on a restriction queue.
      #
      # In both cases, before a job is handed off to resque to be run, we increment
      # the count so we can keep tracking of how many are currently running.  When
      # the job finishes, we then decrement the count.

      # Used by the user in their job class to set the concurrency limit
      def concurrent(limit)
        @concurrent = limit
      end

      # Allows the user to specify the unique key that identifies a set
      # of jobs that share a concurrency limit.  Defaults to the job class name
      def concurrent_identifier(*args)
      end

      # Used to query what the limit the user has set
      def concurrent_limit
        @concurrent ||= 1
      end

      # The key used to acquire a lock so we can operate on multiple
      # redis structures (runnables set, running_count) atomically
      def lock_key(tracking_key)
        parts = tracking_key.split(".")
        "concurrent.lock.#{parts[2..-1].join('.')}"
      end

      # The redis key used to store the number of currently running
      # jobs for the restriction_identifier
      def running_count_key(tracking_key)
        parts = tracking_key.split(".")
        "concurrent.count.#{parts[2..-1].join('.')}"
      end

      # The key for the redis list where restricted jobs for the given resque queue are stored
      def restriction_queue_key(tracking_key, queue)
        parts = tracking_key.split(".")
        "concurrent.queue.#{queue}.#{parts[2..-1].join('.')}"
      end

      # The redis key used to store the aggregate number of jobs
      # in restriction queues by queue name
      def queue_count_key
        "concurrent.queue_counts"
      end

      def restriction_queue_availability_key(tracking_key)
        parts = tracking_key.split(".")
        "concurrent.queue_availability.#{parts[2..-1].join('.')}"
      end

      # The key that groups all jobs of the same restriction_identifier together
      # so that we can work on any of those jobs if they are runnable
      # Stored in runnables set, and used to build keys for each queue where jobs
      # for those queues are stored
      def tracking_key(*args)
        id = concurrent_identifier(*args)
        id = ".#{id}" if id && id.strip.size > 0
        "concurrent.tracking.#{self.to_s}#{id}"
      end

      def tracking_class(tracking_key)
        Resque.constantize(tracking_key.split(".")[2])
      end

      # The key to the redis set where we keep a list of runnable tracking_keys
      def runnables_key(queue=nil)
        key = ".#{queue}" if queue
        "concurrent.runnable#{key}"
      end

      # Encodes the job into the restriction queue
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
      def update_queues_available(tracking_key, queue, action)
        availability_key = restriction_queue_availability_key(tracking_key)
        case action
          when :add then Resque.redis.send(:sadd, availability_key, queue)
          when :remove then Resque.redis.send(:srem, availability_key, queue)
          else raise "Invalid action to ConcurrentRestriction.track_queue"
        end
      end

      def queues_available(tracking_key)
        availability_key = restriction_queue_availability_key(tracking_key)
        Resque.redis.smembers(availability_key)
      end

      # Pushes the job to the restriction queue
      def push_to_restriction_queue(job, location=:back)
        tracking_key = tracking_key(*job.args)

        case location
          when :back then Resque.redis.rpush(restriction_queue_key(tracking_key, job.queue), encode(job))
          when :front then Resque.redis.lpush(restriction_queue_key(tracking_key, job.queue), encode(job))
          else raise "Invalid location to ConcurrentRestriction.push_to_restriction_queue"
        end

        increment_queue_count(job.queue)
        update_queues_available(tracking_key, job.queue, :add)
        mark_runnable(tracking_key, false)
      end

      # Pops a job from the restriction queue
      def pop_from_restriction_queue(tracking_key, queue)
        queue_key = restriction_queue_key(tracking_key, queue)
        str = Resque.redis.lpop(queue_key)
        post_pop_size = Resque.redis.llen(queue_key)

        if post_pop_size == 0
          update_queues_available(tracking_key, queue, :remove)
          clear_runnable(tracking_key, queue)
        end

        decrement_queue_count(queue)

        # increment by one to indicate that we are running
        # do this before update_queues_available so that the current queue gets cleaned
        increment_running_count(tracking_key) if str

        decode(str)
      end

      # Grabs the raw data (undecoded) from the restriction queue
      def restriction_queue_raw(tracking_key, queue)
        Array(Resque.redis.lrange(restriction_queue_key(tracking_key, queue), 0, -1))
      end

      # Grabs the contents of the restriction queue (decoded)
      def restriction_queue(tracking_key, queue)
        restriction_queue_raw(tracking_key, queue).collect {|s| decode(s) }
      end

      # Returns the number of jobs currently running
      def running_count(tracking_key)
        Resque.redis.get(running_count_key(tracking_key)).to_i
      end

      # Returns the number of jobs currently running
      def set_running_count(tracking_key, value)
        count_key = running_count_key(tracking_key)
        Resque.redis.set(count_key, value)
        restricted = (value > concurrent_limit)
        mark_runnable(tracking_key, !restricted)
        return restricted
      end

      def restricted?(tracking_key)
        count_key = running_count_key(tracking_key)
        value = Resque.redis.get(count_key).to_i
        restricted = (value >= concurrent_limit)
        return restricted
      end

      def increment_running_count(tracking_key)
        count_key = running_count_key(tracking_key)
        value = Resque.redis.incr(count_key)
        restricted = (value > concurrent_limit)
        mark_runnable(tracking_key, !restricted)
        return restricted
      end

      def decrement_running_count(tracking_key)
        count_key = running_count_key(tracking_key)
        value = Resque.redis.decr(count_key)
        Resque.redis.set(count_key, 0) if value < 0
        restricted = (value >= concurrent_limit)
        mark_runnable(tracking_key, !restricted)
        return restricted
      end

      def increment_queue_count(queue, by=1)
        value = Resque.redis.hincrby(queue_count_key, queue, by)
        return value
      end

      def decrement_queue_count(queue, by=1)
        value = Resque.redis.hincrby(queue_count_key, queue, -by)
        return value
      end

      def queue_counts
        value = Resque.redis.hgetall(queue_count_key)
        value = Hash[*value.collect {|k, v| [k, v.to_i] }.flatten]
        return value
      end

      def set_queue_count(queue, count)
        Resque.redis.hset(queue_count_key, queue, count)
      end

      def runnable?(tracking_key, queue)
        Resque.redis.sismember(runnables_key(queue), tracking_key)
      end

      def get_next_runnable(queue)
        Resque.redis.srandmember(runnables_key(queue))
      end

      # Returns the list of tracking_keys that have jobs waiting to run (are not over the concurrency limit)
      def runnables(queue=nil)
        Resque.redis.smembers(runnables_key(queue))
      end

      # Keeps track of which jobs are currently runnable, that is the
      # tracking_key should have jobs on some restriction queue and
      # also have less than concurrency_limit jobs running
      #
      def mark_runnable(tracking_key, runnable)
        queues = queues_available(tracking_key)
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

      def clear_runnable(tracking_key, queue)
        Resque.redis.srem(runnables_key(queue), tracking_key)
        Resque.redis.srem(runnables_key, tracking_key)
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
        exp_backoff = 1

        while trying do
          lock_expiration = Time.now.to_i + ConcurrentRestriction.lock_timeout
          if acquire_lock(lock_key, lock_expiration)
            begin
              yield
            ensure
              release_lock(lock_key, lock_expiration)
            end
            trying = false
          else
            sleep(rand(1000) * 0.0001 * exp_backoff)
            exp_backoff *= 2
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
        tracking_key = tracking_key(*job.args)
        lock_key = lock_key(tracking_key)

        run_atomically(lock_key) do

          restricted = restricted?(tracking_key)
          if restricted
            push_to_restriction_queue(job)
          else
            increment_running_count(tracking_key)
          end

        end

        return restricted
      end

      # Returns the next job that is runnable
      def next_runnable_job(queue)
        tracking_key = get_next_runnable(queue)
        return nil unless tracking_key

        job = nil
        lock_key = lock_key(tracking_key)

        run_atomically(lock_key) do

          # since we don't have a lock when we get the runnable,
          # we need to check it again
          still_runnable = runnable?(tracking_key, queue)
          if still_runnable
            klazz = tracking_class(tracking_key)
            job = klazz.pop_from_restriction_queue(tracking_key, queue)
          end

        end

        return job
          
      end

      # Decrements the running_count - to be called at end of job
      def release_restriction(job)
        tracking_key = tracking_key(*job.args)
        lock_key = lock_key(tracking_key)

        run_atomically(lock_key) do

          # decrement the count after a job has run
          decrement_running_count(tracking_key)

        end
      end

      # Resets everything to be runnable
      def reset_restrictions

        counts_reset = 0
        count_keys = Resque.redis.keys("concurrent.count.*")
        counts_reset = Resque.redis.del(*count_keys) if count_keys.size > 0

        runnable_keys = Resque.redis.keys("concurrent.runnable*")
        Resque.redis.del(*runnable_keys) if runnable_keys.size > 0

        Resque.redis.del(queue_count_key)
        queues_enabled = 0
        queue_keys = Resque.redis.keys("concurrent.queue.*")
        queue_keys.each do |k|
          len = Resque.redis.llen(k)
          if len > 0
            parts = k.split(".")
            queue = parts[2]
            ident = parts[3..-1].join('.')
            tracking_key = "concurrent.tracking.#{ident}"

            increment_queue_count(queue, len)
            update_queues_available(tracking_key, queue, :add)
            mark_runnable(tracking_key, true)
            queues_enabled += 1
          end
        end

        return counts_reset, queues_enabled
        
      end

      def stats(extended=false)
        result = {}

        result[:queues] = queue_counts

        if extended
          ident_sizes = {}
          queue_keys = Resque.redis.keys("concurrent.queue.*")
          queue_keys.each do |k|
            parts = k.split(".")
            ident = parts[3..-1].join(".")
            queue_name = parts[2]
            size = Resque.redis.llen(k)
            ident_sizes[ident] ||= {}
            ident_sizes[ident][queue_name] ||= 0
            ident_sizes[ident][queue_name] += size
          end

          count_keys = Resque.redis.keys("concurrent.count.*")
          running_counts = {}
          count_keys.each do |k|
            parts = k.split(".")
            ident = parts[2..-1].join(".")
            ident_sizes[ident] ||= {}
            ident_sizes[ident]["running"] = Resque.redis.get(k).to_i
          end

          result[:identifiers] = ident_sizes
        else
          result[:identifiers] = {}
        end


        lock_keys = Resque.redis.keys("concurrent.lock.*")
        result[:lock_count] = lock_keys.size

        runnable_count = Resque.redis.scard(runnables_key)
        result[:runnable_count] = runnable_count

        return result
        
      end

    end

    # Allows users to subclass instead of extending in their job classes
    class ConcurrentRestrictionJob
      extend ConcurrentRestriction
    end

  end
end
