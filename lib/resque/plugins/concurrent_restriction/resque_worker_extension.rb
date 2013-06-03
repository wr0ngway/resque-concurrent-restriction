module Resque
  module Plugins
    module ConcurrentRestriction

      module Worker

        def self.included(receiver)
           receiver.class_eval do
             alias reserve_without_restriction reserve
             alias reserve reserve_with_restriction

             alias done_working_without_restriction done_working
             alias done_working done_working_with_restriction
           end
         end

        # Wrap reserve so we can pass the job to done_working to release restriction if necessary
        def reserve_with_restriction
          @job_in_progress = reserve_without_restriction
          return @job_in_progress
        end

        # Wrap done_working so we can clear restriction locks after running.
        # We do this here instead of in Job.perform to improve odds of completing successfully
        # by running in the worker parent in case the child segfaults or something.
        # This needs to be a instance method
        def done_working_with_restriction
          begin
            job_class = @job_in_progress.payload_class
            job_class.release_restriction(@job_in_progress) if job_class.is_a?(ConcurrentRestriction)
          ensure
            return done_working_without_restriction
          end
        end
        
      end

      module Job

        # The default number of times to retry while attempting to get a job. Also, the maximum
        # number of jobs that will be moved from a queue to its corresponding restricted queue
        # each invocation of "get_queued_job"
        DEFAULT_GET_QUEUED_JOB_RETRIES = 1

        def self.extended(receiver)
           class << receiver
             alias reserve_without_restriction reserve
             alias reserve reserve_with_restriction
           end
        end

        # Wrap reserve so we can move a job to restriction queue if it is restricted
        # This needs to be a class method
        def reserve_with_restriction(queue)
          order = [:get_queued_job, :get_restricted_job]
          order.reverse! if ConcurrentRestriction.restricted_before_queued

          resque_job = nil
          order.each do |m|
            resque_job ||= self.send(m, queue)
          end

          # Return job or nil to move on to next queue if we couldn't get a job
          return resque_job
        end

        def get_restricted_job(queue)
          # Try to find a runnable job from restriction queues
          # This also acquires a restriction lock, which is released in done_working
          resque_job = ConcurrentRestrictionJob.next_runnable_job(queue)
          return resque_job
        end

        def get_queued_job(queue)
          # Bounded retry
          1.upto(get_queued_job_retries) do |i|
            resque_job = reserve_without_restriction(queue)

            # Short-curcuit if a job was not found
            return if resque_job.nil?

            # If there is a job on regular queues, then only run it if its not restricted
            job_class = resque_job.payload_class
            job_args = resque_job.args

            # Return to work on job if not a restricted job
            return resque_job unless job_class.is_a?(ConcurrentRestriction)

            # Keep trying if job is restricted. If job is runnable, we keep the lock until
            # done_working
            return resque_job unless job_class.stash_if_restricted(resque_job)
          end

          # Safety net, here in case we hit the upper bound and there are still queued items
          return nil
        end

        protected

        # Controls the number of times to retry while attempting to reserve a queued job. Added
        # as a method so that it can easily be overridden in a derived class (minimal validation)
        def get_queued_job_retries
          (ENV['GET_QUEUED_JOB_RETRIES'] =~ /\A\d+\Z/ || DEFAULT_GET_QUEUED_JOB_RETRIES).to_i
        end

      end

    end
  end
end
