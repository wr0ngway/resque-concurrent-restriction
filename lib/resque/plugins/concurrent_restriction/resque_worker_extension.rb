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
            job_class = Helper.payload_class @job_in_progress
            job_class.release_restriction(@job_in_progress) if Helper.restrict_concurrency? @job_in_progress
          ensure
            return done_working_without_restriction
          end
        end
      end

      module Helper
        def self.payload_class(job)
          return job.payload_class unless active_job? job
          args = job_args job
          Object.const_get args['job_class']
        end

        # ActiveJobs are an ActiveJob::QueueAdapters::ResqueAdapter::JobWrapper
        def self.active_job?(job)
          job.payload_class.is_a? ActiveJob::QueueAdapters::ResqueAdapter::JobWrapper
        end

        def self.job_args(job)
          return job.args unless active_job? job
          job.args[0]['arguments']
        end

        def self.restrict_concurrency?(job)
          payload_class(job).is_a? ConcurrentRestriction
        end
      end

      module Job
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
          1.upto(ConcurrentRestriction.reserve_queued_job_attempts) do |i|
            resque_job = reserve_without_restriction(queue)

            # Short-curcuit if a job was not found
            return if resque_job.nil?

            # Return to work on job if not a restricted job
            return resque_job unless Helper.restrict_concurrency? resque_job

            # Keep trying if job is restricted. If job is runnable, we keep the lock until
            # done_working
            return resque_job unless job_class.stash_if_restricted resque_job
          end

          # Safety net, here in case we hit the upper bound and there are still queued items
          return nil
        end

      end

    end
  end
end
