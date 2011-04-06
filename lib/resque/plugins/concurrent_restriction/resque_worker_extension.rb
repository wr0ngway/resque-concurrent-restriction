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

        # Wrap reserve so we can pass the job to done_working to release restriction if neccessary
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

        def self.extended(receiver)
           class << receiver
             alias reserve_without_restriction reserve
             alias reserve reserve_with_restriction
           end
        end

        # Wrap reserve so we can move a job to restriction queue if it is restricted
        # This needs to be a class method
        def reserve_with_restriction(queue)
          resque_job = reserve_without_restriction(queue)

          if resque_job
            # If there is a job on regular queues, then only run it if its not restricted

            job_class = resque_job.payload_class
            job_args = resque_job.args

            # return to work on job if not a restricted job
            if job_class.is_a?(ConcurrentRestriction)
              # Move on to next if job is restricted
              # If job is runnable, we keep the lock until done_working
              resque_job = nil if job_class.stash_if_restricted(resque_job)

            end
            
          else
            # if no queues have a runnable job, then try to find a
            # runnable job from restriction queues
            # This also acquires a restriction lock, which is released in done_working
            resque_job = ConcurrentRestrictionJob.next_runnable_job(queue)

            return resque_job
          end

          # Return job or nil to move on to next queue if we couldn't get a job
          return resque_job

        end

      end

    end
  end
end
