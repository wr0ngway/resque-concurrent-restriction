require 'rspec'
require 'ap'

require 'resque-concurrent-restriction'

# No need to start redis when running in Travis
unless ENV['CI']

  begin
    Resque.queues
  rescue Errno::ECONNREFUSED
    spec_dir = File.dirname(File.expand_path(__FILE__))
    REDIS_CMD = "redis-server #{spec_dir}/redis-test.conf"
    
    puts "Starting redis for testing at localhost..."
    puts `cd #{spec_dir}; #{REDIS_CMD}`
    
    # Schedule the redis server for shutdown when tests are all finished.
    at_exit do
      puts 'Stopping redis'
      pid = File.read("#{spec_dir}/redis.pid").to_i rescue nil
      system ("kill -9 #{pid}") if pid.to_i != 0
      File.delete("#{spec_dir}/redis.pid") rescue nil
      File.delete("#{spec_dir}/redis-server.log") rescue nil
      File.delete("#{spec_dir}/dump.rdb") rescue nil
    end
  end
  
end

##
# Helper to perform job classes
#
module PerformJob

  def run_resque_job(job_class, *job_args)
    opts = job_args.last.is_a?(Hash) ? job_args.pop : {}
    queue = opts[:queue] || Resque.queue_from_class(job_class)

    Resque::Job.create(queue, job_class, *job_args)

    run_resque_queue(queue, opts)
  end

  def run_resque_queue(queue, opts={})
    worker = Resque::Worker.new(queue)
    worker.very_verbose = true if opts[:verbose]

    # do a single job then shutdown
    def worker.done_working
      super
      shutdown
    end

    if opts[:inline]
      job = worker.reserve
      worker.perform(job)
    else
      worker.work(0)
    end
  end

  def dump_redis
    result = {}
    Resque.redis.keys("*").each do |key|
      type = Resque.redis.type(key)
      result[key] = case type
        when 'string' then Resque.redis.get(key)
        when 'list' then Resque.redis.lrange(key, 0, -1)
        when 'set' then Resque.redis.smembers(key)
        else type
      end
    end
    return result
  end

end

module RunCountHelper

  def around_perform(*args)
    begin
      Resque.redis.set("restricted_job_started:#{self}:#{args.to_json}", true)
      Resque.redis.incr("restricted_job_run_count:#{self}:#{args.to_json}")
      yield
    ensure
      Resque.redis.set("restricted_job_ended:#{self}:#{args.to_json}", true)
    end
  end

  def perform(*args)
    #puts "Running job #{self}:#{args}"
  end

  def run_count(*args)
    Resque.redis.get("restricted_job_run_count:#{self}:#{args.to_json}").to_i
  end

  def total_run_count
    keys = Resque.redis.keys("restricted_job_run_count:#{self}:*")
    keys.inject(0) {|sum, k| sum + Resque.redis.get(k).to_i }
  end

  def started?(*args)
    return Resque.redis.get("restricted_job_started#{self}:#{args.to_json}") == true
  end

  def ended?(*args)
    return Resque.redis.get("restricted_job_ended#{self}:#{args.to_json}") == true
  end
end

class NoRestrictionJob
  extend RunCountHelper
  @queue = 'normal'
end

class RestrictionJob
  extend RunCountHelper
  extend Resque::Plugins::ConcurrentRestriction
  concurrent 1
  @queue = 'normal'
end

module Jobs
  class NestedRestrictionJob
    extend RunCountHelper
    extend Resque::Plugins::ConcurrentRestriction
    concurrent 1
    @queue = 'normal'
  end
end

class IdentifiedRestrictionJob
  extend RunCountHelper
  extend Resque::Plugins::ConcurrentRestriction
  concurrent 1
  @queue = 'normal'

  def self.concurrent_identifier(*args)
    args.first.to_s
  end

end

class ConcurrentRestrictionJob
  extend RunCountHelper
  extend Resque::Plugins::ConcurrentRestriction
  concurrent 1

  @queue = 'normal'

  def self.perform(*args)
    raise args.first if args.first
    sleep 0.2
  end
end

class MultipleConcurrentRestrictionJob
  extend RunCountHelper
  extend Resque::Plugins::ConcurrentRestriction
  concurrent 4

  @queue = 'normal'

  def self.perform(*args)
    sleep 0.5
  end
end

class OneConcurrentRestrictionJob
  extend RunCountHelper
  extend Resque::Plugins::ConcurrentRestriction
  concurrent 1

  @queue = 'normal'

  def self.perform(*args)
    sleep 0.5
  end
end
