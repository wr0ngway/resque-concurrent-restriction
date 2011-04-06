require 'rspec'
require 'ap'

spec_dir = File.dirname(File.expand_path(__FILE__))
REDIS_CMD = "redis-server #{spec_dir}/redis-test.conf"

puts "Starting redis for testing at localhost:9736..."
puts `cd #{spec_dir}; #{REDIS_CMD}`

require 'resque'
Resque.redis = 'localhost:9736'
require 'resque-concurrent-restriction'

# Schedule the redis server for shutdown when tests are all finished.
at_exit do
  pid = File.read("#{spec_dir}/redis.pid").to_i rescue nil
  system ("kill -9 #{pid}") if pid != 0
  File.delete("#{spec_dir}/redis.pid") rescue nil
  File.delete("#{spec_dir}/redis-server.log") rescue nil
  File.delete("#{spec_dir}/dump.rdb") rescue nil
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
  def perform(*args)
    Resque.redis.incr("restricted_job_run_count:#{self}")
  end

  def run_count
    Resque.redis.get("restricted_job_run_count:#{self}").to_i
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

class IdentifiedRestrictionJob
  extend RunCountHelper
  extend Resque::Plugins::ConcurrentRestriction
  concurrent 1
  @queue = 'normal'

  def self.concurrent_identifier(*args)
    [self.to_s, args.first].join(":")
  end

end

class ConcurrentRestrictionJob
  extend RunCountHelper
  extend Resque::Plugins::ConcurrentRestriction
  concurrent 1

  @queue = 'normal'

  def self.perform(*args)
    super
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
    super
    sleep 1
  end
end
